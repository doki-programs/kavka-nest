package kavkanest

import (
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type Consumer interface {
	KafkaClient() *KafkaClient
	TimeOut() time.Duration
	KafkaTopics() []string
	HandleMessage(msg *kafka.Message) error
}

var clog = log.New(os.Stdout, "[consumer] ", log.LstdFlags)

var (
	ErrEmptyGroupID = errors.New("group id is requird")
)

type consumer struct {
	consumer   Consumer
	groupID    string
	connection *kafka.Consumer
}

func NewConsumer(c Consumer, groupID string) (*consumer, error) {

	if err := c.KafkaClient().Validate(); err != nil {
		return nil, err
	}

	if groupID == "" {
		return nil, ErrEmptyGroupID
	}

	config := &kafka.ConfigMap{
		"client.id":                c.KafkaClient().Id,
		"metadata.broker.list":     c.KafkaClient().BrokersUrl,
		"security.protocol":        "SASL_SSL",
		"sasl.mechanisms":          c.KafkaClient().ScramAlgorithm.String(),
		"sasl.username":            c.KafkaClient().Username,
		"sasl.password":            c.KafkaClient().Password,
		"ssl.ca.location":          c.KafkaClient().CertLocation,
		"group.id":                 groupID,
		"session.timeout.ms":       int(c.TimeOut().Milliseconds()),
		"auto.offset.reset":        "earliest",
		"enable.auto.offset.store": false,
	}

	if c.KafkaClient().DebugLevel != "" {
		config.Set(fmt.Sprintf("debug=%s", c.KafkaClient().DebugLevel.String()))
	}

	conn, err := kafka.NewConsumer(config)
	if err != nil {
		return nil, err
	}

	return &consumer{
		groupID:    groupID,
		connection: conn,
		consumer:   c,
	}, nil
}

func (c *consumer) Consume(stop chan bool) error {

	if err := c.connection.SubscribeTopics(c.consumer.KafkaTopics(), nil); err != nil {
		return err
	}

	run := true

	for run {
		select {
		case <-stop:
			clog.Printf("Caught stop signal >>> terminating...")
			run = false
		default:
			event := c.connection.Poll(int(c.consumer.TimeOut()))
			if event == nil {
				continue
			}

			switch e := event.(type) {
			case *kafka.Message:
				if err := c.consumer.HandleMessage(e); err != nil {
					clog.Println(err)
				}
				_, err := c.connection.StoreMessage(e)
				if err != nil {
					fmt.Fprintf(os.Stderr, "Error storing offset after message %s:\n", e.TopicPartition)
				}
			case kafka.Error:
				// Errors should generally be considered informational, the client will try to
				// automatically recover. But we choose to terminate the consumer in case of
				// all brokers are down or local timeout occcured.
				switch e.Code() {
				case kafka.ErrAllBrokersDown:
					run = false
				case kafka.ErrTimedOut:
					run = false
				default:
					fmt.Fprintf(os.Stderr, "Error: %v: %v\n", e.Code(), e)
				}
			case kafka.OffsetsCommitted:
				if e.Error != nil {
					clog.Printf("failed to commit offsets >>> %s", e.Error.Error())
				}
				for _, topicPartition := range e.Offsets {
					clog.Printf("topic %s has been committed successfully", topicPartition)
				}
			default:
				clog.Printf("Ignored event >>> %v\n", e)
			}
		}
	}

	return c.connection.Close()
}
