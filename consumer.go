package kavkanest

import (
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

var clogger = log.New(os.Stdout, "[consumer] ", log.LstdFlags)

var (
	ErrEmptyGroupID = errors.New("group id is requird")
)

type Handler func(payload []byte) error

type consumer struct {
	groupID string
	timeout time.Duration
	kavka   *kafka.Consumer
}

func NewConsumer(client *Client, groupID string, timeout time.Duration) (*consumer, error) {

	timeoutMs := int(timeout.Milliseconds())

	if len(client.BrokersUrl) == 0 {
		return nil, ErrInvalidBrokersUrl
	}

	if client.Username == "" {
		return nil, ErrInvalidUsername
	}

	if client.Password == "" {
		return nil, ErrInvalidPassword
	}
	if groupID == "" {
		return nil, ErrEmptyGroupID
	}

	config := &kafka.ConfigMap{
		"metadata.broker.list":     client.BrokersUrl,
		"security.protocol":        "SASL_SSL",
		"sasl.mechanisms":          client.ScramAlgorithm.String(),
		"sasl.username":            client.Username,
		"sasl.password":            client.Password,
		"group.id":                 groupID,
		"session.timeout.ms":       timeoutMs,
		"auto.offset.reset":        "earliest",
		"enable.auto.offset.store": false,

		// "debug": "generic,broker,security",
	}

	cons, err := kafka.NewConsumer(config)
	if err != nil {
		return nil, err
	}

	return &consumer{
		groupID: groupID,
		timeout: timeout,
		kavka:   cons,
	}, nil
}

func (c *consumer) Consume(topics []string, stop chan bool, fnHandler Handler) error {

	if err := c.kavka.SubscribeTopics(topics, nil); err != nil {
		return err
	}

	// A signal handler or similar could be used to set this to false to break the loop.
	run := true

	for run {
		select {
		case sig := <-stop:
			clogger.Printf("Caught stop signal %v: terminating\n", sig)
			run = false
		default:
			event := c.kavka.Poll(int(c.timeout.Milliseconds()))
			if event == nil {
				continue
			}

			switch e := event.(type) {
			case *kafka.Message:
				// fmt.Printf("%% Message on %s:\n%s\n", e.TopicPartition, string(e.Value))
				if e.Headers != nil {
					fmt.Printf("%% Headers: %v\n", e.Headers)
				}
				if err := fnHandler(e.Value); err != nil {
					clogger.Println(err)
				}
				_, err := c.kavka.StoreMessage(e)
				if err != nil {
					fmt.Fprintf(os.Stderr, "Error storing offset after message %s:\n", e.TopicPartition)
				}
			case kafka.Error:
				// Errors should generally be considered
				// informational, the client will try to
				// automatically recover.
				// But in this example we choose to terminate
				// the application if all brokers are down.
				switch e.Code() {
				case kafka.ErrAllBrokersDown:
					run = false
				case kafka.ErrTimedOut:
					run = false
				default:
					fmt.Fprintf(os.Stderr, "Error: %v: %v\n", e.Code(), e)
				}
			default:
				clogger.Printf("Ignored %v\n", e)
			}
		}
	}

	return c.kavka.Close()
}
