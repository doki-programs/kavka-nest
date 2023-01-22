package kavkanest

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type Handler func(payload []byte) error

type consumer struct {
	*kafka.Consumer
}

func NewConsumer(c *Client, groupID string, timeout time.Duration) (*consumer, error) {
	timeoutMs := int(timeout.Milliseconds())

	if len(c.BrokersUrl) == 0 {
		return nil, ErrInvalidBrokersUrl
	}

	if c.Username == "" {
		return nil, ErrInvalidUsername
	}

	if c.Password == "" {
		return nil, ErrInvalidPassword
	}

	config := &kafka.ConfigMap{
		"metadata.broker.list":     c.BrokersUrl,
		"security.protocol":        "SASL_SSL",
		"sasl.mechanisms":          c.ScramAlgorithm,
		"sasl.username":            c.Username,
		"sasl.password":            c.Password,
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

	return &consumer{cons}, nil
}

func (c *consumer) Consume(topics []string, timeout time.Duration, stop chan bool, fnHandler Handler) error {
	if err := c.SubscribeTopics(topics, nil); err != nil {
		return err
	}

	// A signal handler or similar could be used to set this to false to break the loop.
	run := true

	for run {
		select {
		case sig := <-stop:
			fmt.Printf("Caught stop signal %v: terminating\n", sig)
			run = false
		default:
			event := c.Poll(int(timeout.Milliseconds()))
			if event == nil {
				continue
			}

			switch e := event.(type) {
			case *kafka.Message:
				fmt.Printf("%% Message on %s:\n%s\n", e.TopicPartition, string(e.Value))
				if e.Headers != nil {
					fmt.Printf("%% Headers: %v\n", e.Headers)
				}
				if err := fnHandler(e.Value); err != nil {
					log.Println(err)
				}
				_, err := c.StoreMessage(e)
				if err != nil {
					fmt.Fprintf(os.Stderr, "%% Error storing offset after message %s:\n", e.TopicPartition)
				}
			case kafka.Error:
				// Errors should generally be considered
				// informational, the client will try to
				// automatically recover.
				// But in this example we choose to terminate
				// the application if all brokers are down.
				fmt.Fprintf(os.Stderr, "%% Error: %v: %v\n", e.Code(), e)
				if e.Code() == kafka.ErrAllBrokersDown {
					run = false
				}
			default:
				fmt.Printf("Ignored %v\n", e)
			}
		}
	}

	return c.Close()
}
