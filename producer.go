package kavkanest

import (
	"fmt"
	"log"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type producer struct {
	*kafka.Producer
}

func NewProducer(c *Client) (*producer, error) {
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
		"metadata.broker.list": c.BrokersUrl,
		"security.protocol":    "SASL_SSL",
		"sasl.mechanisms":      c.ScramAlgorithm.String(),
		"sasl.username":        c.Username,
		"sasl.password":        c.Password,
		// "debug": "generic,broker,security",
	}

	p, err := kafka.NewProducer(config)
	if err != nil {
		return nil, err
	}

	return &producer{p}, nil
}

func (p *producer) AsyncProduce(topic string, messages [][]byte /*,headers map[string]string,*/, timeout time.Duration) {

	// Delivery report handler for produced messages
	go func() {
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Printf("Delivery failed: %v\n", ev.TopicPartition)
				} else {
					fmt.Printf("Delivered message to %v\n", ev.TopicPartition)
				}
			}
		}
	}()
	// kHeaders := []kafka.Header{}

	// for key, value := range headers {
	// 	kHeaders = append(kHeaders, kafka.Header{Key: key, Value: []byte(value)})
	// }

	deliveryChan := make(chan kafka.Event)

	// Produce messages to topic (asynchronously)
	for _, msg := range messages {
		err := p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic:     &topic,
				Partition: kafka.PartitionAny,
			},
			// Key:   []byte(key),
			Value: msg,
			// Headers: kHeaders,
		}, deliveryChan)
		if err != nil {
			log.Println("failed to produce message >>> ", err)
		}
		e := <-deliveryChan
		m := e.(*kafka.Message)
		if m.TopicPartition.Error != nil {
			fmt.Printf("Delivery failed: %v\n", m.TopicPartition.Error)
		} else {
			fmt.Printf("Delivered message to topic %s [%d] at offset %v\n",
				*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
		}
	}
	close(deliveryChan)
	// Wait for message deliveries before shutting down
	p.Flush(int(timeout.Milliseconds()))
}
