package kavkanest

import (
	"fmt"

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
		"sasl.mechanisms":      "SCRAM-SHA-256",
		"sasl.mechanismss":     c.ScramAlgorithm,
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

func (p *producer) AsyncProduce(topic string, messages, headers map[string]string, timeoutMs int) {

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
	kHeaders := []kafka.Header{}

	for key, value := range headers {
		kHeaders = append(kHeaders, kafka.Header{Key: key, Value: []byte(value)})
	}
	// Produce messages to topic (asynchronously)
	for key, msg := range messages {
		p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic:     &topic,
				Partition: kafka.PartitionAny,
			},
			Key:     []byte(key),
			Value:   []byte(msg),
			Headers: kHeaders,
		}, nil)
	}

	// Wait for message deliveries before shutting down
	p.Flush(timeoutMs)
}
