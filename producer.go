package kavkanest

import (
	"log"
	"os"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type Producer interface {
	SASL() *SASL
	TimeOut() time.Duration
}

var plog = log.New(os.Stdout, "[Producer]", log.LstdFlags)

type producer struct {
	producer   Producer
	connection *kafka.Producer
}

func NewProducer(p Producer) (*producer, error) {
	if p.SASL() == nil {
		return nil, ErrNilSASL
	}
	if len(p.SASL().BrokersUrl) == 0 {
		return nil, ErrEmptyBrokersUrl
	}

	if p.SASL().Username == "" {
		return nil, ErrEmptyUsername
	}

	if p.SASL().Password == "" {
		return nil, ErrEmptyPassword
	}
	config := &kafka.ConfigMap{
		"metadata.broker.list": p.SASL().BrokersUrl,
		"security.protocol":    "SASL_SSL",
		"sasl.mechanisms":      p.SASL().ScramAlgorithm.String(),
		"sasl.username":        p.SASL().Username,
		"sasl.password":        p.SASL().Password,
		// "debug": "generic,broker,security",
	}

	conn, err := kafka.NewProducer(config)
	if err != nil {
		return nil, err
	}

	return &producer{connection: conn, producer: p}, nil
}

func (p *producer) AsyncProduce(messages []*kafka.Message) {

	deliveryChan := make(chan kafka.Event)

	// Produce messages to topic (asynchronously)
	for _, msg := range messages {
		// x := &kafka.Message{
		// 	TopicPartition: kafka.TopicPartition{
		// 		Topic:     &topic,
		// 		Partition: kafka.PartitionAny,
		// 	},
		// 	Key:     []byte(),
		// 	Value:   msg,
		// 	Headers: kHeaders,
		// }
		err := p.connection.Produce(msg, deliveryChan)
		if err != nil {
			plog.Println("failed to produce message >>> ", err)
		}
		e := <-deliveryChan
		m := e.(*kafka.Message)
		if m.TopicPartition.Error != nil {
			plog.Printf("Delivery failed: %v\n", m.TopicPartition.Error)
		} else {
			plog.Printf("Delivered message to topic %s [%d] at offset %v\n",
				*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
		}
	}
	// Wait for message deliveries before shutting down
	p.connection.Flush(int(p.producer.TimeOut()))
	close(deliveryChan)
}

func (p *producer) Close() {
	p.connection.Close()
}
