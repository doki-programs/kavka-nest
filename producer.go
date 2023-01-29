package kavkanest

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type Producer interface {
	KafkaClient() *KafkaClient
	TimeOut() time.Duration
}

var plog = log.New(os.Stdout, "[Producer]", log.LstdFlags)

type producer struct {
	producer   Producer
	connection *kafka.Producer
}

func NewProducer(p Producer) (*producer, error) {
	if p.KafkaClient() == nil {
		return nil, ErrNilKafkaClient
	}
	if len(p.KafkaClient().BrokersUrl) == 0 {
		return nil, ErrEmptyBrokersUrl
	}

	if p.KafkaClient().Username == "" {
		return nil, ErrEmptyUsername
	}

	if p.KafkaClient().Password == "" {
		return nil, ErrEmptyPassword
	}
	config := &kafka.ConfigMap{
		"client.id":            p.KafkaClient().Id,
		"metadata.broker.list": p.KafkaClient().BrokersUrl,
		"security.protocol":    "SASL_SSL",
		"sasl.mechanisms":      p.KafkaClient().ScramAlgorithm.String(),
		"sasl.username":        p.KafkaClient().Username,
		"sasl.password":        p.KafkaClient().Password,
	}

	if p.KafkaClient().DebugLevel != "" {
		config.Set(fmt.Sprintf("debug=%s", p.KafkaClient().DebugLevel.String()))
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
