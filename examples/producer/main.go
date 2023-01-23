package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	kavkanest "github.com/doki-programs/kavka-nest"
	"github.com/google/uuid"
	"github.com/joho/godotenv"
)

var (
	topic   = "default"
	timeout = 10 * time.Second
)

type sampleProducer struct {
}

func (p *sampleProducer) KafkaClient() *kavkanest.KafkaClient {
	return &kavkanest.KafkaClient{
		Id:             "test-client-id",
		Username:       os.Getenv("KAFKA_USERNAME"),
		Password:       os.Getenv("KAFKA_PASSWORD"),
		ScramAlgorithm: kavkanest.SCRAM_SHA_256,
		BrokersUrl:     os.Getenv("KAFKA_BROKERS"),
	}
}

func (p *sampleProducer) TimeOut() time.Duration {
	return timeout
}

func main() {
	if err := godotenv.Load("./examples/.env"); err != nil {
		log.Fatal("failed to load client config >>> ", err)
	}
	p, err := kavkanest.NewProducer(&sampleProducer{})
	if err != nil {
		log.Fatal("failed to create new producer >>> ", err)
	}

	messages := []*kafka.Message{}
	for i := 0; i < 9; i++ {
		key := uuid.New().String()
		value := fmt.Sprintf("value%d", i)
		messages = append(messages, &kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic:     &topic,
				Partition: kafka.PartitionAny,
			},
			Key:   []byte(key),
			Value: []byte(value),
		})
	}
	p.AsyncProduce(messages)
	p.Close()
}
