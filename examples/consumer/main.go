package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	kavkanest "github.com/doki-programs/kavka-nest"
	"github.com/joho/godotenv"
)

var (
	topics  = []string{"quickstart"}
	groupID = "test-consumer-group"
	timeout = 60 * time.Second
)

type sampleConsumer struct {
}

func (c *sampleConsumer) KafkaClient() *kavkanest.KafkaClient {
	return &kavkanest.KafkaClient{
		Id:             "test_client_id",
		Username:       os.Getenv("KAFKA_USERNAME"),
		Password:       os.Getenv("KAFKA_PASSWORD"),
		ScramAlgorithm: kavkanest.SCRAM_SHA_256,
		CertLocation:   os.Getenv("CERT_LOCATION"),
		BrokersUrl:     os.Getenv("KAFKA_BROKERS"),
		// DebugLevel:     kavkanest.DEBUG_LEVEL_ALL,
	}
}

func (p *sampleConsumer) TimeOut() time.Duration {
	return timeout
}

func (c *sampleConsumer) KafkaTopics() []string {
	return topics
}

func (c *sampleConsumer) HandleMessage(msg *kafka.Message) error {
	log.Println("sample consumer handler >>> message recieved:")
	if len(msg.Headers) > 0 {
		log.Printf("headers: %v", msg.Headers)
	}

	topic := *msg.TopicPartition.Topic
	partiotion := msg.TopicPartition.Partition
	offset := msg.TopicPartition.Offset
	time := msg.Timestamp

	log.Printf("key: %s, value: %s", string(msg.Key), string(msg.Value))
	log.Printf("topic[partiotion]@offset: %s[%d]@%v created at %v", topic, partiotion, offset, time)
	return nil
}

func main() {
	if err := godotenv.Load("./examples/.env"); err != nil {
		log.Fatal("failed to load client config >>> ", err)
	}
	stop := make(chan bool)
	go func() {
		c, err := kavkanest.NewConsumer(&sampleConsumer{}, groupID)
		if err != nil {
			log.Fatal("failed to create new consumer >>> ", err)
		}
		if err := c.Consume(stop); err != nil {
			log.Fatal(err)
		}
	}()

	fmt.Println("Press Ctrl+C to stop consuming...")
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT)
	defer signal.Stop(signals)

	<-signals // wait for signal
	go func() {
		<-signals // hard exit on second signal (in case shutdown gets stuck)
		os.Exit(1)
	}()
	stop <- true
}
