package main

import (
	"log"
	"os"
	"time"

	kavkanest "github.com/doki-programs/kavka-nest"
	"github.com/joho/godotenv"
)

var (
	topics  = []string{"default", "test"}
	groupID = "test-consumer-group"
	timeout = 60 * time.Second
)

func main() {
	if err := godotenv.Load("./examples/.env"); err != nil {
		log.Fatal("failed to load client config >>> ", err)
	}
	client := &kavkanest.Client{
		Username:       os.Getenv("KAFKA_USERNAME"),
		Password:       os.Getenv("KAFKA_PASSWORD"),
		ScramAlgorithm: kavkanest.SCRAM_SHA_256,
		BrokersUrl:     os.Getenv("KAFKA_BROKERS"),
	}
	c, err := kavkanest.NewConsumer(client, groupID, timeout)
	if err != nil {
		log.Fatal("failed to create new consumer >>> ", err)
	}

	if err := c.Consume(topics, make(chan bool), sampleHandler); err != nil {
		log.Fatal(err)
	}
}

// sampleHandler just logs the payload to standard output.
func sampleHandler(payload []byte) error {
	log.Printf("sampleHandler >>> msg: %s", string(payload))
	return nil
}
