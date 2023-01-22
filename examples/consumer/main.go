package main

import (
	"encoding/json"
	"log"
	"os"
	"time"

	kavkanest "github.com/doki-programs/kavka-nest"
	"github.com/joho/godotenv"
)

var (
	topics = []string{"default", "test"}
)

func main() {
	if err := godotenv.Load("./examples/.env"); err != nil {
		log.Fatal("failed to load client config >>> ", err)
	}
	c, err := kavkanest.NewConsumer(&kavkanest.Client{
		Username:       os.Getenv("KAFKA_USERNAME"),
		Password:       os.Getenv("KAFKA_PASSWORD"),
		ScramAlgorithm: "SCRAM-SHA-256",
		BrokersUrl:     os.Getenv("KAFKA_BROKERS"),
	}, "test-consumer-group", 60*time.Second)
	if err != nil {
		log.Fatal("failed to create new consumer >>> ", err)
	}

	if err := c.Consume(topics, 60*time.Second, make(chan bool), sampleHandler); err != nil {
		log.Fatal(err)
	}
}

// sampleHandler just logs the payload to standard output.
func sampleHandler(payload []byte) error {
	type event struct {
		Body string `json:"body"`
	}
	e := event{}
	if err := json.Unmarshal(payload, &e); err != nil {
		return err
	}
	log.Printf("sampleHandler >>> text: %s", e.Body)
	return nil
}
