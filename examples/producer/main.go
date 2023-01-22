package main

import (
	"fmt"
	"log"
	"os"
	"time"

	kavkanest "github.com/doki-programs/kavka-nest"
	"github.com/joho/godotenv"
)

var (
	topic = "test"
)

func main() {
	if err := godotenv.Load("./examples/.env"); err != nil {
		log.Fatal("failed to load client config >>> ", err)
	}
	p, err := kavkanest.NewProducer(&kavkanest.Client{
		Username:       os.Getenv("KAFKA_USERNAME"),
		Password:       os.Getenv("KAFKA_PASSWORD"),
		ScramAlgorithm: kavkanest.SCRAM_SHA_256,
		BrokersUrl:     os.Getenv("KAFKA_BROKERS"),
	})
	if err != nil {
		log.Fatal("failed to create new producer >>> ", err)
	}

	messages := [][]byte{}
	for i := 0; i < 9; i++ {
		// key := fmt.Sprintf("key%d", i)
		value := fmt.Sprintf("value%d", i)
		messages = append(messages, []byte(value))
	}
	p.AsyncProduce(topic, messages, 10*time.Second)
	p.Close()
}
