# **Kavka Nest**

**Kavka nest** is a wrapper around [Confluent's Apache Kafka Golang client](https://github.com/confluentinc/confluent-kafka-go) which provides you a procucer/consumer interface to communicate with kafka brokers using SASL. 

## **Producer Interface**

 In order to produce a kafka message in a topic/partition, just implement kavakanest's *Producer* interface: 

```go
type Producer interface {
	KafkaClient() *KafkaClient
	TimeOut() time.Duration
}
```
Given a remote kafka broker client's SASL credentials you can implement ***KafkaClient*** method.

```go
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

```

Given a sample ***Producer*** interface implementation, you can initialize and produce kafka messages. Here is a simple example: 

```go
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

```

 Full example is available in [producer examples](https://github.com/doki-programs/kavka-nest/blob/main/examples/producer/main.go)

## **Consumer Interface**

 In order to consume a kafka message in topics, just implement kavakanest's ***Consumer*** interface: 

```go
type Consumer interface {
	KafkaClient() *KafkaClient
	TimeOut() time.Duration
	KafkaTopics() []string
	HandleMessage(msg *kafka.Message) error
}

```
Like the *Producer* interface, you can implement ***KafkaClient*** method using your  remote kafka broker client's SASL credentials. 

```go
var (
	topics  = []string{"default", "test"}
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
		BrokersUrl:     os.Getenv("KAFKA_BROKERS"),
	}
}

func (p *sampleConsumer) TimeOut() time.Duration {
	return timeout
}

func (c *sampleConsumer) KafkaTopics() []string {
	return topics
}

```

You can access to kafka messages headers, topic, partition, offset, timestamp, and so on in your ***HandleMessage*** implementation. Here is a simple implementation: 

```go
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

```

Given a sample ***Consumer*** interface implementation, you can initialize and produce kafka messages: 

```go
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

```

 Full example is available in [consumer examples](https://github.com/doki-programs/kavka-nest/blob/main/examples/consumer/main.go)

