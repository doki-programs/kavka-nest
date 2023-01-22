.PHONY: producer
producer: 
	go run examples/producer/main.go

.PHONY: consumer
consumer: 
	go run examples/consumer/main.go	