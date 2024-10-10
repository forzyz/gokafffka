package main

import (
	"fmt"
	"log"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
	})
	if err != nil {
		log.Fatal(err)
	}

	defer p.Close()

	topic := "foobartopic"
	err = p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic: &topic, 
			Partition: kafka.PartitionAny,
		},
		Value:          []byte("foobarbaz"),
	}, nil)

	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(p)
}
