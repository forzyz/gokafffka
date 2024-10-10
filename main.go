package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"os"
	"sync"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type Storage struct {
	mu   sync.RWMutex
	data map[string][]byte
}

func NewStorage() *Storage {
	return &Storage{
		data: map[string][]byte{},
	}
}

func (s *Storage) Put(key string, value []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = value

	return nil
}

var (
	topic = "foobarbaztopic"
)

type MessageState int

const (
	MessageStateCompleted MessageState = iota
	MessageStateProgress
	MessageStateFailed
)

type Message struct {
	State MessageState
}

func main() {
	produce()
	consume()
}

func consume() {
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":        "localhost:9092",
		"broker.address.family":    "v4",
		"group.id":                 "group1",
		"session.timeout.ms":       6000,
		"auto.offset.reset":        "earliest",
		"enable.auto.offset.store": false,
	})

	if err != nil {
		log.Fatal(err)
	}

	err = c.SubscribeTopics([]string{topic}, nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to subscribe to topics: %s\n", err)
		os.Exit(1)
	}

	for {
		ev := c.Poll(100)
		if ev == nil {
			continue
		}
		switch e := ev.(type) {
		case *kafka.Message:
			_, err := c.StoreMessage(e)
			if err != nil {
				fmt.Println("store msg error:", err)
			}
			var msg Message
			if err := json.Unmarshal(e.Value, &msg); err != nil {
				log.Fatal(err)
			}
			fmt.Println(msg)
		case kafka.Error:
			if e.Code() == kafka.ErrAllBrokersDown {
				break

			}
		}
	}
}

func produce() {
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
	})
	if err != nil {
		log.Fatal(err)
	}

	for i := 0; i < 1000; i++ {
		msg := Message{
			State: MessageState(rand.Intn(3)),
		}
		b, err := json.Marshal(msg)
		if err != nil {
			log.Fatal(err)
		}
		err = p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic:     &topic,
				Partition: kafka.PartitionAny,
			},
			Value: b,
		}, nil)
		if err != nil {
			log.Fatal(err)
		}
	}
}
