package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"log/slog"
	"math/rand"
	"sync"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type Storer interface {
	Put(MessageState, []byte) error
	Get(MessageState) ([][]byte, error)
	MarkProcessed(id string)
	AllProcessed() bool
}

type Storage struct {
	mu             sync.RWMutex
	data           map[MessageState][][]byte
	processedIDs   map[string]bool
	expectedIDs    map[string]bool
}

func NewStorage(expectedIDs map[string]bool) *Storage {
	return &Storage{
		data: map[MessageState][][]byte{
			MessageStateCompleted: {},
			MessageStateFailed:    {},
			MessageStateProgress:  {},
		},
		processedIDs: expectedIDs,
		expectedIDs:  expectedIDs,
	}
}

func (s *Storage) Put(state MessageState, value []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[state] = append(s.data[state], value)

	return nil
}

func (s *Storage) Get(state MessageState) ([][]byte, error) {
	s.mu.RLock()
	defer s.mu.Unlock()
	val, ok := s.data[state]
	if !ok {
		return nil, fmt.Errorf("value not found")
	}
	return val, nil
}

func (s *Storage) MarkProcessed(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.processedIDs[id] = true
}

func (s *Storage) AllProcessed() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	for id := range s.expectedIDs {
		if !s.processedIDs[id] {
			return false
		}
	}
	return true
}

type MessageState int

const (
	MessageStateCompleted MessageState = iota
	MessageStateProgress
	MessageStateFailed
)

type Message struct {
	ID    string       `json:"id"`
	State MessageState `json:"state"`
}

var (
	topic        = "foobarbaztopic"
	lenMessages  = 1000
	producerMsgs = make(map[string]bool) // Tracking message IDs sent by producer
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	expectedIDs := generateMessageIDs(lenMessages)
	produce(cancel)

	c, err := NewConsumer(NewStorage(expectedIDs))
	if err != nil {
		log.Fatal(err)
	}
	c.consumeLoop(ctx)

	if c.Storage.AllProcessed() {
		fmt.Println("All messages processed successfully.")
	} else {
		fmt.Println("Some messages were not processed.")
	}
	fmt.Printf("Data: %+v\n", c.Storage.(*Storage).data)
}

type Consumer struct {
	consumer *kafka.Consumer
	Storage  Storer
}

func NewConsumer(storage Storer) (*Consumer, error) {
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":        "localhost:9092",
		"broker.address.family":    "v4",
		"group.id":                 "group1",
		"session.timeout.ms":       6000,
		"auto.offset.reset":        "earliest",
		"enable.auto.offset.store": false,
	})

	if err != nil {
		return nil, err
	}

	err = c.SubscribeTopics([]string{topic}, nil)
	if err != nil {
		return nil, err
	}
	return &Consumer{
		Storage:  storage,
		consumer: c,
	}, nil
}

func (c *Consumer) consumeLoop(ctx context.Context) {
	count := 0
	for {
		select {
		case <-ctx.Done():
			return
		default:
			ev := c.consumer.Poll(100)
			if ev == nil {
				continue
			}
			switch e := ev.(type) {
			case *kafka.Message:
				count++
				_, err := c.consumer.StoreMessage(e)
				if err != nil {
					fmt.Println("store msg error:", err)
				}

				var msg Message
				if err := json.Unmarshal(e.Value, &msg); err != nil {
					log.Fatal(err)
				}

				// Mark message as processed
				c.Storage.MarkProcessed(msg.ID)

				if err := c.Storage.Put(msg.State, e.Value); err != nil {
					log.Fatal(err)
				}

				if count == lenMessages {
					return
				}
			case kafka.Error:
				if e.Code() == kafka.ErrAllBrokersDown {
					return
				}
			}
		}
	}
}

func produce(cancel context.CancelFunc) {
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
	})
	if err != nil {
		log.Fatal(err)
	}

	slog.Info("start producing", "topic", topic, "messages", lenMessages)
	for i := 0; i < lenMessages; i++ {
		state := MessageState(rand.Intn(3))
		id := fmt.Sprintf("msg-%d", i) // Generate unique message ID
		producerMsgs[id] = true
		msg := Message{
			ID:    id,
			State: state,
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
	slog.Info("done producing", "topic", topic, "messages", lenMessages)
}

func generateMessageIDs(count int) map[string]bool {
	ids := make(map[string]bool)
	for i := 0; i < count; i++ {
		id := fmt.Sprintf("msg-%d", i)
		ids[id] = false
	}
	return ids
}
