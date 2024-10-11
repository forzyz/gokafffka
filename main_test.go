package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"testing"
	"time"
)

// MockStorer is a mock implementation of Storer
type MockStorer struct {
	mu           sync.RWMutex
	data         map[MessageState][][]byte
	processedIDs map[string]bool
}

func NewMockStorer() *MockStorer {
	return &MockStorer{
		data: map[MessageState][][]byte{
			MessageStateCompleted: {},
			MessageStateFailed:    {},
			MessageStateProgress:  {},
		},
		processedIDs: make(map[string]bool),
	}
}

func (s *MockStorer) Put(state MessageState, value []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[state] = append(s.data[state], value)
	return nil
}

func (s *MockStorer) Get(state MessageState) ([][]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	val, ok := s.data[state]
	if !ok {
		return nil, fmt.Errorf("value not found")
	}
	return val, nil
}

func (s *MockStorer) MarkProcessed(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.processedIDs[id] = true
}

func (s *MockStorer) AllProcessed() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for id := range producerMsgs {
		if !s.processedIDs[id] {
			return false
		}
	}
	return true
}

func (s *MockStorer) Count(state MessageState) int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.data[state])
}

// TestMessageParsing asserts that messages are parsed into the correct buckets
func TestMessageParsing(t *testing.T) {
	const totalMessages = 1000
	expectedCompleted := totalMessages / 2
	expectedInProgress := totalMessages / 4
	expectedFailed := totalMessages / 4

	// Create a mock storage
	storage := NewMockStorer()

	// Produce test messages
	for i := 0; i < totalMessages; i++ {
		var state MessageState
		if i < expectedCompleted {
			state = MessageStateCompleted
		} else if i < expectedCompleted+expectedInProgress {
			state = MessageStateProgress
		} else {
			state = MessageStateFailed
		}

		msg := Message{State: state}
		b, err := json.Marshal(msg)
		if err != nil {
			log.Fatal(err)
		}
		// Store messages in the mock storage
		if err := storage.Put(state, b); err != nil {
			t.Fatal(err)
		}
	}

	// Run consumer to process messages
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // Ensure cancellation is handled

	// Start the consumer
	c, err := NewConsumer(storage)
	if err != nil {
		t.Fatal(err)
	}
	go c.consumeLoop(ctx)

	// Wait a bit for the consumer to process messages
	time.Sleep(1 * time.Second)

	// Assertions
	if count := storage.Count(MessageStateCompleted); count != expectedCompleted {
		t.Errorf("expected %d completed messages, got %d", expectedCompleted, count)
	}

	if count := storage.Count(MessageStateProgress); count != expectedInProgress {
		t.Errorf("expected %d in-progress messages, got %d", expectedInProgress, count)
	}

	if count := storage.Count(MessageStateFailed); count != expectedFailed {
		t.Errorf("expected %d failed messages, got %d", expectedFailed, count)
	}
}
