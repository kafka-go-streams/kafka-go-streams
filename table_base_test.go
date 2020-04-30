package streams

import (
	"context"
	"fmt"
	"testing"
	"time"

	kafka "github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/stretchr/testify/assert"
)

func TestNewTableBase(t *testing.T) {
	topic := makeTopic()
	createTopic("localhost", topic)

	// Produce test message
	producer, err := kafka.NewProducer(
		&kafka.ConfigMap{"bootstrap.servers": "localhost"},
	)
	if err != nil {
		t.Fatalf("Failed: %v", err)
	}
	testMessage := &kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key:            []byte("Key"),
		Value:          []byte("Value"),
	}
	err = producer.Produce(testMessage, nil)
	if err != nil {
		t.Fatalf("Failed: %v", err)
	}

	time.Sleep(2 * time.Second)

	// Start table
	tb, err := NewTableBase(&TableBaseConfig{
		StoragePath: "test_database",
		Brokers:     "localhost",
		Topic:       topic,
		Handler: func(p Pair) []Pair {
			fmt.Printf("%v: %v\n", p.Key, p.Value)
			return []Pair{p}
		},
		Context: context.Background(),
	})
	if err != nil {
		t.Fatalf("Failed: %v", err)
	}
	defer tb.Close()

	time.Sleep(2 * time.Second)
	s := tb.Get([]byte("Key"))
	assert.Equal(t, "Value", string(s))
}
