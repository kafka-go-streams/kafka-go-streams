package streams

import (
	"context"
	"testing"
	"time"

	k "github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/stretchr/testify/assert"
)

func TestNewGlobalTable(t *testing.T) {
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

	// Start table
	tb, err := NewGlobalTable(&GlobalTableConfig{
		StoragePath: "test_database",
		Brokers:     "localhost",
		Topic:       topic,
		Context:     context.Background(),
	})
	if err != nil {
		t.Fatalf("Failed: %v", err)
	}
	defer tb.Close()

	time.Sleep(2 * time.Second)
	s := tb.Get([]byte("Key"))
	assert.Equal(t, "Value", string(s))
}
