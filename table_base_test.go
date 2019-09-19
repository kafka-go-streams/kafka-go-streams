package streams

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	kafka "gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

func makeTopic() string {
	rand.Seed(time.Now().Unix())
	return fmt.Sprintf("kafka-go-%016x", rand.Int63())
}

func TestNewTableBase(t *testing.T) {
	topic := makeTopic()

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

	time.Sleep(2 * time.Second)
	s := tb.Get([]byte("Key"))
	assert.Equal(t, "Value", string(s))
	tb.Close()
}
