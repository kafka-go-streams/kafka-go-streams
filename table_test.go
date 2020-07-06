package streams

import (
	"context"
	"os"
	"testing"

	k "github.com/confluentinc/confluent-kafka-go/kafka"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func TestNewTable(t *testing.T) {
	brokers := "localhost:9092"
	groupId := "my_test_group"
	// consumer
	consumer, err := k.NewConsumer(&k.ConfigMap{
		"bootstrap.servers":  brokers,
		"group.id":           groupId,
		"enable.auto.commit": false,
	})
	if err != nil {
		t.Errorf("Failed to construct consumer: %v", err)
	}
	routingConsumer := NewRoutingConsumer(consumer)

	log := &log.Logger{
		Out:       os.Stderr,
		Formatter: new(log.JSONFormatter),
		Hooks:     make(log.LevelHooks),
		Level:     log.DebugLevel,
	}
	defaultRocksDB, err := DefaultRocksDB("my_table_test.db")
	if err != nil {
		t.Fatalf("Failed to construct rocksdb: %v", err)
	}
	table, err := NewTable(&TableConfig{
		Brokers:  brokers,
		GroupID:  groupId,
		Consumer: routingConsumer,
		DB:       defaultRocksDB,
		Topic:    "test_topic",
		Context:  context.Background(),
		Logger:   log,
	})

	c := make(chan int)
	<-c
	assert.Nil(t, err)
	assert.NotNil(t, table)
}
