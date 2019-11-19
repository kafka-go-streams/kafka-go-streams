package streams

import (
	"context"
	"os"
	"testing"

	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func TestNewTable(t *testing.T) {
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
		Brokers: "localhost:9092",
		GroupID: "my_test_group",
		DB:      defaultRocksDB,
		Topic:   "test_topic",
		Context: context.Background(),
		Logger:  log,
	})

	c := make(chan int)
	<-c
	assert.Nil(t, err)
	assert.NotNil(t, table)
}
