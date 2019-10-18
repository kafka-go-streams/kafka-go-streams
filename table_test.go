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
	table, err := NewTable(&TableConfig{
		StoragePath: "table.db",
		Brokers:     "localhost:9092",
		GroupId:     "my_test_group",
		Topic:       "test_topic",
		Context:     context.Background(),
		Logger:      log,
	})

	c := make(chan int)
	<-c
	assert.Nil(t, err)
	assert.NotNil(t, table)
}
