package streams

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewTable(t *testing.T) {
	table, err := NewTable(&TableConfig{
		StoragePath: "table.db",
		Brokers:     "localhost:9092",
		GroupId:     "my_test_group",
		Topic:       "test_topic",
		Context:     context.Background(),
	})

	c := make(chan int)
	<-c
	assert.Nil(t, err)
	assert.NotNil(t, table)
}
