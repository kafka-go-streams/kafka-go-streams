package streams

import (
	"context"
	"log"
	"testing"
	"time"
)

func TestManual(t *testing.T) {
	tb, err := NewGlobalTable(&GlobalTableConfig{
		StoragePath: "manual_database",
		Brokers:     "localhost",
		Context:     context.Background(),
		Topic:       "test_topic",
	})
	if err != nil {
		t.Fatalf("Error: %v", err)
	}
	defer tb.Close()
	for {
		res := tb.Get([]byte("AnotherKey"))
		log.Printf("%v", string(res))
		time.Sleep(10 * time.Second)
	}
}
