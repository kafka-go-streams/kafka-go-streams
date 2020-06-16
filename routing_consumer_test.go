package streams

import (
	"context"
	"os"
	"testing"

	k "github.com/confluentinc/confluent-kafka-go/kafka"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func rctRebalanceListener(c *RoutingConsumer, e k.Event) error {
	log.Printf("Subscription: Event from rebalance listener: %v", e)
	return nil
}

func printer(name string, sub *Subscription, rc *RoutingConsumer) {
	for {
		e := sub.Poll()
		switch v := e.(type) {
		case *k.Message:
			log.Printf("Received by consumer: %s, Key: %s, Value: %s", name, v.Key, v.Value)
			_, err := rc.CommitMessage(v)
			if err != nil {
				log.Printf("Failed to commit message offset: %v", err)
			}
		case *k.Error:
			if v.Code() != k.ErrTimedOut {
				log.Printf("Error receiving message: %v\n", v)
			}
		default:
			log.Printf("Unknown event type: %v\n", v)
		}
	}
}

func TestRoutingConsumer(t *testing.T) {
	consumer, err := k.NewConsumer(&k.ConfigMap{
		"bootstrap.servers":  "localhost:9092",
		"group.id":           "rebalance_test_group_id",
		"enable.auto.commit": false,
		"auto.offset.reset":  "beginning",
	})
	if err != nil {
		t.Errorf("Failed to construct consumer: %v", err)
		return
	}

	rc := NewRoutingConsumer(consumer)
	sub1, err := rc.Subscribe([]string{"input_topic"}, rctRebalanceListener)
	if err != nil {
		log.Printf("Failed to subscribe: %v", err)
	}
	go printer("ONE", sub1, rc)
	endCh := make(chan struct{})
	<-endCh
}

func TestTwoRoutingConsumers(t *testing.T) {
	consumer, err := k.NewConsumer(&k.ConfigMap{
		"bootstrap.servers":  "localhost:9092",
		"group.id":           "rebalance_test_group_id",
		"enable.auto.commit": false,
		"auto.offset.reset":  "beginning",
	})
	if err != nil {
		t.Errorf("Failed to construct consumer: %v", err)
		return
	}

	rc := NewRoutingConsumer(consumer)
	sub1, err := rc.Subscribe([]string{"input_topic"}, rctRebalanceListener)
	if err != nil {
		log.Printf("Failed to subscribe: %v", err)
	}
	go printer("ONE", sub1, rc)
	sub2, err := rc.Subscribe([]string{"test_topic"}, rctRebalanceListener)
	if err != nil {
		log.Printf("Failed to subscribe: %v", err)
	}
	go printer("TWO", sub2, rc)
	endCh := make(chan struct{})
	<-endCh
}

func TestTablePlusConsumer(t *testing.T) {
	groupId := "rebalance_test_group_id_table"
	brokers := "localhost:9092"
	consumer, err := k.NewConsumer(&k.ConfigMap{
		"bootstrap.servers":  brokers,
		"group.id":           groupId,
		"enable.auto.commit": false,
		"auto.offset.reset":  "beginning",
	})
	if err != nil {
		t.Errorf("Failed to construct consumer: %v", err)
		return
	}

	rc := NewRoutingConsumer(consumer)

	// Constructing table

	log := &log.Logger{
		Out:       os.Stderr,
		Formatter: new(log.JSONFormatter),
		Hooks:     make(log.LevelHooks),
		Level:     log.TraceLevel,
	}

	defaultRocksDB, err := DefaultRocksDB("my_table_test.db")
	if err != nil {
		t.Fatalf("Failed to construct rocksdb: %v", err)
	}
	table, err := NewTable(&TableConfig{
		Brokers:  brokers,
		GroupID:  groupId,
		Consumer: rc,
		DB:       defaultRocksDB,
		Topic:    "test_topic",
		Context:  context.Background(),
		Logger:   log,
	})

	assert.Nil(t, err)

	// Running subscription
	sub, err := rc.Subscribe([]string{"input_topic"}, rctRebalanceListener)
	if err != nil {
		log.Printf("Failed to subscribe: %v", err)
	}

	for {
		curAssignment, err := consumer.Assignment()
		assert.Nil(t, err)
		log.Printf("Current Assignment: %v", curAssignment)
		e := sub.Poll()
		switch v := e.(type) {
		case *k.Message:
			log.Printf("Received by consumer. Key: %s, Value: %s", v.Key, v.Value)
			tableValue, err := table.Get(v.Key)
			assert.Nil(t, err)
			log.Printf("Table value: %s", tableValue)
			_, err = rc.CommitMessage(v)
			if err != nil {
				log.Printf("Failed to commit message offset: %v", err)
			}
		case *k.Error:
			if v.Code() != k.ErrTimedOut {
				log.Printf("Error receiving message: %v\n", v)
			}
		default:
			log.Printf("Unknown event type: %v\n", v)
		}
	}

}
