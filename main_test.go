package streams

import (
	"bytes"
	"context"
	"flag"
	"testing"

	k "github.com/confluentinc/confluent-kafka-go/kafka"
	log "github.com/sirupsen/logrus"
)

var storage_path = flag.String("storage", "storage", "Path to storage")

func TestMain(t *testing.T) {
	//flag.Parse()

	rocksDB, err := DefaultRocksDB(*storage_path)

	if err != nil {
		log.Fatalf("Failed to construct rocks db")
	}

	brokers := "localhost:9092"
	groupId := "table_primer_group"

	logger := log.New()
	logger.SetLevel(log.TraceLevel)

	log.Printf("Created rocksdb: %v", rocksDB)

	consumer, err := k.NewConsumer(&k.ConfigMap{
		"bootstrap.servers":  brokers,
		"group.id":           groupId,
		"enable.auto.commit": false,
	})

	if err != nil {
		log.Fatalf("%v\n", err)
	}

	routingConsumer := NewRoutingConsumer(consumer)

	table, err := NewTable(&TableConfig{
		Brokers:  brokers,
		GroupID:  groupId,
		Topic:    "test_topic",
		DB:       rocksDB,
		Context:  context.Background(),
		Logger:   logger,
		Name:     "test_name",
		Consumer: routingConsumer,
	})
	if err != nil {
		log.Fatalf("%v\n", err)
	}

	subscription, err := routingConsumer.Subscribe([]string{"input_topic"}, func(c *RoutingConsumer, e k.Event) error {
		log.Printf("Rebalancing event:")
		switch v := e.(type) {
		case k.AssignedPartitions:
			printPartitions(v.Partitions)
		default:
			log.Printf("%v", e)
		}
		return nil
	})

	if err != nil {
		log.Fatalf("Failed to subscribe: %v", err)
	}

	log.Printf("Starting consumer poll")
	for {
		e := subscription.Poll()
		switch v := e.(type) {
		case *k.Message:
			log.Printf("Received message with key: %s", v.Key)
			tableValue, _ := table.Get(v.Key)
			log.Printf("Joined value: %s", bytes.Join([][]byte{tableValue.Data(), v.Value}, []byte(" -- ")))
		case *k.Error:
			if v.Code() != k.ErrTimedOut {
				log.Errorf("Error receiving message: %v", v)
			}
		default:
			log.Printf("Unknown event type: %v\n", v)
		}
	}

	//fmt.Printf("%v\n", table)
}
