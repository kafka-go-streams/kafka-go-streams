package streams

import (
	"log"
	"testing"

	k "github.com/confluentinc/confluent-kafka-go/kafka"
)

func rctRebalanceListener(c *RoutingConsumer, e k.Event) error {
	log.Printf("Event from rebalance listener: %v", e)
	return nil
}

func TestRoutingConsumer(t *testing.T) {
	consumer, err := k.NewConsumer(&k.ConfigMap{
		"bootstrap.servers":  "localhost:9092",
		"group.id":           "rebalance_test_group_id",
		"enable.auto.commit": false,
	})
	if err != nil {
		t.Errorf("Failed to construct consumer: %v", err)
		return
	}

	rc := NewRoutingConsumer(consumer)
	sub, err := rc.Subscribe([]string{"input_topic"}, rctRebalanceListener)
	if err != nil {
		log.Printf("Failed to subscribe: %v", err)
	}
	for {
		e := sub.Poll()
		switch v := e.(type) {
		case *k.Message:
			log.Printf("Key: %s, Value: %s", v.Key, v.Value)
		case *k.Error:
			if v.Code() != k.ErrTimedOut {
				log.Printf("Error receiving message: %v\n", v)
			}
		default:
			log.Printf("Unknown event type: %v\n", v)
		}
	}
}
