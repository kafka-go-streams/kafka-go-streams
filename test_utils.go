package streams

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	kafka "gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

func makeTopic() string {
	rand.Seed(time.Now().Unix())
	return fmt.Sprintf("kafka-go-%016x", rand.Int63())
}

func createTopic(brokers string, topic string) error {
	admin, err := kafka.NewAdminClient(&kafka.ConfigMap{
		"bootstrap.servers": brokers,
	})
	if err != nil {
		return err
	}
	rs, err := admin.CreateTopics(
		context.Background(),
		[]kafka.TopicSpecification{
			kafka.TopicSpecification{
				Topic:             topic,
				NumPartitions:     1,
				ReplicationFactor: 1,
			},
		},
	)
	for _, r := range rs {
		if r.Error.Code() != kafka.ErrNoError {
			return r.Error
		}
	}
	return err
}
