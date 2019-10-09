package streams

import (
	"context"
	"fmt"

	k "gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

type TableConfig struct {
	StoragePath string
	Brokers     string
	GroupId     string
	Topic       string
	Context     context.Context
}

type Table struct {
	consumer *k.Consumer
}

func rebalanceCb(c *k.Consumer, e k.Event) error {
	fmt.Printf("Rebalance event: %v\n", e)
	return nil
}

func NewTable(config *TableConfig) (*Table, error) {
	consumer, err := k.NewConsumer(&k.ConfigMap{
		"bootstrap.servers": config.Brokers,
		"group.id":          config.GroupId,
	})
	if err != nil {
		return nil, err
	}
	consumer.Subscribe(config.Topic, rebalanceCb)

	t := &Table{
		consumer: consumer,
	}
	go t.run()
	return t, nil
}

func (t *Table) run() {
	for {
		e := t.consumer.Poll(1000)
		fmt.Printf("Poll event: %v\n", e)
	}
}
