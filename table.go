package streams

import (
	"context"

	log "github.com/sirupsen/logrus"
	rocksdb "github.com/tecbot/gorocksdb"
	k "gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

const (
	changeLogTopicSuffix = "-changelog"
)

func changelogTopicName(groupId, storageName string) string {
	return groupId + "-" + storageName + changeLogTopicSuffix
}

type TableConfig struct {
	StoragePath string
	Brokers     string
	GroupId     string
	Topic       string
	Context     context.Context
	Logger      *log.Logger
}

type Table struct {
	consumer *k.Consumer
	config   *TableConfig
	db       *rocksdb.DB
}

type rebalanceListener struct {
	log *log.Logger
}

func (l *rebalanceListener) rebalance(c *k.Consumer, e k.Event) error {
	l.log.Debugf("Rebalance event: %v", e)
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

	rl := &rebalanceListener{
		log: config.Logger,
	}
	consumer.Subscribe(config.Topic, func(c *k.Consumer, e k.Event) error {
		return rl.rebalance(c, e)
	})

	t := &Table{
		consumer: consumer,
		config:   config,
	}
	go t.run()
	return t, nil
}

func (t *Table) log(level log.Level, format string, args ...interface{}) {
	if t.config.Logger != nil {
		t.config.Logger.Logf(level, format, args...)
	}
}

func (t *Table) run() {
	for {
		e := t.consumer.Poll(1000)
		t.log(log.DebugLevel, "Poll event: %v\n", e)
	}
}
