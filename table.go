package streams

import (
	"context"
	"fmt"
	"time"

	log "github.com/sirupsen/logrus"
	rocksdb "github.com/tecbot/gorocksdb"
	k "gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

const (
	changeLogTopicSuffix = "-changelog"
)

func changelogTopicName(groupID, storageName string) string {
	return groupID + "-" + storageName + changeLogTopicSuffix
}

// TableConfig is a structure for configuring table.
type TableConfig struct {
	StoragePath string
	Brokers     string
	GroupID     string
	Topic       string
	Context     context.Context
	Logger      *log.Logger
}

// Table is a primitive for working with distributed tables.
type Table struct {
	consumer *k.Consumer
	config   *TableConfig
	db       *rocksdb.DB
	ctx      context.Context
	cancel   context.CancelFunc
	finished chan struct{}
	log      *LogWrapper
}

type rebalanceListener struct {
	log *LogWrapper
}

func (l *rebalanceListener) rebalance(c *k.Consumer, e k.Event) error {
	switch v := e.(type) {
	case k.AssignedPartitions:
		l.log.log(log.DebugLevel, "It was assigned partitions event: %v", v)
	case k.RevokedPartitions:
		l.log.log(log.DebugLevel, "It was revoked partitions event: %v", v)
	default:
		l.log.log(log.DebugLevel, "Unknown rebalance event: %v", e)
	}
	return nil
}

// NewTable constructs a new table.
func NewTable(config *TableConfig) (*Table, error) {

	logWrapper := &LogWrapper{config.Logger}

	bbto := rocksdb.NewDefaultBlockBasedTableOptions()
	bbto.SetBlockCache(rocksdb.NewLRUCache(3 << 30))
	opts := rocksdb.NewDefaultOptions()
	opts.SetBlockBasedTableFactory(bbto)
	opts.SetCreateIfMissing(true)
	db, err := rocksdb.OpenDb(opts, config.StoragePath)
	if err != nil {
		return nil, err
	}

	consumer, err := k.NewConsumer(&k.ConfigMap{
		"bootstrap.servers": config.Brokers,
		"group.id":          config.GroupID,
	})
	if err != nil {
		return nil, err
	}

	rl := &rebalanceListener{
		log: logWrapper,
	}
	consumer.Subscribe(config.Topic, func(c *k.Consumer, e k.Event) error {
		return rl.rebalance(c, e)
	})

	context, cancelFunc := context.WithCancel(config.Context)

	t := &Table{
		consumer: consumer,
		config:   config,
		db:       db,
		ctx:      context,
		cancel:   cancelFunc,
		finished: make(chan struct{}),
		log:      logWrapper,
	}
	go t.run()
	return t, nil
}

func (t *Table) Get(key []byte) []byte {
	opts := rocksdb.NewDefaultReadOptions()
	slice, _ := t.db.Get(opts, valueKey(key))
	return slice.Data()
}

type LogWrapper struct {
	logger *log.Logger
}

func (l *LogWrapper) log(level log.Level, format string, args ...interface{}) {
	if l != nil && l.logger != nil {
		l.logger.Logf(level, format, args...)
	}
}

func (t *Table) run() {
	opts := rocksdb.NewDefaultWriteOptions()
loop:
	for {
		select {
		case <-t.ctx.Done():
			break loop
		default:
		}
		e := t.consumer.Poll(1000)
		switch v := e.(type) {
		case *k.Message:
			t.db.Put(opts, valueKey(v.Key), v.Value)
			value := fmt.Sprintf("%v", v.TopicPartition.Offset)
			t.db.Put(opts, []byte(partitionKey(v.TopicPartition.Partition)), []byte(value))

			t.log.log(log.DebugLevel, "It was poll event: %v", v)
		case *k.Error:
			if v.Code() != k.ErrTimedOut {
				fmt.Printf("Error receiving message: %v\n", v)
			}
		default:
			t.log.log(log.DebugLevel, "Unknown event type: %v\n", v)
		}
		time.Sleep(2 * time.Second)
	}
	close(t.finished)
}
