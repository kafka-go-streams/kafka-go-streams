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
	changelogTopicSuffix = "-changelog"
)

func changelogTopicName(groupID, storageName string) string {
	return groupID + "-" + storageName + changelogTopicSuffix
}

// TableConfig is a structure for configuring table.
type TableConfig struct {
	StoragePath string
	Brokers     string
	GroupID     string
	Topic       string
	DB          *rocksdb.DB
	Context     context.Context
	Logger      *log.Logger
	Name        string
}

// Table is a primitive for working with distributed tables.
type Table struct {
	consumer *k.Consumer
	producer *k.Producer
	config   *TableConfig
	db       *rocksdb.DB
	ctx      context.Context
	cancel   context.CancelFunc
	finished chan struct{}
	log      *LogWrapper
}

type rebalanceListener struct {
	changelogTopicName string
	log                *LogWrapper
}

func (l *rebalanceListener) rebalance(c *k.Consumer, e k.Event) error {
	switch v := e.(type) {
	case k.AssignedPartitions:
		l.log.Debugf("Recovering the partition from assignment: %v", v)
		for _, p := range v.Partitions {
			if *p.Topic == l.changelogTopicName {
				p.Offset = k.OffsetBeginning
				l.log.Debugf("Resetting partition: %v", p)
				c.Seek(p, 10000)
			}
		}
	case k.RevokedPartitions:
		l.log.Debugf("It was revoked partitions event: %v", v)
	default:
		l.log.Debugf("Unknown rebalance event: %v", e)
	}
	return nil
}

func DefaultRocksDB(storagePath string) (*rocksdb.DB, error) {
	bbto := rocksdb.NewDefaultBlockBasedTableOptions()
	bbto.SetBlockCache(rocksdb.NewLRUCache(3 << 30))
	opts := rocksdb.NewDefaultOptions()
	opts.SetBlockBasedTableFactory(bbto)
	opts.SetCreateIfMissing(true)
	return rocksdb.OpenDb(opts, storagePath)
}

// NewTable constructs a new table.
func NewTable(config *TableConfig) (t *Table, err error) {

	logWrapper := &LogWrapper{config.Logger}

	var db *rocksdb.DB
	if config.DB == nil {
		db, err = DefaultRocksDB(config.GroupID + ".db")
		if err != nil {
			return nil, err
		}
	} else {
		db = config.DB
	}

	// consumer
	consumer, err := k.NewConsumer(&k.ConfigMap{
		"bootstrap.servers":  config.Brokers,
		"group.id":           config.GroupID,
		"enable.auto.commit": false,
	})
	if err != nil {
		return nil, err
	}

	// producer
	producer, err := k.NewProducer(&k.ConfigMap{
		"bootstrap.servers": config.Brokers,
	})

	// context
	context, cancelFunc := context.WithCancel(config.Context)

	t = &Table{
		consumer: consumer,
		producer: producer,
		config:   config,
		db:       db,
		ctx:      context,
		cancel:   cancelFunc,
		finished: make(chan struct{}),
		log:      logWrapper,
	}

	// Crate changelog topic
	changelogTopicName := changelogTopicName(t.config.GroupID, t.config.Name)
	adminClient, err := k.NewAdminClient(&k.ConfigMap{
		"bootstrap.servers": config.Brokers,
	})
	if err != nil {
		return nil, err
	}
	t.log.Tracef("Change log topic name: %v", changelogTopicName)
	metadata, err := adminClient.GetMetadata(&config.Topic, false, 10)
	if err != nil {
		return nil, err
	}
	originalTopicNumPartitions := len(metadata.Topics[config.Topic].Partitions)
	originalTopicReplicationFactor := len(metadata.Topics[config.Topic].Partitions[0].Replicas)
	logWrapper.Debugf("%v", metadata.Topics)
	topicSpec := k.TopicSpecification{
		Topic:             changelogTopicName,
		NumPartitions:     originalTopicNumPartitions,
		ReplicationFactor: originalTopicReplicationFactor,
		Config:            map[string]string{"cleanup.policy": "compact"},
	}
	_, err = adminClient.CreateTopics(config.Context, []k.TopicSpecification{topicSpec})
	if err != nil {
		return nil, err
	}

	rl := &rebalanceListener{
		changelogTopicName: changelogTopicName,
		log:                logWrapper,
	}
	consumer.SubscribeTopics([]string{config.Topic, changelogTopicName}, func(c *k.Consumer, e k.Event) error {
		return rl.rebalance(c, e)
	})
	go t.run(changelogTopicName)
	return t, nil
}

func (t *Table) Get(key []byte) []byte {
	opts := rocksdb.NewDefaultReadOptions()
	slice, _ := t.db.Get(opts, valueKey(key))
	return slice.Data()
}

func (t *Table) run(changelogTopicName string) {
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
			t.log.Debugf("Storing in the database: %v: %v", string(valueKey(v.Key)), string(v.Value))
			if *v.TopicPartition.Topic == changelogTopicName {
				err := t.db.Put(opts, valueKey(v.Key), v.Value)
				if err != nil {
					t.log.Errorf("Failed to store key value in the store. Aborting consumer loop.")
					break loop
				}
			} else {
				t.log.Debugf("Handling non changelog entry")
				value := fmt.Sprintf("%v", v.TopicPartition.Offset)
				err := t.db.Put(opts, []byte(partitionKey(v.TopicPartition.Partition)), []byte(value))
				if err != nil {
					t.log.Errorf("Failed to store key value in the store. Aborting consumer loop.")
					break loop
				}
				deliveryChan := make(chan k.Event)
				t.producer.Produce(&k.Message{
					TopicPartition: k.TopicPartition{Topic: &changelogTopicName, Partition: k.PartitionAny},
					Key:            v.Key,
					Value:          v.Value,
				}, deliveryChan)
				_, err = t.consumer.CommitMessage(v)
				if err != nil {
					t.log.Warnf("Failed to commit offset. Continuing consumer loop in hope to commit offset on the next iteration.")
				}
			}

			t.log.Debugf("It was poll event: %v", v)
		case *k.Error:
			if v.Code() != k.ErrTimedOut {
				fmt.Printf("Error receiving message: %v\n", v)
			}
		default:
			t.log.Debugf("Unknown event type: %v\n", v)
		}
		time.Sleep(2 * time.Second)
	}
	close(t.finished)
}
