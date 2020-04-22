package streams

import (
	"context"
	"fmt"

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
	Brokers string
	GroupID string
	Topic   string
	DB      *rocksdb.DB
	Context context.Context
	Logger  *log.Logger
	Name    string
}

// Table is a primitive for working with distributed tables.
type Table struct {
	consumer           *k.Consumer
	changelogConsumer  *k.Consumer
	producer           *k.Producer
	config             *TableConfig
	db                 *rocksdb.DB
	ctx                context.Context
	cancel             context.CancelFunc
	finished           chan struct{}
	log                *LogWrapper
	changelogTopicName string
}

type rebalanceListener struct {
	changelogTopicName string
	changelogConsumer  *k.Consumer
	log                *LogWrapper
}

func (l *rebalanceListener) rebalance(c *k.Consumer, e k.Event) error {
	switch v := e.(type) {
	case k.AssignedPartitions:
		l.log.Debugf("Recovering the partition from assignment: %v", v)
		for i := 0; i < len(v.Partitions); i++ {
			v.Partitions[i].Topic = &l.changelogTopicName
			v.Partitions[i].Offset = k.OffsetBeginning
		}
		err := l.changelogConsumer.Assign(v.Partitions)
		if err != nil {
			l.log.Errorf("Failed to assign changelog partitions: %v", err)
		}
		l.log.Debugf("Successfully assigned partitions to changelog reader.")
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

	if config.DB == nil {
		return nil, fmt.Errorf("Rocks db is expected. Use DefaultRocksDB function to construct default value.")
	}
	db := config.DB

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

	// Crate changelog topic
	changelogTopicName := changelogTopicName(config.GroupID, config.Name)
	adminClient, err := k.NewAdminClient(&k.ConfigMap{
		"bootstrap.servers": config.Brokers,
	})
	if err != nil {
		return nil, err
	}
	logWrapper.Tracef("Change log topic name: %v", changelogTopicName)
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

	// Changelog consumer

	changelogConsumer, err := k.NewConsumer(&k.ConfigMap{
		"bootstrap.servers":  config.Brokers,
		"group.id":           config.GroupID,
		"enable.auto.commit": false,
	})
	if err != nil {
		return nil, err
	}

	// Subscribe to the main topic
	rl := &rebalanceListener{
		changelogTopicName: changelogTopicName,
		changelogConsumer:  changelogConsumer,
		log:                logWrapper,
	}

	consumer.Subscribe(config.Topic, func(c *k.Consumer, e k.Event) error {
		return rl.rebalance(c, e)
	})

	// Construct table
	t = &Table{
		consumer:           consumer,
		changelogConsumer:  changelogConsumer,
		producer:           producer,
		config:             config,
		db:                 db,
		ctx:                context,
		cancel:             cancelFunc,
		finished:           make(chan struct{}),
		log:                logWrapper,
		changelogTopicName: changelogTopicName,
	}

	go t.consumeTopic()
	go t.consumeChangelog()
	return t, nil
}

func (t *Table) Get(key []byte) []byte {
	opts := rocksdb.NewDefaultReadOptions()
	defer opts.Destroy()
	slice, _ := t.db.Get(opts, valueKey(key)) // TODO destroy slice
	return slice.Data()
}

func (t *Table) consumeTopic() {
loop:
	for {
		select {
		case <-t.ctx.Done():
			break loop
		default:
		}
		e := t.consumer.Poll(2000)
		switch v := e.(type) {
		case *k.Message:
			t.log.Debugf("Passing to change log")

			deliveryChan := make(chan k.Event)
			t.producer.Produce(&k.Message{
				TopicPartition: k.TopicPartition{Topic: &t.changelogTopicName, Partition: v.TopicPartition.Partition},
				Key:            v.Key,
				Value:          v.Value,
			}, deliveryChan)

			de := <-deliveryChan
			switch dev := de.(type) {
			case *k.Message:
				t.log.Debugf("Delivery channel message: %v", dev)
			case *k.Error:
				t.log.Errorf("Delivery channel error: %v", dev)
			default:
				t.log.Warnf("Unknown reply from delivery: %v", dev)
			}

			_, err := t.consumer.CommitMessage(v)
			if err != nil {
				t.log.Warnf("Failed to commit offset. Continuing consumer loop in hope to commit offset on the next iteration.")
			}
		case *k.Error:
			if v.Code() != k.ErrTimedOut {
				t.log.Errorf("Error receiving message: %v\n", v)
			}
		default:
			t.log.Debugf("Unknown event type: %v\n", v)
		}
	}
}

func (t *Table) consumeChangelog() {
	opts := rocksdb.NewDefaultWriteOptions()
	defer opts.Destroy()
loop:
	for {
		select {
		case <-t.ctx.Done():
			break loop
		default:
		}
		e := t.changelogConsumer.Poll(2000)
		switch v := e.(type) {
		case *k.Message:
			key := valueKey(v.Key)
			val := v.Value
			t.log.Tracef("Handling message: %v: %v", string(key), string(val))

			err := t.db.Put(opts, key, val)
			if err != nil {
				t.log.Errorf("Failed to store message in the local store")
				break loop
			}

			t.log.Debugf("It was poll event: %v", v)
		case *k.Error:
			if v.Code() != k.ErrTimedOut {
				fmt.Printf("Error receiving message: %v\n", v)
			}
		default:
			t.log.Debugf("Unknown event type: %v\n", v)
		}
	}
	close(t.finished)
}
