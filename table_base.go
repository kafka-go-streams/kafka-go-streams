package streams

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	rocksdb "github.com/tecbot/gorocksdb"
	kafka "gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

type TableBaseConfig struct {
	StoragePath string
	Brokers     string
	Topic       string
	Handler     Handler
	Context     context.Context
}

type TableBase struct {
	db       *rocksdb.DB
	consumer *kafka.Consumer
	ctx      context.Context
	cancel   context.CancelFunc
	finished chan struct{}
}

type Handler func(p Pair) []Pair

type Pair struct {
	Key   []byte
	Value []byte
}

func NewTableBase(config *TableBaseConfig) (*TableBase, error) {
	bbto := rocksdb.NewDefaultBlockBasedTableOptions()
	bbto.SetBlockCache(rocksdb.NewLRUCache(3 << 30))
	opts := rocksdb.NewDefaultOptions()
	opts.SetBlockBasedTableFactory(bbto)
	opts.SetCreateIfMissing(true)
	db, err := rocksdb.OpenDb(opts, config.StoragePath)
	if err != nil {
		return nil, err
	}
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": config.Brokers,
		"group.id":          "Useless empty group",
	})
	if err != nil {
		return nil, err
	}

	err = restoreOffsets(db, consumer, config.Topic)
	if err != nil {
		return nil, err
	}

	context, cancelFunc := context.WithCancel(config.Context)
	tb := &TableBase{db, consumer, context, cancelFunc, make(chan struct{})}
	go tb.run()
	return tb, nil
}

func restoreOffsets(db *rocksdb.DB, consumer *kafka.Consumer, topic string) error {
	meta, err := consumer.GetMetadata(&topic, false, 1000)
	if err != nil {
		return err
	}
	topicMeta, ok := meta.Topics[topic]
	if !ok {
		return errors.New("Topic is not known to the broker")
	}
	assignment := make([]kafka.TopicPartition, len(topicMeta.Partitions))
	for i, p := range topicMeta.Partitions {
		offset, err := getOffset(db, p.ID)
		if err != nil {
			return err
		}
		assignment[i] = kafka.TopicPartition{
			Topic:     &topic,
			Partition: p.ID,
			Offset:    kafka.Offset(offset),
		}
	}
	err = consumer.Assign(assignment)
	if err != nil {
		return err
	}
	return nil
}

func partitionKey(partition int32) string {
	return fmt.Sprintf("partition-offset-%v", partition)
}

func getOffset(db *rocksdb.DB, partition int32) (kafka.Offset, error) {
	opts := rocksdb.NewDefaultReadOptions()
	slice, _ := db.Get(opts, []byte(partitionKey(partition)))
	if !slice.Exists() {
		return kafka.OffsetBeginning, nil
	}
	intOffset, err := strconv.ParseInt(string(slice.Data()), 0, 64)
	return kafka.Offset(intOffset), err

}

func (tb *TableBase) run() {
	opts := rocksdb.NewDefaultWriteOptions()
loop:
	for {
		select {
		case <-tb.ctx.Done():
			break loop
		default:
		}
		msg, err := tb.consumer.ReadMessage(5 * time.Second)
		if err != nil {
			if err.(kafka.Error).Code() != kafka.ErrTimedOut {
				fmt.Printf("Error receiving message: %v\n", err)
			}
		} else {
			tb.db.Put(opts, []byte(msg.Key), []byte(msg.Value))
			value := fmt.Sprintf("%v", msg.TopicPartition.Offset)
			tb.db.Put(opts, []byte(partitionKey(msg.TopicPartition.Partition)), []byte(value))
		}
		time.Sleep(2 * time.Second)
	}
	close(tb.finished)
}

func (tb *TableBase) Get(key []byte) []byte {
	opts := rocksdb.NewDefaultReadOptions()
	slice, _ := tb.db.Get(opts, key)
	return slice.Data()
}

func (tb *TableBase) Close() error {
	tb.cancel()
	<-tb.finished
	tb.db.Close()
	return tb.consumer.Close()
}
