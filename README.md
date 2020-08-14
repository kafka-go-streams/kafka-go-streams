A library for go with primitives that allow you to build structures similar to
the ones provided in kafka streams.

Designing principle of this library is to produce elemental composable
primitives for constructing streams. For example, original kafka streams don't
allow us to construct streams that join streams with keys of different nature
or require too much jumping through the hoops to achieve it.

Table Base
----------

Table base is a primitive that allows you to connect to a topic and start
consuming from it. The consumer will consume from all partitions of the
provided topic. The output of your processor will be stored in a local store.
Offsets will be stored in the local store as well.

Global Table
------------

Is a simple wrapper around table base. It configures table base with a simple
processor that just passes values to the store.

Constructing a global ktable:

```
// Start table
tb, err := NewGlobalTable(&GlobalTableConfig{
	StoragePath: "test_database",
	Brokers:     "localhost",
	Topic:       topic,
	Context:     context.Background(),
})
if err != nil {
	t.Fatalf("Failed: %v", err)
}
defer tb.Close()
```

Table
-----

Table is a primitive that is configured with group id. If the application
is started on several nodes then each node will handle only a subset
of partitions. Table maintains a compacted change log topic with the latest
values received in the source topic.

The following code demonstrates how you can construct a table and a consumer
that are co-partitioned. Anything that is published to the `table_topic` will
be stored locally in the rocksDB store.
Whenever a value from `input_topic` it gets joined with the value from the table.

Starting another instance of this code will rebalance the partitions between the
old instance and the new instance.

```
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
	Topic:    "table_topic",
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
```
