package streams

import (
	"context"
)

// GlobalTable provides primitive for consuming kafka topic and storing it in a
// local rocksdb store.
type GlobalTable struct {
	TableBase
}

// GlobalTableConfig - configuration of global table.
type GlobalTableConfig struct {
	StoragePath string
	Brokers     string
	Topic       string
	Context     context.Context
}

// NewGlobalTable creates new global table from the config.
func NewGlobalTable(config *GlobalTableConfig) (*GlobalTable, error) {
	tableBase, err := NewTableBase(&TableBaseConfig{
		StoragePath: config.StoragePath,
		Brokers:     config.Brokers,
		Topic:       config.Topic,
		Handler:     idHandler,
		Context:     config.Context,
	})
	if err != nil {
		return nil, err
	}
	return &GlobalTable{*tableBase}, nil
}

func idHandler(p Pair) []Pair {
	return []Pair{p}
}
