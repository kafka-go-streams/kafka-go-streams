package streams

import (
	"context"
)

type GlobalTable struct {
	TableBase
}

type GlobalTableConfig struct {
	StoragePath string
	Brokers     string
	Topic       string
	Context     context.Context
}

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
