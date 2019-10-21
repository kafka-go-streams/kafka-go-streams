package main

import (
	"context"
	"os"

	streams "github.com/kafka-go-streams/kafka-go-streams"
	log "github.com/sirupsen/logrus"
)

func main() {

	logFile, err := os.Create("log.txt")
	if err != nil {
		log.Fatalf("Failed to open log file")
	}

	log := &log.Logger{
		Out:       logFile,
		Formatter: new(log.JSONFormatter),
		Hooks:     make(log.LevelHooks),
		Level:     log.DebugLevel,
	}
	table, err := streams.NewTable(&streams.TableConfig{
		StoragePath: "table.db",
		Brokers:     "localhost:9092",
		GroupID:     "my_test_group",
		Topic:       "test_topic",
		Context:     context.Background(),
		Logger:      log,
	})

	if err != nil {
		log.Fatalf("Failed to construct table: %v", err)
	}

	v := table.Get([]byte("key"))
	log.Infof("%v", v)

	c := make(chan int)
	<-c
}
