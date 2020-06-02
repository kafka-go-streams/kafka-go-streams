package streams

import (
	"fmt"
	"log"

	k "github.com/confluentinc/confluent-kafka-go/kafka"
)

// RoutingConsumer allows you to have several processors that consume from different topics,
// but use the feature of copartitioning topics.
type RoutingConsumer struct {
	consumer      *k.Consumer
	currentTopics map[string]*Subscription
}

func NewRoutingConsumer(consumer *k.Consumer) *RoutingConsumer {
	c := &RoutingConsumer{
		consumer,
		make(map[string]*Subscription),
	}
	go c.run()
	return c
}

func (c *RoutingConsumer) run() {
	for {
		e := c.consumer.Poll(2000)
		switch v := e.(type) {
		case *k.Message:
			topic := *v.TopicPartition.Topic
			c.currentTopics[topic].c <- v
		case *k.Error:
			if v.Code() != k.ErrTimedOut {
				log.Printf("Error receiving message: %v\n", v)
			}
		default:
			log.Printf("Unknown event type: %v\n", v)
		}
	}
}

type RebalanceListener func(c *RoutingConsumer, e k.Event) error

func (c *RoutingConsumer) Subscribe(topics []string, rebalanceListener RebalanceListener) (*Subscription, error) {
	for _, t := range topics {
		if _, ok := c.currentTopics[t]; ok {
			return nil, fmt.Errorf("Already subscribed to topic: %s.", t)
		}
	}
	sub := &Subscription{make(chan k.Event), rebalanceListener}
	for _, t := range topics {
		c.currentTopics[t] = sub
	}
	var topicList []string
	for t, _ := range c.currentTopics {
		topicList = append(topicList, t)
	}
	err := c.consumer.SubscribeTopics(topicList, func(kc *k.Consumer, e k.Event) error {
		return c.rebalance(kc, e)
	})
	if err != nil {
		return nil, err
	}
	return sub, nil
}

func (c *RoutingConsumer) ResetOffsets(offsets []Offset) error {
	offsetMap := make(map[string]int64)
	for _, o := range offsets {
		offsetMap[o.Topic] = o.Offset
	}

	ps, err := c.consumer.Assignment()
	if err != nil {
		return err
	}
	for i := 0; i < len(ps); i++ {
		if newOffset, ok := offsetMap[*ps[i].Topic]; ok {
			ps[i].Offset = k.Offset(newOffset)
		}
	}
	return c.consumer.Assign(ps)
}

type Offset struct {
	Offset int64
	Topic  string
}

func (c *RoutingConsumer) rebalance(kc *k.Consumer, e k.Event) error {
	switch v := e.(type) {
	case k.AssignedPartitions:
		topics := make(map[string][]k.TopicPartition)
		for _, p := range v.Partitions {
			topics[*p.Topic] = append(topics[*p.Topic], p)
		}
		for t, p := range topics {
			c.currentTopics[t].rebalanceListener(c, k.AssignedPartitions{p})
		}
		log.Printf("Received partitions in routing consumer: %v", v)
	}
	return nil
}

type Subscription struct {
	c                 chan k.Event
	rebalanceListener RebalanceListener
}

func (s *Subscription) Poll() k.Event {
	return <-s.c
}
