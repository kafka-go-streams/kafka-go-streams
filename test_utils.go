package streams

import (
	"fmt"
	"math/rand"
	"time"
)

func makeTopic() string {
	rand.Seed(time.Now().Unix())
	return fmt.Sprintf("kafka-go-%016x", rand.Int63())
}
