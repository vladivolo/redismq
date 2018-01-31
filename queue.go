package rmq

import (
	"fmt"
	_ "github.com/dchest/uniuri"
	_ "gopkg.in/redis.v3"
	"strings"
	"time"
)

const (
	queueReadyTemplate    = "rmq::queue::[{queue}]::ready"
	queueRejectedTemplate = "rmq::queue::[{queue}]::rejected"
	queueAckTemplate      = "rmq::queue::[{queue}]::ack"
	queueUnackedTemplate  = "rmq::queue::[{queue}]::inprocess"

	phQueue    = "{queue}"
	phConsumer = "{consumer}"
)

type Queue interface {
	Publish(payload string) error
	Ack(payload string) error
	Reject(payload string) error
	StartConsuming(duration time.Duration, consumer Consumer) error
}

type redisQueue struct {
	name         string
	readyKey     string // main queue, wait consumers
	rejectedKey  string // rejected
	ackedKey     string // ack
	unackedKey   string // unacked ( ready -> unacked -> (ack || reject) )
	redis_conn   *redisConnection
	deliveryChan chan Delivery
	pollDuration time.Duration
}

func newQueue(name string, rc *redisConnection) *redisQueue {
	rq := &redisQueue{
		name:         name,
		readyKey:     strings.Replace(queueReadyTemplate, phQueue, name, 1),
		rejectedKey:  strings.Replace(queueRejectedTemplate, phQueue, name, 1),
		ackedKey:     strings.Replace(queueAckTemplate, phQueue, name, 1),
		unackedKey:   strings.Replace(queueUnackedTemplate, phQueue, name, 1),
		redis_conn:   rc,
		deliveryChan: nil,
	}

	return rq
}

func (rq *redisQueue) Name() string {
	return rq.name
}

func (rq *redisQueue) Publish(payload string) error {
	return rq.redis_conn.redisClient.LPush(rq.readyKey, payload).Err()
}

func (rq *redisQueue) StartConsuming(duration time.Duration, consumer Consumer) error {
	if rq.deliveryChan != nil {
		return fmt.Errorf("Consumer already working")
	}

	rq.pollDuration = duration
	rq.deliveryChan = make(chan Delivery, 16)

	go rq.consume()
	go rq.consumerHandler(consumer)

	fmt.Printf("StartConsuming: Queue %s", rq.Name)

	return nil
}

func (rq *redisQueue) ReturnRejected(count int) int {
	for i := 0; i < count; i++ {
		if rq.redis_conn.redisClient.RPopLPush(rq.rejectedKey, rq.readyKey).Err() != nil {
			return i
		}
	}
	return count
}

func (rq *redisQueue) msgCount(queue_name string) int {
	result := rq.redis_conn.redisClient.LLen(queue_name)
	if result.Err() != nil {
		return 0
	}
	return int(result.Val())
}

func (rq *redisQueue) ReadyCount() int {
	return rq.msgCount(rq.readyKey)
}

func (rq *redisQueue) UnackedCount() int {
	return rq.msgCount(rq.unackedKey)
}

func (rq *redisQueue) RejectedCount() int {
	return rq.msgCount(rq.rejectedKey)
}

func (rq *redisQueue) Ack(payload string) error {
	return rq.redis_conn.redisClient.LRem(rq.unackedKey, 1, payload).Err()
}

func (rq *redisQueue) Reject(payload string) error {
	if err := rq.redis_conn.redisClient.LPush(rq.rejectedKey, payload).Err(); err != nil {
		return err
	}

	if err := rq.redis_conn.redisClient.LRem(rq.unackedKey, 1, payload).Err(); err != nil {
		return err
	}

	return nil
}

func (rq *redisQueue) consume() {
	for {
		for {
			result := rq.redis_conn.redisClient.RPopLPush(rq.readyKey, rq.unackedKey)
			if result.Err() != nil {
				break
			}
			rq.deliveryChan <- newDelivery(result.Val(), rq)
		}
		time.Sleep(rq.pollDuration)
	}
}

func (rq *redisQueue) consumerHandler(consumer Consumer) {
	for {
		select {
		case delivery, ok := <-rq.deliveryChan:
			if !ok {
				return
			}

			consumer.Consume(delivery)
		}
	}
}
