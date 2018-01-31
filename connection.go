package rmq

import (
	"fmt"
	"github.com/dchest/uniuri"
	"gopkg.in/redis.v3"
	"strings"
)

const (
	connectionHeartbeatTemplate = "rmq::connection::{connection}::heartbeat"
	phConnection                = "{connection}"
)

type Connection interface {
	OpenQueue(name string) Queue
}

type redisConnection struct {
	Name        string
	redisClient *redis.Client
	queue       map[string]*redisQueue
}

func openConnectionWithRedisClient(tag string, redisClient *redis.Client) *redisConnection {
	conn := &redisConnection{
		Name:        fmt.Sprintf("%s-%s", tag, uniuri.NewLen(6)),
		redisClient: redisClient,
		queue:       make(map[string]*redisQueue),
	}

	return conn
}

func OpenConnection(tag, network, address string, db int) (*redisConnection, error) {
	redisClient := redis.NewClient(&redis.Options{
		Network: network,
		Addr:    address,
		DB:      int64(db),
	})

	rc := openConnectionWithRedisClient(tag, redisClient)

	return rc, rc.Check()
}

func (rc *redisConnection) OpenQueue(name string) Queue {
	rq := newQueue(name, rc)

	rc.queue[name] = rq

	return rq
}

func (rc *redisConnection) Check() error {
	heartbeatKey := strings.Replace(connectionHeartbeatTemplate, phConnection, rc.Name, 1)

	return rc.redisClient.TTL(heartbeatKey).Err()
}
