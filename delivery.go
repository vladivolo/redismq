package rmq

import ()

type Delivery interface {
	Payload() string
	Ack() error
	Reject() error
	QueueName() string
}

type wrapDelivery struct {
	payload string
	rq      *redisQueue
}

func newDelivery(payload string, queue *redisQueue) *wrapDelivery {
	wd := &wrapDelivery{
		payload: payload,
		rq:      queue,
	}

	return wd
}

func (delivery *wrapDelivery) Payload() string {
	return delivery.payload
}

func (delivery *wrapDelivery) Ack() error {
	return delivery.rq.Ack(delivery.Payload())
}

func (delivery *wrapDelivery) Reject() error {
	return delivery.rq.Reject(delivery.Payload())
}

func (delivery *wrapDelivery) QueueName() string {
	return delivery.rq.Name()
}
