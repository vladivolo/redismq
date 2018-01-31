package rmq

import ()

type Consumer interface {
	Consume(delivery Delivery)
}
