package callRabbitMQ

import "time"

type reconnectionInfo struct{
	ReconnectionCounts int
	ReconnectionTime time.Duration
}

type handleErrRetryInfo struct{
	handleCount int
	handleTime time.Duration
	isRequeue bool
}
