package kafka

import (
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"syscall"
	"time"

	"github.com/dropbox/kafka/proto"
	"github.com/jpillora/backoff"
)

// Consumer is the interface that wraps the Consume method.
//
// Consume reads a message from a consumer, returning an error when
// encountered.
type Consumer interface {
	Consume() (*proto.Message, error)
}

// BatchConsumer is the interface that wraps the ConsumeBatch method.
//
// ConsumeBatch reads a batch of messages from a consumer, returning an error
// when encountered.
type BatchConsumer interface {
	ConsumeBatch() ([]*proto.Message, error)
}

type ConsumerConf struct {
	// Topic name that should be consumed
	Topic string

	// Partition ID that should be consumed.
	Partition int32

	// RequestTimeout controls fetch request timeout. This operation is
	// blocking the whole connection, so it should always be set to a small
	// value. By default it's set to 50ms.
	// To control fetch function timeout use RetryLimit and RetryWait.
	RequestTimeout time.Duration

	// RetryLimit limits fetching messages a given amount of times before
	// returning ErrNoData error.
	//
	// Default is -1, which turns this limit off.
	RetryLimit int

	// RetryWait controls the duration of wait between fetch request calls,
	// when no data was returned. This follows an exponential backoff model
	// so that we don't overload servers that have very little data.
	//
	// Default is 50ms.
	RetryWait time.Duration

	// RetryErrLimit limits the number of retry attempts when an error is
	// encountered.
	//
	// Default is 10.
	RetryErrLimit int

	// RetryErrWait controls the wait duration between retries after failed
	// fetch request. This follows the exponential backoff curve.
	//
	// Default is 500ms.
	RetryErrWait time.Duration

	// MinFetchSize is the minimum size of messages to fetch in bytes.
	//
	// Default is 1 to fetch any message available.
	MinFetchSize int32

	// MaxFetchSize is the maximum size of data which can be sent by kafka node
	// to consumer.
	//
	// Default is 2000000 bytes.
	MaxFetchSize int32

	// Consumer cursor starting point. Set to StartOffsetNewest to receive only
	// newly created messages or StartOffsetOldest to read everything. Assign
	// any offset value to manually set cursor -- consuming starts with the
	// message whose offset is equal to given value (including first message).
	//
	// Default is StartOffsetOldest.
	StartOffset int64
}

// NewConsumerConf returns the default consumer configuration.
func NewConsumerConf(topic string, partition int32) ConsumerConf {
	return ConsumerConf{
		Topic:          topic,
		Partition:      partition,
		RequestTimeout: time.Millisecond * 50,
		RetryLimit:     -1,
		RetryWait:      time.Millisecond * 50,
		RetryErrLimit:  10,
		RetryErrWait:   time.Millisecond * 500,
		MinFetchSize:   1,
		MaxFetchSize:   2000000,
		StartOffset:    StartOffsetOldest,
	}
}

// Consumer represents a single partition reading buffer. Consumer is also
// providing limited failure handling and message filtering.
type consumer struct {
	broker *Broker
	conf   ConsumerConf

	// mu protects the following and must not be used outside of consumer.
	mu     *sync.Mutex
	offset int64 // offset of next NOT consumed message
	msgbuf []*proto.Message
}

// Consumer creates a new consumer instance, bound to the broker.
func (b *Broker) Consumer(conf ConsumerConf) (Consumer, error) {
	return b.consumer(conf)
}

// BatchConsumer creates a new BatchConsumer instance, bound to the broker.
func (b *Broker) BatchConsumer(conf ConsumerConf) (BatchConsumer, error) {
	return b.consumer(conf)
}

func (b *Broker) consumer(conf ConsumerConf) (*consumer, error) {
	offset := conf.StartOffset
	if offset < 0 {
		switch offset {
		case StartOffsetNewest:
			off, err := b.OffsetLatest(conf.Topic, conf.Partition)
			if err != nil {
				return nil, err
			}
			offset = off
		case StartOffsetOldest:
			off, err := b.OffsetEarliest(conf.Topic, conf.Partition)
			if err != nil {
				return nil, err
			}
			offset = off
		default:
			return nil, fmt.Errorf("invalid start offset: %d", conf.StartOffset)
		}
	}
	c := &consumer{
		broker: b,
		mu:     &sync.Mutex{},
		conf:   conf,
		msgbuf: make([]*proto.Message, 0),
		offset: offset,
	}
	return c, nil
}

// consume is returning a batch of messages from consumed partition.
// Consumer can retry fetching messages even if responses return no new
// data. Retry behaviour can be configured through RetryLimit and RetryWait
// consumer parameters.
//
// consume can retry sending request on common errors. This behaviour can
// be configured with RetryErrLimit and RetryErrWait consumer configuration
// attributes.
func (c *consumer) consume() ([]*proto.Message, error) {
	var msgbuf []*proto.Message
	var retry int
	for len(msgbuf) == 0 {
		var err error
		msgbuf, err = c.fetch()
		if err != nil {
			return nil, err
		}
		if len(msgbuf) == 0 {
			retry += 1
			if c.conf.RetryLimit != -1 && retry > c.conf.RetryLimit {
				return nil, ErrNoData
			}
			if c.conf.RetryWait > 0 {
				time.Sleep(c.conf.RetryWait)
			}
		}
	}

	return msgbuf, nil
}

func (c *consumer) Consume() (*proto.Message, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.msgbuf) == 0 {
		var err error
		c.msgbuf, err = c.consume()
		if err != nil {
			return nil, err
		}
	}

	msg := c.msgbuf[0]
	c.msgbuf = c.msgbuf[1:]
	c.offset = msg.Offset + 1
	return msg, nil
}

func (c *consumer) ConsumeBatch() ([]*proto.Message, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	batch, err := c.consume()
	if err != nil {
		return nil, err
	}
	c.offset = batch[len(batch)-1].Offset + 1

	return batch, nil
}

// fetch and return next batch of messages. In case of certain set of errors,
// retry sending fetch request. Retry behaviour can be configured with
// RetryErrLimit and RetryErrWait consumer configuration attributes.
func (c *consumer) fetch() ([]*proto.Message, error) {
	req := proto.FetchReq{
		ClientID:    c.broker.conf.ClientID,
		MaxWaitTime: c.conf.RequestTimeout,
		MinBytes:    c.conf.MinFetchSize,
		Topics: []proto.FetchReqTopic{
			{
				Name: c.conf.Topic,
				Partitions: []proto.FetchReqPartition{
					{
						ID:          c.conf.Partition,
						FetchOffset: c.offset,
						MaxBytes:    c.conf.MaxFetchSize,
					},
				},
			},
		},
	}

	var resErr error
	retry := &backoff.Backoff{Min: c.conf.RetryErrWait, Jitter: true}
consumeRetryLoop:
	for try := 0; try < c.conf.RetryErrLimit; try++ {
		if try != 0 {
			time.Sleep(retry.Duration())
		}

		conn, err := c.broker.leaderConnection(c.conf.Topic, c.conf.Partition)
		if err != nil {
			resErr = err
			continue
		}
		defer func(lconn *connection) { go c.broker.conns.Idle(lconn) }(conn)

		resp, err := conn.Fetch(&req)
		resErr = err
		if _, ok := err.(*net.OpError); ok || err == io.EOF || err == syscall.EPIPE {
			log.Debugf("connection died while fetching messages from %s:%d: %s",
				c.conf.Topic, c.conf.Partition, err)
			conn.Close()
			continue
		}

		if err != nil {
			log.Debugf("cannot fetch messages (try %d): %s", retry, err)
			conn.Close()
			continue
		}

		// Should only be a single topic/partition in the response, the one we asked about.
		for _, t := range resp.Topics {
			for _, p := range t.Partitions {
				if t.Name != c.conf.Topic || p.ID != c.conf.Partition {
					log.Warningf("fetch response with unexpected data for %s:%d",
						t.Name, p.ID)
					continue
				}

				switch p.Err {
				case proto.ErrLeaderNotAvailable, proto.ErrNotLeaderForPartition,
					proto.ErrBrokerNotAvailable:
					// Failover happened, so we probably need to talk to a different broker. Let's
					// kick off a metadata refresh.
					log.Debugf("cannot fetch messages (try %d): %s", retry, p.Err)
					if err := c.broker.metadata.Refresh(); err != nil {
						log.Debugf("cannot refresh metadata: %s", err)
					}
					continue consumeRetryLoop
				}
				return p.Messages, p.Err
			}
		}
		return nil, errors.New("incomplete fetch response")
	}

	return nil, resErr
}
