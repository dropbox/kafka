package kafka

import (
	"errors"
	"io"
	"net"
	"syscall"
	"time"

	"github.com/dropbox/kafka/proto"
	"github.com/jpillora/backoff"
)

type ProducerConf struct {
	// Compression method to use, defaulting to proto.CompressionNone.
	Compression proto.Compression

	// Timeout of single produce request. By default, 5 seconds.
	RequestTimeout time.Duration

	// Message ACK configuration. Use proto.RequiredAcksAll to require all
	// servers to write, proto.RequiredAcksLocal to wait only for leader node
	// answer or proto.RequiredAcksNone to not wait for any response.
	// Setting this to any other, greater than zero value will make producer to
	// wait for given number of servers to confirm write before returning.
	RequiredAcks int16

	// RetryLimit specify how many times message producing should be retried in
	// case of failure, before returning the error to the caller. By default
	// set to 10.
	RetryLimit int

	// RetryWait specify wait duration before produce retry after failure. This
	// is subject to exponential backoff.
	//
	// Defaults to 200ms.
	RetryWait time.Duration
}

// NewProducerConf returns a default producer configuration.
func NewProducerConf() ProducerConf {
	return ProducerConf{
		Compression:    proto.CompressionNone,
		RequestTimeout: 5 * time.Second,
		RequiredAcks:   proto.RequiredAcksAll,
		RetryLimit:     10,
		RetryWait:      200 * time.Millisecond,
	}
}

// producer is the link to the client with extra configuration.
type producer struct {
	conf   ProducerConf
	broker *Broker
}

// Producer returns new producer instance, bound to the broker.
func (b *Broker) Producer(conf ProducerConf) Producer {
	return &producer{
		conf:   conf,
		broker: b,
	}
}

// Produce writes messages to the given destination. Writes within the call are
// atomic, meaning either all or none of them are written to kafka.  Produce
// has a configurable amount of retries which may be attempted when common
// errors are encountered.  This behaviour can be configured with the
// RetryLimit and RetryWait attributes.
//
// Upon a successful call, the message's Offset field is updated.
func (p *producer) Produce(
	topic string, partition int32, messages ...*proto.Message) (offset int64, err error) {

	retry := &backoff.Backoff{Min: p.conf.RetryWait, Jitter: true}
retryLoop:
	for try := 0; try < p.conf.RetryLimit; try++ {
		if try != 0 {
			time.Sleep(retry.Duration())
		}

		offset, err = p.produce(topic, partition, messages...)

		switch err {
		case nil:
			break retryLoop
		case io.EOF, syscall.EPIPE:
			// p.produce call is closing connection when this error shows up,
			// but it's also returning it so that retry loop can count this
			// case
			// we cannot handle this error here, because there is no direct
			// access to connection
		default:
			if err := p.broker.metadata.Refresh(); err != nil {
				log.Debugf("cannot refresh metadata: %s", err)
			}
		}
		log.Debugf("cannot produce messages (try %d): %s", retry, err)
	}

	if err == nil {
		// offset is the offset value of first published messages
		for i, msg := range messages {
			msg.Offset = int64(i) + offset
		}
	}

	return offset, err
}

// produce send produce request to leader for given destination.
func (p *producer) produce(
	topic string, partition int32, messages ...*proto.Message) (offset int64, err error) {

	conn, err := p.broker.leaderConnection(topic, partition)
	if err != nil {
		return 0, err
	}
	defer func(lconn *connection) { go p.broker.conns.Idle(lconn) }(conn)

	req := proto.ProduceReq{
		ClientID:     p.broker.conf.ClientID,
		Compression:  p.conf.Compression,
		RequiredAcks: p.conf.RequiredAcks,
		Timeout:      p.conf.RequestTimeout,
		Topics: []proto.ProduceReqTopic{
			{
				Name: topic,
				Partitions: []proto.ProduceReqPartition{
					{
						ID:       partition,
						Messages: messages,
					},
				},
			},
		},
	}

	resp, err := conn.Produce(&req)
	if err != nil {
		if _, ok := err.(*net.OpError); ok || err == io.EOF || err == syscall.EPIPE {
			// Connection is broken, so should be closed, but the error is
			// still valid and should be returned so that retry mechanism have
			// chance to react.
			log.Debugf("connection died while sending message to %s:%d: %s",
				topic, partition, err)
			conn.Close()
		}
		return 0, err
	}

	// No response if we've asked for no acks
	if req.RequiredAcks == proto.RequiredAcksNone {
		return 0, err
	}

	// Presently we only handle producing to a single topic/partition so return it as
	// soon as we've found it
	for _, t := range resp.Topics {
		for _, p := range t.Partitions {
			if t.Name != topic || p.ID != partition {
				log.Warningf("produce response with unexpected data for %s:%d",
					t.Name, p.ID)
				continue
			}

			return p.Offset, p.Err
		}
	}

	// If we get here we didn't find the topic/partition in the response, this is an
	// error condition of some kind
	return 0, errors.New("incomplete produce response")
}
