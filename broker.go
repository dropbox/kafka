package kafka

import (
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net"
	"sync"
	"syscall"
	"time"

	"github.com/dropbox/kafka/proto"
	"github.com/jpillora/backoff"
)

const (
	// StartOffsetNewest configures the consumer to fetch messages produced
	// after creating the consumer.
	StartOffsetNewest = -1

	// StartOffsetOldest configures the consumer to fetch starting from the
	// oldest message available.
	StartOffsetOldest = -2
)

var (
	// Logger used by the various components of the library
	log Logger = &nullLogger{}

	// Set up a new random source (by default Go doesn't seed it). This is not thread safe,
	// so you must use the rndIntn method.
	rnd   = rand.New(rand.NewSource(time.Now().UnixNano()))
	rndmu = &sync.Mutex{}

	// Returned by consumers on Fetch when the retry limit is set and exceeded.
	ErrNoData = errors.New("no data")

	// Make sure interfaces are implemented
	_ Client            = &Broker{}
	_ Consumer          = &consumer{}
	_ Producer          = &producer{}
	_ OffsetCoordinator = &offsetCoordinator{}
)

// Client is the interface implemented by Broker.
type Client interface {
	Producer(conf ProducerConf) Producer
	Consumer(conf ConsumerConf) (Consumer, error)
	OffsetCoordinator(conf OffsetCoordinatorConf) (OffsetCoordinator, error)
	OffsetEarliest(topic string, partition int32) (offset int64, err error)
	OffsetLatest(topic string, partition int32) (offset int64, err error)
	Close()
}

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

// Producer is the interface that wraps the Produce method.
//
// Produce writes the messages to the given topic and partition.
// It returns the offset of the first message and any error encountered.
// The offset of each message is also updated accordingly.
type Producer interface {
	Produce(topic string, partition int32, messages ...*proto.Message) (offset int64, err error)
}

// OffsetCoordinator is the interface which wraps the Commit and Offset methods.
type OffsetCoordinator interface {
	Commit(topic string, partition int32, offset int64) error
	Offset(topic string, partition int32) (offset int64, metadata string, err error)
	Close()
}

type topicPartition struct {
	topic     string
	partition int32
}

func (tp topicPartition) String() string {
	return fmt.Sprintf("%s:%d", tp.topic, tp.partition)
}

type BrokerConf struct {
	// Kafka client ID.
	ClientID string

	// LeaderRetryLimit limits the number of connection attempts to a single
	// node before failing. Use LeaderRetryWait to control the wait time
	// between retries.
	//
	// Defaults to 10.
	LeaderRetryLimit int

	// LeaderRetryWait sets a limit to the waiting time when trying to connect
	// to a single node after failure. This is the initial time, we will do an
	// exponential backoff with increasingly long durations.
	//
	// Defaults to 500ms.
	//
	// Timeout on a connection is controlled by the DialTimeout setting.
	LeaderRetryWait time.Duration

	// AllowTopicCreation enables a last-ditch "send produce request" which
	// happens if we do not know about a topic. This enables topic creation
	// if your Kafka cluster is configured to allow it.
	//
	// Defaults to False.
	AllowTopicCreation bool

	// Any new connection dial timeout.
	//
	// Default is 10 seconds.
	DialTimeout time.Duration

	// DialRetryLimit limits the number of connection attempts to every node in
	// cluster before failing. Use DialRetryWait to control the wait time
	// between retries.
	//
	// Defaults to 10.
	DialRetryLimit int

	// DialRetryWait sets a limit to the waiting time when trying to establish
	// broker connection to single node to fetch cluster metadata. This is subject to
	// exponential backoff, so the second and further retries will be more than this
	// value.
	//
	// Defaults to 500ms.
	DialRetryWait time.Duration

	// MetadataRefreshTimeout is the maximum time to wait for a metadata refresh. This
	// is compounding with many of the retries -- various failures trigger a metadata
	// refresh. This should be set fairly high, as large metadata objects or loaded
	// clusters can take a little while to return data.
	//
	// Defaults to 30s.
	MetadataRefreshTimeout time.Duration

	// MetadataRefreshFrequency is how often we should refresh metadata regardless of whether we
	// have encountered errors.
	//
	// Defaults to 0 which means disabled.
	MetadataRefreshFrequency time.Duration

	// ConnectionLimit sets a limit on how many outstanding connections may exist to a
	// single broker. This limit is for all connections in any state -- we will never use
	// more than this many connections at a time. Setting this too low can limit your
	// throughput, but setting it too high can cause problems for your cluster.
	//
	// Defaults to 10.
	ConnectionLimit int

	// IdleConnectionLimit sets a limit on how many currently idle connections can
	// be open to each broker. Lowering this will reduce the number of unused connections
	// to Kafka, but can result in extra latency during request spikes if connections
	// are not available and have to be established.
	//
	// Defaults to 5.
	IdleConnectionLimit int

	// IdleConnectionWait sets a timeout on how long we should wait for a connection to
	// become idle before we establish a new one. This value sets a cap on how much latency
	// you're willing to add to a request before establishing a new connection.
	//
	// Default is 200ms.
	IdleConnectionWait time.Duration
}

func NewBrokerConf(clientID string) BrokerConf {
	return BrokerConf{
		ClientID:                 clientID,
		DialTimeout:              10 * time.Second,
		DialRetryLimit:           10,
		DialRetryWait:            500 * time.Millisecond,
		AllowTopicCreation:       false,
		LeaderRetryLimit:         10,
		LeaderRetryWait:          500 * time.Millisecond,
		MetadataRefreshTimeout:   30 * time.Second,
		MetadataRefreshFrequency: 0,
		ConnectionLimit:          10,
		IdleConnectionLimit:      5,
		IdleConnectionWait:       200 * time.Millisecond,
	}
}

type nodeMap map[int32]string

// Broker is an abstract connection to kafka cluster, managing connections to
// all kafka nodes.
type Broker struct {
	conf BrokerConf

	// mu protects all read/write accesses to the metadata/conns structures. This
	// lock must not be held during network operations.
	mu       *sync.Mutex
	metadata *clusterMetadata
	conns    *connectionPool
}

// SetLogger provides a logging instance. Should be called before any Dial or
// other work is done to ensure everybody has the logger.
func SetLogger(logger Logger) {
	log = logger
}

// Dial connects to any node from a given list of kafka addresses and after
// successful metadata fetch, returns broker.
//
// The returned broker is not initially connected to any kafka node.
func Dial(nodeAddresses []string, conf BrokerConf) (*Broker, error) {
	pool := newConnectionPool(conf)
	broker := &Broker{
		mu:       &sync.Mutex{},
		conf:     conf,
		conns:    pool,
		metadata: newClusterMetadata(conf, pool),
	}

	if len(nodeAddresses) == 0 {
		return nil, errors.New("no addresses provided")
	}

	// Set up the pool
	broker.conns.InitializeAddrs(nodeAddresses)

	// Attempt to connect to the cluster but we want to do this with backoff and make sure we
	// don't exceed the limits
	retry := &backoff.Backoff{Min: conf.DialRetryWait, Jitter: true}
	for try := 0; try < conf.DialRetryLimit; try++ {
		if try > 0 {
			sleepFor := retry.Duration()
			log.Debug("cannot fetch metadata from any connection",
				"retry", try,
				"sleep", sleepFor)
			time.Sleep(sleepFor)
		}

		resultChan := make(chan error, 1)
		go func() {
			resultChan <- broker.metadata.Refresh()
		}()

		select {
		case err := <-resultChan:
			if err == nil {
				// Metadata has been refreshed, so this broker is ready to go
				return broker, nil
			}
			log.Error("cannot fetch metadata",
				"error", err)
		case <-time.After(conf.DialTimeout):
			log.Error("timeout fetching metadata")
		}
	}
	return nil, errors.New("cannot connect (exhausted retries)")
}

// Close closes the broker and all active kafka nodes connections.
func (b *Broker) Close() {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.conns.Close()
}

// Metadata returns a copy of the metadata. This does not require a lock as it's fetching
// a new copy from Kafka, we never use our internal state.
func (b *Broker) Metadata() (*proto.MetadataResp, error) {
	resp, err := b.metadata.Fetch()
	return resp, err
}

// PartitionCount returns the count of partitions in a topic, or 0 and an error if the topic
// does not exist.
func (b *Broker) PartitionCount(topic string) (int32, error) {
	return b.metadata.PartitionCount(topic)
}

// leaderConnection returns connection to leader for given partition. If
// connection does not exist, broker will try to connect.
//
// Failed connection retry is controlled by broker configuration.
//
// If broker is configured to allow topic creation, then if we don't find
// the leader we will return a random broker. The broker will error if we end
// up producing to it incorrectly (i.e., our metadata happened to be out of
// date).
func (b *Broker) leaderConnection(
	topic string, partition int32) (conn *connection, err error) {

	tp := topicPartition{topic, partition}
	retry := &backoff.Backoff{Min: b.conf.LeaderRetryWait, Jitter: true}
	for try := 0; try < b.conf.LeaderRetryLimit; try++ {
		if try != 0 {
			sleepFor := retry.Duration()
			log.Debug("cannot get leader connection",
				"topic", topic,
				"partition", partition,
				"retry", try,
				"sleep", sleepFor)
			time.Sleep(sleepFor)
		}

		// Attempt to learn where this topic/partition is. This may return an error in which
		// case we don't know about it and should refresh metadata.
		var nodeID int32
		nodeID, err = b.metadata.GetEndpoint(tp)
		if err != nil {
			err = b.metadata.Refresh()
			if err != nil {
				log.Info("cannot get leader connection: cannot refresh metadata",
					"error", err)
				continue
			}
			nodeID, err = b.metadata.GetEndpoint(tp)
			if err != nil {
				err = proto.ErrUnknownTopicOrPartition
				// If we allow topic creation, now is the point where it is likely that this
				// is a brand new topic, so try to get metadata on it (which will trigger
				// the creation process)
				if b.conf.AllowTopicCreation {
					_, err := b.metadata.Fetch(topic)
					if err != nil {
						log.Info("failed to fetch metadata for new topic",
							"topic", topic,
							"error", err)
					}
				} else {
					log.Info("cannot get leader connection: unknown topic or partition",
						"topic", topic,
						"partition", partition,
						"endpoint", tp)
					// We've already refreshed the metadata. The topic doesn't exist and we're
					// not in creation mode so let's return now so we don't spend all the retries
					// refreshing a topic that doesn't exist.
					break
				}
				continue
			}
		}

		// Now attempt to get a connection to this node
		addr := b.metadata.GetNodeAddress(nodeID)
		if addr == "" {
			log.Info("cannot get leader connection: no information about node",
				"nodeID", nodeID)
			err = proto.ErrBrokerNotAvailable
			// Forget the endpoint so we'll refresh metadata after the next retry.
			b.metadata.ForgetEndpoint(tp)
			continue
		}

		conn, err = b.conns.GetConnectionByAddr(addr)
		if err != nil {
			log.Info("cannot get leader connection: cannot connect to node",
				"address", addr,
				"error", err)
			// Forget the endpoint. It's possible this broker has failed and we want to wait
			// for Kafka to elect a new leader. To trick our algorithm into working we have to
			// forget this endpoint so it will refresh metadata.
			b.metadata.ForgetEndpoint(tp)
			continue
		}
		return conn, nil
	}
	return nil, err
}

// coordinatorConnection returns connection to offset coordinator for given group.
//
// Failed connection retry is controlled by broker configuration.
func (b *Broker) coordinatorConnection(consumerGroup string) (conn *connection, resErr error) {
	addrs := b.conns.GetAllAddrs()
	retry := &backoff.Backoff{Min: b.conf.LeaderRetryWait, Jitter: true}
	for try := 0; try < b.conf.LeaderRetryLimit; try++ {
		if try != 0 {
			time.Sleep(retry.Duration())
		}

		// First try an idle connection
		conn := b.conns.GetIdleConnection()
		if conn == nil {
			for _, idx := range rndPerm(len(addrs)) {
				var err error
				conn, err = b.conns.GetConnectionByAddr(addrs[idx])
				if err == nil {
					// No error == have a nice connection.
					break
				}
			}
		}

		if conn != nil {
			defer func() { go b.conns.Idle(conn) }()
			resp, err := conn.ConsumerMetadata(&proto.ConsumerMetadataReq{
				ClientID:      b.conf.ClientID,
				ConsumerGroup: consumerGroup,
			})
			if err != nil {
				log.Debug("cannot fetch consumer metadata",
					"consumGrp", consumerGroup,
					"error", err)
				resErr = err
				continue
			}
			if resp.Err != nil {
				log.Debug("metadata response error",
					"consumGrp", consumerGroup,
					"error", resp.Err)
				resErr = resp.Err
				continue
			}

			addr := fmt.Sprintf("%s:%d", resp.CoordinatorHost, resp.CoordinatorPort)
			conn, err = b.conns.GetConnectionByAddr(addr)
			if err != nil {
				log.Debug("cannot connect to coordinator node",
					"coordinatorID", resp.CoordinatorID,
					"address", addr,
					"error", err)
				resErr = err
				continue
			}
			return conn, nil
		}

		// Current error state
		resErr = proto.ErrNoCoordinator

		// Refresh metadata. At this point it looks like none of the connections worked, so
		// possibly we need a new set.
		if err := b.metadata.Refresh(); err != nil {
			log.Debug("cannot refresh metadata",
				"error", err)
			resErr = err
			continue
		}
	}
	return nil, resErr
}

// offset will return offset value for given partition. Use timems to specify
// which offset value should be returned.
func (b *Broker) offset(topic string, partition int32, timems int64) (offset int64, err error) {
	conn, err := b.leaderConnection(topic, partition)
	if err != nil {
		return 0, err
	}
	defer func() { go b.conns.Idle(conn) }()

	resp, err := conn.Offset(&proto.OffsetReq{
		ClientID:  b.conf.ClientID,
		ReplicaID: -1, // any client
		Topics: []proto.OffsetReqTopic{
			{
				Name: topic,
				Partitions: []proto.OffsetReqPartition{
					{
						ID:         partition,
						TimeMs:     timems,
						MaxOffsets: 2,
					},
				},
			},
		},
	})

	if err != nil {
		if _, ok := err.(*net.OpError); ok || err == io.EOF || err == syscall.EPIPE {
			// Connection is broken, so should be closed, but the error is
			// still valid and should be returned so that retry mechanism have
			// chance to react.
			log.Debug("connection died while sending message",
				"topic", topic,
				"partition", partition,
				"error", err)
			conn.Close()
		}
		return 0, err
	}
	found := false
	for _, t := range resp.Topics {
		if t.Name != topic {
			log.Debug("unexpected topic information",
				"expected", topic,
				"got", t.Name)
			continue
		}
		for _, part := range t.Partitions {
			if part.ID != partition {
				log.Debug("unexpected partition information",
					"topic", t.Name,
					"expected", partition,
					"got", part.ID)
				continue
			}
			found = true
			// happens when there are no messages
			if len(part.Offsets) == 0 {
				offset = 0
			} else {
				offset = part.Offsets[0]
			}
			err = part.Err
		}
	}
	if !found {
		return 0, errors.New("incomplete fetch response")
	}
	return offset, err
}

// OffsetEarliest returns the oldest offset available on the given partition.
func (b *Broker) OffsetEarliest(topic string, partition int32) (int64, error) {
	return b.offset(topic, partition, -2)
}

// OffsetLatest return the offset of the next message produced in given partition
func (b *Broker) OffsetLatest(topic string, partition int32) (int64, error) {
	return b.offset(topic, partition, -1)
}

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
				log.Debug("cannot refresh metadata",
					"error", err)
			}
		}
		log.Debug("cannot produce messages",
			"retry", retry,
			"error", err)
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
	defer func() { go p.broker.conns.Idle(conn) }()

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
			log.Debug("connection died while sending message",
				"topic", topic,
				"partition", partition,
				"error", err)
			conn.Close()
		}
		return 0, err
	}

	if req.RequiredAcks == proto.RequiredAcksNone {
		return 0, err
	}

	// we expect single partition response
	found := false
	for _, t := range resp.Topics {
		if t.Name != topic {
			log.Debug("unexpected topic information received",
				"expected", topic,
				"got", t.Name)
			continue
		}
		for _, part := range t.Partitions {
			if part.ID != partition {
				log.Debug("unexpected partition information received",
					"topic", t.Name,
					"expected", partition,
					"got", part.ID)
				continue
			}
			found = true
			offset = part.Offset
			err = part.Err
		}
	}

	if !found {
		return 0, errors.New("incomplete produce response")
	}
	return offset, err
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
			if c.conf.RetryWait > 0 {
				time.Sleep(c.conf.RetryWait)
			}
			retry += 1
			if c.conf.RetryLimit != -1 && retry > c.conf.RetryLimit {
				return nil, ErrNoData
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
		defer func() { go c.broker.conns.Idle(conn) }()

		resp, err := conn.Fetch(&req)
		resErr = err
		if _, ok := err.(*net.OpError); ok || err == io.EOF || err == syscall.EPIPE {
			log.Debug("connection died while fetching message",
				"topic", c.conf.Topic,
				"partition", c.conf.Partition,
				"error", err)
			conn.Close()
			continue
		}

		if err != nil {
			log.Debug("cannot fetch messages: unknown error",
				"retry", retry,
				"error", err)
			conn.Close()
			continue
		}

		for _, topic := range resp.Topics {
			if topic.Name != c.conf.Topic {
				log.Warn("unexpected topic information received",
					"got", topic.Name,
					"expected", c.conf.Topic)
				continue
			}
			for _, part := range topic.Partitions {
				if part.ID != c.conf.Partition {
					log.Warn("unexpected partition information received",
						"topic", topic.Name,
						"expected", c.conf.Partition,
						"got", part.ID)
					continue
				}
				switch part.Err {
				case proto.ErrLeaderNotAvailable, proto.ErrNotLeaderForPartition, proto.ErrBrokerNotAvailable:
					// Failover happened, so we probably need to talk to a different broker. Let's
					// kick off a metadata refresh.
					log.Debug("cannot fetch messages",
						"retry", retry,
						"error", part.Err)
					if err := c.broker.metadata.Refresh(); err != nil {
						log.Debug("cannot refresh metadata",
							"error", err)
					}
					continue consumeRetryLoop
				}
				return part.Messages, part.Err
			}
		}
		return nil, errors.New("incomplete fetch response")
	}

	return nil, resErr
}

type OffsetCoordinatorConf struct {
	ConsumerGroup string

	// RetryErrLimit limits messages fetch retry upon failure. By default 10.
	RetryErrLimit int

	// RetryErrWait controls wait duration between retries after failed fetch
	// request. By default 500ms.
	RetryErrWait time.Duration
}

// NewOffsetCoordinatorConf returns default OffsetCoordinator configuration.
func NewOffsetCoordinatorConf(consumerGroup string) OffsetCoordinatorConf {
	return OffsetCoordinatorConf{
		ConsumerGroup: consumerGroup,
		RetryErrLimit: 10,
		RetryErrWait:  time.Millisecond * 500,
	}
}

type offsetCoordinator struct {
	conf   OffsetCoordinatorConf
	broker *Broker

	mu   *sync.Mutex
	conn *connection
}

// OffsetCoordinator returns offset management coordinator for single consumer
// group, bound to broker.
func (b *Broker) OffsetCoordinator(conf OffsetCoordinatorConf) (OffsetCoordinator, error) {
	conn, err := b.coordinatorConnection(conf.ConsumerGroup)
	if err != nil {
		return nil, err
	}
	c := &offsetCoordinator{
		broker: b,
		mu:     &sync.Mutex{},
		conf:   conf,
		conn:   conn,
	}
	return c, nil
}

// Commit is saving offset information for given topic and partition.
//
// Commit can retry saving offset information on common errors. This behaviour
// can be configured with with RetryErrLimit and RetryErrWait coordinator
// configuration attributes.
func (c *offsetCoordinator) Commit(topic string, partition int32, offset int64) error {
	return c.commit(topic, partition, offset, "")
}

// Commit works exactly like Commit method, but store extra metadata string
// together with offset information.
func (c *offsetCoordinator) CommitFull(topic string, partition int32, offset int64, metadata string) error {
	return c.commit(topic, partition, offset, metadata)
}

// getConnection returns a copy of the connection. This takes the lock so that we
// ensure we get a consistent copy.
func (c *offsetCoordinator) getConnection() (*connection, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Only return the connection if it's still open. It can have closed for some reason
	// by someone else.
	if c.conn != nil && !c.conn.IsClosed() {
		return c.conn, nil
	}

	// connection can be set to nil if previously reference connection died. We do this in
	// the lock because otherwise everybody might rush to create new connections and then
	// overwrite each other. This lock is scoped small enough that we're OK waiting for the
	// network operation within the lock.
	conn, err := c.broker.coordinatorConnection(c.conf.ConsumerGroup)
	if err != nil {
		log.Debug("cannot connect to coordinator",
			"consumGrp", c.conf.ConsumerGroup,
			"error", err)
		return nil, err
	}
	c.conn = conn
	return c.conn, nil
}

// commit is saving offset and metadata information. Provides limited error
// handling configurable through OffsetCoordinatorConf.
func (c *offsetCoordinator) commit(
	topic string, partition int32, offset int64, metadata string) (resErr error) {

	retry := &backoff.Backoff{Min: c.conf.RetryErrWait, Jitter: true}
	for try := 0; try < c.conf.RetryErrLimit; try++ {
		if try != 0 {
			time.Sleep(retry.Duration())
		}

		// get a copy of our connection with the lock, this might establish a new
		// connection so can take a bit
		conn, err := c.getConnection()
		if conn == nil {
			resErr = err
			continue
		}

		resp, err := conn.OffsetCommit(&proto.OffsetCommitReq{
			ClientID:      c.broker.conf.ClientID,
			ConsumerGroup: c.conf.ConsumerGroup,
			Topics: []proto.OffsetCommitReqTopic{
				{
					Name: topic,
					Partitions: []proto.OffsetCommitReqPartition{
						{ID: partition, Offset: offset, TimeStamp: time.Now(), Metadata: metadata},
					},
				},
			},
		})
		resErr = err

		if _, ok := err.(*net.OpError); ok || err == io.EOF || err == syscall.EPIPE {
			log.Debug("connection died while committing",
				"topic", topic,
				"partition", partition,
				"consumGrp", c.conf.ConsumerGroup)
			conn.Close()

		} else if err == nil {
			for _, t := range resp.Topics {
				if t.Name != topic {
					log.Debug("unexpected topic information received",
						"got", t.Name,
						"expected", topic)
					continue

				}
				for _, part := range t.Partitions {
					if part.ID != partition {
						log.Debug("unexpected partition information received",
							"topic", topic,
							"got", part.ID,
							"expected", partition)
						continue
					}
					return part.Err
				}
			}
			return errors.New("response does not contain commit information")
		}
	}
	return resErr
}

// Offset is returning last offset and metadata information committed for given
// topic and partition.
//
// Offset can retry sending request on common errors. This behaviour can be
// configured with with RetryErrLimit and RetryErrWait coordinator
// configuration attributes.
func (c *offsetCoordinator) Offset(topic string, partition int32) (offset int64, metadata string, resErr error) {
	retry := &backoff.Backoff{Min: c.conf.RetryErrWait, Jitter: true}
	for try := 0; try < c.conf.RetryErrLimit; try++ {
		if try != 0 {
			time.Sleep(retry.Duration())
		}

		// get a copy of our connection with the lock, this might establish a new
		// connection so can take a bit
		conn, err := c.getConnection()
		if conn == nil {
			resErr = err
			continue
		}

		resp, err := conn.OffsetFetch(&proto.OffsetFetchReq{
			ConsumerGroup: c.conf.ConsumerGroup,
			Topics: []proto.OffsetFetchReqTopic{
				{
					Name:       topic,
					Partitions: []int32{partition},
				},
			},
		})
		resErr = err

		switch err {
		case io.EOF, syscall.EPIPE:
			log.Debug("connection died while fetching offset",
				"topic", topic,
				"partition", partition,
				"consumGrp", c.conf.ConsumerGroup)
			conn.Close()

		case nil:
			for _, t := range resp.Topics {
				if t.Name != topic {
					log.Debug("unexpected topic information received",
						"got", t.Name,
						"expected", topic)
					continue
				}
				for _, part := range t.Partitions {
					if part.ID != partition {
						log.Debug("unexpected partition information received",
							"topic", topic,
							"expected", partition,
							"get", part.ID)
						continue
					}
					if part.Err != nil {
						return 0, "", part.Err
					}
					return part.Offset, part.Metadata, nil
				}
			}
			return 0, "", errors.New("response does not contain offset information")
		}
	}

	return 0, "", resErr
}

// Close releases the underlying connection back to the broker pool. Calling
// clients should assume the coordinator cannot be re-used once it has been
// closed.
func (c *offsetCoordinator) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conn != nil && !c.conn.IsClosed() {
		go c.broker.conns.Idle(c.conn)
		c.conn = nil
	}
}

// rndIntn adds locking around accessing the random number generator. This is required because
// Go doesn't provide locking within the rand.Rand object.
func rndIntn(n int) int {
	rndmu.Lock()
	defer rndmu.Unlock()

	return rnd.Intn(n)
}

// rndPerm adds locking around using the random number generator.
func rndPerm(n int) []int {
	rndmu.Lock()
	defer rndmu.Unlock()

	return rnd.Perm(n)
}
