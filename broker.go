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
	// single broker. This limit is for all connections except Metadata fetches which are exempted
	// but separately limited to one per cluster. That is, the maximum number of connections per
	// broker is ConnectionLimit + 1 but the maximum number of connections per cluster is
	// NumBrokers * ConnectionLimit + 1 not NumBrokers * (ConnectionLimit + 1).
	// Setting this too low can limit your throughput, but setting it too high can cause problems
	// for your cluster.
	//
	// Defaults to 10.
	ConnectionLimit int

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
		IdleConnectionWait:       200 * time.Millisecond,
	}
}

type nodeMap map[int32]string

// Broker is an abstract connection to kafka cluster, managing connections to
// all kafka nodes.
type Broker struct {
	conf     BrokerConf
	conns    connectionPool
	metadata clusterMetadata
}

// Dial connects to any node from a given list of kafka addresses and after
// successful metadata fetch, returns broker.
//
// The returned broker is not initially connected to any kafka node.
func Dial(nodeAddresses []string, conf BrokerConf) (*Broker, error) {
	broker := &Broker{
		conf:  conf,
		conns: newConnectionPool(conf),
	}
	broker.metadata = newClusterMetadata(conf, &broker.conns)

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
			log.Debugf("cannot fetch metadata from any connection (try %d, sleep %f)",
				try, sleepFor)
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
			log.Errorf("cannot fetch metadata: %s", err)
		case <-time.After(conf.DialTimeout):
			log.Error("timeout fetching metadata")
		}
	}
	return nil, errors.New("cannot connect (exhausted retries)")
}

// Close closes the broker and all active kafka nodes connections.
func (b *Broker) Close() {
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

// getLeaderEndpoint returns the ID of the node responsible for a topic/partition.
// This may refresh metadata and may also initiate topic creation if the topic is
// unknown and such is enabled. This method may take a long time to return.
func (b *Broker) getLeaderEndpoint(topic string, partition int32) (int32, error) {
	// Attempt to learn where this topic/partition is. This may return an error in which
	// case we don't know about it and should refresh metadata.
	if nodeID, err := b.metadata.GetEndpoint(topic, partition); err == nil {
		return nodeID, nil
	}

	// Endpoint is unknown, refresh metadata (synchronous, blocks a while)
	if err := b.metadata.Refresh(); err != nil {
		log.Warningf("[getLeaderEndpoint %s:%d] cannot refresh metadata: %s",
			topic, partition, err)
		return 0, err
	}

	// Successfully refreshed metadata, try to get endpoint again
	if nodeID, err := b.metadata.GetEndpoint(topic, partition); err == nil {
		return nodeID, nil
	}

	// If we're not allowed to create topics, exit now we're done
	if !b.conf.AllowTopicCreation {
		log.Warningf("[getLeaderEndpoint %s:%d] unknown topic or partition (no create)",
			topic, partition)
		return 0, proto.ErrUnknownTopicOrPartition
	}

	// Try to create the topic by requesting the metadata for that one specific topic
	// (this is the hack Kafka uses to allow topics to be created on demand)
	if _, err := b.metadata.Fetch(topic); err != nil {
		log.Warningf("[getLeaderEndpoint %s:%d] failed to get metadata for topic: %s",
			topic, partition, err)
		return 0, err
	}

	// Successfully refreshed metadata, try to get endpoint again
	if nodeID, err := b.metadata.GetEndpoint(topic, partition); err == nil {
		return nodeID, nil
	}

	// This topic is dead to us, we failed to find it and failed to create it
	log.Warningf("[getLeaderEndpoint %s:%d] unknown topic or partition (post-create)",
		topic, partition)
	return 0, proto.ErrUnknownTopicOrPartition
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
func (b *Broker) leaderConnection(topic string, partition int32) (*connection, error) {
	retry := &backoff.Backoff{Min: b.conf.LeaderRetryWait, Jitter: true}
	for try := 0; try < b.conf.LeaderRetryLimit; try++ {
		if try != 0 {
			sleepFor := retry.Duration()
			log.Debugf("cannot get leader connection for %s:%d: retry=%d, sleep=%s",
				topic, partition, try, sleepFor)
			time.Sleep(sleepFor)
		}

		if b.IsClosed() {
			return nil, errors.New("Broker was closed, giving up on leaderConnection.")
		}

		// Figure out which broker (node/endpoint) is presently leader for this t/p
		nodeID, err := b.getLeaderEndpoint(topic, partition)
		if err != nil {
			continue
		}

		// Now attempt to get a connection to this node
		if addr := b.metadata.GetNodeAddress(nodeID); addr == "" {
			// Forget the endpoint so we'll refresh metadata next try
			log.Warningf("[leaderConnection %s:%d] unknown broker ID: %d",
				topic, partition, nodeID)
			b.metadata.ForgetEndpoint(topic, partition)
		} else {
			// TODO: Can this return a non-nil error if the connection pool is full?
			if conn, err := b.conns.GetConnectionByAddr(addr); err != nil {
				// Forget the endpoint. It's possible this broker has failed and we want to wait
				// for Kafka to elect a new leader. To trick our algorithm into working we have to
				// forget this endpoint so it will refresh metadata.
				log.Warningf("[leaderConnection %s:%d] failed to connect to %s: %s",
					topic, partition, addr, err)
				b.metadata.ForgetEndpoint(topic, partition)
			} else {
				// Successful (supposedly) connection
				return conn, nil
			}
		}
	}

	// All paths lead to the topic/partition being unknown, a more specific error would have
	// been returned earlier
	return nil, proto.ErrUnknownTopicOrPartition
}

// coordinatorConnection returns connection to offset coordinator for given group. May
// return proto.ErrNoCoordinator if we are unable to find a broker to talk to. May also
// return other errors (connection errors, ErrNoTopic, etc).
//
// NOTE: this function returns a connection and it is the caller's responsibility to ensure
// that this connection is eventually returned to the pool with Idle.
func (b *Broker) coordinatorConnection(consumerGroup string) (*connection, error) {
	// Get group coordinator
	resp, err := b.getGroupCoordinator(consumerGroup)
	if err != nil {
		log.Warningf("coordinatorConnection: failed to discover coordinator: %s", err)
		return nil, proto.ErrNoCoordinator
	}

	// Now get connection to actual coordinator
	addr := fmt.Sprintf("%s:%d", resp.CoordinatorHost, resp.CoordinatorPort)
	conn, err := b.conns.GetConnectionByAddr(addr)
	if err != nil {
		log.Errorf("coordinatorConnection: failed to reach node %d at %s: %s",
			resp.CoordinatorID, addr, err)
		return nil, proto.ErrNoCoordinator
	}

	// Return to caller, they must ensure Idle is eventually called
	return conn, nil
}

// getGroupCoordinator is an internal function that fetches a group coordinator.
func (b *Broker) getGroupCoordinator(consumerGroup string) (*proto.GroupCoordinatorResp, error) {
	// Attempt to get idle connection first, else, try all possible brokers
	// randomly permuted
	conn := b.conns.GetIdleConnection()
	if conn == nil {
		addrs := b.conns.GetAllAddrs()
		for _, idx := range rndPerm(len(addrs)) {
			var err error
			conn, err = b.conns.GetConnectionByAddr(addrs[idx])
			if err == nil {
				// No error == have a nice connection.
				break
			}
		}
	}
	if conn == nil {
		log.Warningf("coordinatorConnection: failed to connect to any broker")
		return nil, errors.New("failed to connect to any broker")
	}

	// Ensure we release this connection
	defer func(lconn *connection) { go b.conns.Idle(lconn) }(conn)

	// Now fetch coordinator from this broker
	resp, err := conn.GroupCoordinator(&proto.GroupCoordinatorReq{
		ClientID: b.conf.ClientID,
		GroupID:  consumerGroup,
	})
	if err != nil {
		log.Errorf("coordinatorConnection: metadata error for %s: %s",
			consumerGroup, err)
		return nil, err
	}
	if resp.Err != nil {
		log.Errorf("coordinatorConnection: metadata response error for %s: %s",
			consumerGroup, resp.Err)
		return nil, resp.Err
	}
	return resp, nil
}

// offset will return offset value for given partition. Use timems to specify
// which offset value should be returned.
func (b *Broker) offset(topic string, partition int32, timems int64) (int64, error) {
	req := &proto.OffsetReq{
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
	}

	var resErr error
	retry := &backoff.Backoff{Min: b.conf.LeaderRetryWait, Jitter: true}
offsetRetryLoop:
	for try := 0; try < b.conf.LeaderRetryLimit; try++ {
		if try != 0 {
			time.Sleep(retry.Duration())
		}

		conn, err := b.leaderConnection(topic, partition)
		if err != nil {
			return 0, err
		}
		defer func(lconn *connection) { go b.conns.Idle(lconn) }(conn)

		resp, err := conn.Offset(req)
		if err != nil {
			if _, ok := err.(*net.OpError); ok || err == io.EOF || err == syscall.EPIPE {
				log.Debugf("connection died while sending message to %s:%d: %s",
					topic, partition, err)
				conn.Close()
				resErr = err
				continue
			}
			return 0, err
		}

		for _, t := range resp.Topics {
			for _, p := range t.Partitions {
				if t.Name != topic || p.ID != partition {
					log.Warningf("offset response with unexpected data for %s:%d",
						t.Name, p.ID)
					continue
				}
				resErr = p.Err

				switch p.Err {
				case proto.ErrLeaderNotAvailable, proto.ErrNotLeaderForPartition,
					proto.ErrBrokerNotAvailable:
					// Failover happened, so we probably need to talk to a different broker. Let's
					// kick off a metadata refresh.
					log.Debugf("cannot fetch offset: %s", p.Err)
					if err := b.metadata.Refresh(); err != nil {
						log.Debugf("cannot refresh metadata: %s", err)
					}
					continue offsetRetryLoop
				}

				// Happens when there are no messages in the partition
				if len(p.Offsets) == 0 {
					return 0, p.Err
				} else {
					return p.Offsets[0], p.Err
				}
			}
		}
	}

	if resErr == nil {
		return 0, errors.New("incomplete fetch response")
	}
	return 0, resErr
}

// OffsetEarliest returns the oldest offset available on the given partition.
func (b *Broker) OffsetEarliest(topic string, partition int32) (int64, error) {
	return b.offset(topic, partition, -2)
}

// OffsetLatest return the offset of the next message produced in given partition
func (b *Broker) OffsetLatest(topic string, partition int32) (int64, error) {
	return b.offset(topic, partition, -1)
}

func (b *Broker) IsClosed() bool {
	return b.conns.IsClosed()
}

type OffsetCoordinatorConf struct {
	GroupID string

	// RetryErrLimit limits messages fetch retry upon failure. By default 10.
	RetryErrLimit int

	// RetryErrWait controls wait duration between retries after failed fetch
	// request. By default 500ms.
	RetryErrWait time.Duration
}

// NewOffsetCoordinatorConf returns default OffsetCoordinator configuration.
func NewOffsetCoordinatorConf(consumerGroup string) OffsetCoordinatorConf {
	return OffsetCoordinatorConf{
		GroupID:       consumerGroup,
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
	c := &offsetCoordinator{
		broker: b,
		conf:   conf,
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
		conn, err := c.broker.coordinatorConnection(c.conf.GroupID)
		if conn == nil {
			resErr = err
			continue
		}
		defer func(lconn *connection) { go c.broker.conns.Idle(lconn) }(conn)

		resp, err := conn.OffsetCommit(&proto.OffsetCommitReq{
			ClientID: c.broker.conf.ClientID,
			GroupID:  c.conf.GroupID,
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
			log.Debugf("connection died while committing on %s:%d for %s: %s",
				topic, partition, c.conf.GroupID, err)
			conn.Close()

		} else if err == nil {
			// Should be a single response in the payload.
			for _, t := range resp.Topics {
				for _, p := range t.Partitions {
					if t.Name != topic || p.ID != partition {
						log.Warningf("commit response with unexpected data for %s:%d",
							t.Name, p.ID)
						continue
					}
					return p.Err
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
func (c *offsetCoordinator) Offset(
	topic string, partition int32) (
	offset int64, metadata string, resErr error) {

	retry := &backoff.Backoff{Min: c.conf.RetryErrWait, Jitter: true}
	for try := 0; try < c.conf.RetryErrLimit; try++ {
		if try != 0 {
			time.Sleep(retry.Duration())
		}

		// get a copy of our connection with the lock, this might establish a new
		// connection so can take a bit
		conn, err := c.broker.coordinatorConnection(c.conf.GroupID)
		if conn == nil {
			resErr = err
			continue
		}
		defer func(lconn *connection) { go c.broker.conns.Idle(lconn) }(conn)

		resp, err := conn.OffsetFetch(&proto.OffsetFetchReq{
			GroupID: c.conf.GroupID,
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
			log.Debugf("connection died while fetching offsets on %s:%d for %s: %s",
				topic, partition, c.conf.GroupID, err)
			conn.Close()

		case nil:
			for _, t := range resp.Topics {
				for _, p := range t.Partitions {
					if t.Name != topic || p.ID != partition {
						log.Warningf("offset response with unexpected data for %s:%d",
							t.Name, p.ID)
						continue
					}

					if p.Err != nil {
						return 0, "", p.Err
					}
					return p.Offset, p.Metadata, nil
				}
			}
			return 0, "", errors.New("response does not contain offset information")
		}
	}

	return 0, "", resErr
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
