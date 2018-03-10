package kafka

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dropbox/kafka/proto"
	"github.com/jpillora/backoff"
)

// Cluster maintains the metadata and connectionPools for a Kafka cluster.
type Cluster struct {
	// ConnectionPool with only one connection to use for metadata requests
	metadataConnPool *ConnectionPool

	// ConnectionPoolCache for all other requests
	connPoolCache *ConnectionPoolCache

	conf ClusterConnectionConf

	// mu protects the contents of this structure, but should only be gotten/used
	// by the clusterMetadata methods.
	mu         *sync.RWMutex
	refLock    *sync.Mutex
	epoch      *int64
	timeout    time.Duration
	created    time.Time
	nodes      nodeMap                  // node ID to address
	endpoints  map[topicPartition]int32 // partition to leader node ID
	partitions map[string]int32         // topic to number of partitions
}

func newCluster(conf ClusterConnectionConf, pool *ConnectionPool, connPoolCache *ConnectionPoolCache) *Cluster {
	result := &Cluster{
		mu:               &sync.RWMutex{},
		timeout:          conf.MetadataRefreshTimeout,
		refLock:          &sync.Mutex{},
		epoch:            new(int64),
		metadataConnPool: pool,
		connPoolCache:    connPoolCache,
		conf:             conf,
	}
	if conf.MetadataRefreshFrequency > 0 {
		go func() {
			log.Infof("Periodically refreshing metadata (frequency=%s)",
				conf.MetadataRefreshFrequency)
			for {
				select {
				case <-time.After(conf.MetadataRefreshFrequency):
					log.Info("Initiating periodic metadata refresh.")
					_ = result.RefreshMetadata()
				}
			}
		}()
	}
	return result
}

// NewCluster connects to a cluster from a given list of kafka addresses and after successful
// metadata fetch, returns Cluster.
func NewCluster(nodeAddresses []string, conf ClusterConnectionConf) (*Cluster, error) {
	connPoolCache := newConnPoolCache()
	metadataConnPool, err := connPoolCache.getOrCreateConnectionPool(metadataCacheClientId, conf, nodeAddresses)
	if err != nil {
		return nil, err
	}
	clusterMetadata := newCluster(conf, metadataConnPool, connPoolCache)

	// Attempt to connect to the cluster but we want to do this with backoff and make sure we
	// don't exceed the limits.  Use the same configuration from connection pool for DialRetry.
	retry := &backoff.Backoff{Min: conf.DialRetryWait, Jitter: true}
	for try := 0; try < conf.DialRetryLimit; try++ {
		if try > 0 {
			sleepFor := retry.Duration()
			log.Infof("cannot fetch metadata from any connection (try %d, sleep %v)",
				try, sleepFor)
			time.Sleep(sleepFor)
		}

		resultChan := make(chan error, 1)
		go func() {
			resultChan <- clusterMetadata.RefreshMetadata()
		}()

		select {
		case err := <-resultChan:
			if err == nil {
				// Metadata has been refreshed, so this is ready to go
				return clusterMetadata, nil
			}
			log.Errorf("cannot fetch metadata: %s", err)
		case <-time.After(conf.DialTimeout):
			log.Error("timeout fetching metadata")
		}
	}
	return nil, errors.New("cannot connect (exhausted retries)")
}

// cache creates new internal metadata representation using data from
// given response.
//
// Do not call with partial metadata response, this assumes we have the full
// set of metadata in the response!
func (cm *Cluster) cache(resp *proto.MetadataResp) {
	if len(resp.Brokers) <= 0 {
		log.Errorf("Refusing to cache new metadata: %+v", resp)
		return
	}

	cm.mu.Lock()
	defer cm.mu.Unlock()

	log.Debugf("Caching new metadata: %+v", resp)

	cm.created = time.Now()
	cm.nodes = make(nodeMap)
	cm.endpoints = make(map[topicPartition]int32)
	cm.partitions = make(map[string]int32)

	addrs := make([]string, 0)
	for _, node := range resp.Brokers {
		addr := fmt.Sprintf("%s:%d", node.Host, node.Port)
		addrs = append(addrs, addr)
		cm.nodes[node.NodeID] = addr
	}
	for _, topic := range resp.Topics {
		for _, part := range topic.Partitions {
			dest := topicPartition{topic.Name, part.ID}
			cm.endpoints[dest] = part.Leader
		}
		cm.partitions[topic.Name] = int32(len(topic.Partitions))
	}
	cm.connPoolCache.reinitializeAddrs(addrs)
}

// connectionPoolForClient returns the ConnectionPool to this cluster for the given client ID.
func (cm *Cluster) connectionPoolForClient(clientId string, conf ClusterConnectionConf) (*ConnectionPool, error) {
	return cm.connPoolCache.getOrCreateConnectionPool(clientId, conf, cm.metadataConnPool.GetAllAddrs())
}

// Refresh is requesting metadata information from any node and refresh
// internal cached representation. This method can block for a long time depending
// on how long it takes to update metadata.
func (cm *Cluster) RefreshMetadata() error {
	updateChan := make(chan error, 1)

	go func() {
		// The goal of this code is to ensure that only one person refreshes the metadata at a time
		// and that everybody waiting for metadata can return whenever it's updated. The epoch
		// counter is updated every time we get new metadata.
		ctr1 := atomic.LoadInt64(cm.epoch)
		cm.refLock.Lock()
		defer cm.refLock.Unlock()

		ctr2 := atomic.LoadInt64(cm.epoch)
		if ctr2 > ctr1 {
			// This happens when someone else has already updated the metadata by the time
			// we have gotten the lock.
			updateChan <- nil
			return
		}

		// The counter has not updated, so it's on us to update metadata.
		log.Info("refreshing metadata")
		if meta, err := cm.Fetch(metadataCacheClientId); err == nil {
			// Update metadata + update counter to be old value plus one.
			cm.cache(meta)
			atomic.StoreInt64(cm.epoch, ctr1+1)
			updateChan <- nil
		} else {
			// An error, note we do not update the epoch. This means that the next person to
			// get the lock will try again, but we definitely return an error for this
			// particular caller.
			updateChan <- err
		}
	}()

	select {
	case err := <-updateChan:
		return err
	case <-time.After(cm.getTimeout()):
		return errors.New("timed out refreshing metadata")
	}
}

// Fetch is requesting metadata information from any node and return
// protocol response if successful. This will attempt to talk to every node at
// least once until one returns a successful response. We walk the nodes in
// a random order.
//
// If "topics" are specified, only fetch metadata for those topics (can be
// used to create a topic)
func (cm *Cluster) Fetch(clientId string, topics ...string) (*proto.MetadataResp, error) {
	// Get all addresses, then walk the array in permuted random order.
	addrs := cm.metadataConnPool.GetAllAddrs()
	log.Infof("metadata fetch addrs: %s", addrs)
	// split the timeout so that we can try getting the metadata from more than one broker.
	perBrokerTimeout := cm.getTimeout() / 2
	for _, idx := range rndPerm(len(addrs)) {
		// Directly connect, ignoring connection pool limits. This connection must be closed here.
		conn, err := newTCPConnection(addrs[idx], perBrokerTimeout)
		if err != nil {
			log.Warningf("metadata fetch failed to connect to node %s: %s", addrs[idx], err)
			continue
		}
		resp, err := conn.Metadata(&proto.MetadataReq{
			ClientID: clientId,
			Topics:   topics,
		})
		_ = conn.Close()
		if err != nil {
			log.Warningf("cannot fetch metadata from node %s: %s", addrs[idx], err)
			continue
		}
		return resp, nil
	}

	return nil, errors.New("cannot fetch metadata")
}

// PartitionCount returns how many partitions a given topic has. If a topic
// is not known, 0 and an error are returned.
func (cm *Cluster) PartitionCount(topic string) (int32, error) {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	if count, ok := cm.partitions[topic]; ok {
		return count, nil
	}
	return 0, fmt.Errorf("topic %s not found in metadata", topic)
}

// GetEndpoint returns a nodeID for a topic/partition. Returns an error if
// the topic/partition is unknown.
func (cm *Cluster) GetEndpoint(topic string, partition int32) (int32, error) {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	if nodeID, ok := cm.endpoints[topicPartition{topic, partition}]; ok {
		return nodeID, nil
	}
	return 0, errors.New("topic/partition not found in metadata")
}

// ForgetEndpoint is used to remove an endpoint that doesn't see to lead to
// a valid location.
func (cm *Cluster) ForgetEndpoint(topic string, partition int32) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	delete(cm.endpoints, topicPartition{topic, partition})
}

// GetNodes returns a map of nodes that exist in the cluster.
func (cm *Cluster) GetNodes() nodeMap {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	nodes := make(nodeMap)
	for nodeID, addr := range cm.nodes {
		nodes[nodeID] = addr
	}
	return nodes
}

// GetNodeAddress returns the address to a node if we know it.
func (cm *Cluster) GetNodeAddress(nodeID int32) string {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	addr, _ := cm.nodes[nodeID]
	return addr
}

func (cm *Cluster) getTimeout() time.Duration {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	return cm.timeout
}
