package kafka

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dropbox/kafka/proto"
)

type clusterMetadata struct {
	conns *connectionPool
	conf  BrokerConf

	// mu protects the contents of this structure, but should only be gotten/used
	// by the clusterMetadata methods.
	mu         *sync.RWMutex
	refLock    *sync.Mutex
	epoch      *int64
	timeout    time.Duration
	created    time.Time
	nodes      nodeMap                  // node ID to address
	endpoints  map[topicPartition]int32 // partition to leader node ID
	partitions map[string]int32         // topic to numer of partitions
}

func newClusterMetadata(conf BrokerConf, pool *connectionPool) clusterMetadata {
	result := clusterMetadata{
		mu:      &sync.RWMutex{},
		timeout: conf.MetadataRefreshTimeout,
		refLock: &sync.Mutex{},
		epoch:   new(int64),
		conf:    conf,
		conns:   pool,
	}
	if conf.MetadataRefreshFrequency > 0 {
		go func() {
			log.Infof("Periodically refreshing metadata (frequency=%s)",
				conf.MetadataRefreshFrequency)
			for {
				select {
				case <-time.After(conf.MetadataRefreshFrequency):
					log.Info("Initiating periodic metadata refresh.")
					result.Refresh()
				case <-pool.ClosedChan():
					log.Info("Aborting periodic metadata refresh due to closed connection pool.")
					return
				}
			}
		}()
	}
	return result
}

// cache creates new internal metadata representation using data from
// given response.
//
// Do not call with partial metadata response, this assumes we have the full
// set of metadata in the response!
func (cm *clusterMetadata) cache(resp *proto.MetadataResp) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if !cm.created.IsZero() {
		log.Debugf("rewriting old metadata: %s", time.Now().Sub(cm.created))
	}

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
	cm.conns.InitializeAddrs(addrs)
}

// Refresh is requesting metadata information from any node and refresh
// internal cached representation. This method can block for a long time depending
// on how long it takes to update metadata.
func (cm *clusterMetadata) Refresh() error {
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
		if meta, err := cm.Fetch(); err == nil {
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
func (cm *clusterMetadata) Fetch(topics ...string) (*proto.MetadataResp, error) {
	// Get all addresses, then walk the array in permuted random order.
	addrs := cm.conns.GetAllAddrs()
	log.Infof("metadata fetch addrs: %s", addrs)
	for _, idx := range rndPerm(len(addrs)) {
		conn, err := cm.conns.GetConnectionByAddr(addrs[idx])
		if err != nil {
			continue
		}
		defer func() { go cm.conns.Idle(conn) }()

		resp, err := conn.Metadata(&proto.MetadataReq{
			ClientID: cm.conf.ClientID,
			Topics:   topics,
		})
		if err != nil {
			log.Debugf("cannot fetch metadata from node %s: %s", addrs[idx], err)
			conn.Close()
			continue
		}
		return resp, nil
	}

	return nil, errors.New("cannot fetch metadata")
}

// PartitionCount returns how many partitions a given topic has. If a topic
// is not known, 0 and an error are returned.
func (cm *clusterMetadata) PartitionCount(topic string) (int32, error) {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	if count, ok := cm.partitions[topic]; ok {
		return count, nil
	}
	return 0, fmt.Errorf("topic %s not found in metadata", topic)
}

// GetEndpoint returns a nodeID for a topic/partition. Returns an error if
// the topic/partition is unknown.
func (cm *clusterMetadata) GetEndpoint(topic string, partition int32) (int32, error) {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	if nodeID, ok := cm.endpoints[topicPartition{topic, partition}]; ok {
		return nodeID, nil
	}
	return 0, errors.New("topic/partition not found in metadata")
}

// ForgetEndpoint is used to remove an endpoint that doesn't see to lead to
// a valid location.
func (cm *clusterMetadata) ForgetEndpoint(topic string, partition int32) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	delete(cm.endpoints, topicPartition{topic, partition})
}

// GetNodes returns a map of nodes that exist in the cluster.
func (cm *clusterMetadata) GetNodes() nodeMap {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	nodes := make(nodeMap)
	for nodeID, addr := range cm.nodes {
		nodes[nodeID] = addr
	}
	return nodes
}

// GetNodeAddress returns the address to a node if we know it.
func (cm *clusterMetadata) GetNodeAddress(nodeID int32) string {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	addr, _ := cm.nodes[nodeID]
	return addr
}

func (cm *clusterMetadata) getTimeout() time.Duration {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	return cm.timeout
}
