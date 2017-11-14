package kafka

import (
	"sync"
)

const (
	// This will be used as the ClientId for any metadata requests.
	metadataCacheClientId = "metadata-cache"
)

var globalMetadataCacheLock sync.Mutex
var globalMetadataCache *MetadataCache

// If initialized, Kafka connections will be cached globally.
func InitializeMetadataCache() {
	globalMetadataCacheLock.Lock()
	defer globalMetadataCacheLock.Unlock()
	globalMetadataCache = newMetadataCache()
}

// Only useful for internal tests.
func uninitializeMetadataCache() {
	globalMetadataCacheLock.Lock()
	defer globalMetadataCacheLock.Unlock()
	globalMetadataCache = nil
}

func getMetadataCache() *MetadataCache {
	globalMetadataCacheLock.Lock()
	defer globalMetadataCacheLock.Unlock()
	if globalMetadataCache != nil {
		return globalMetadataCache
	} else {
		log.Infof("Creating metadata without using cache.")
		return newMetadataCache()
	}
}

// MetadataCache is a threadsafe cache of ClusterMetadata by clusterName.  Entries are never removed
// or changed so the implementation is straightforward: use the existing entry if it exists,
// otherwise take the lock, check again if there is an existing entry, and add a new entry if not.
type MetadataCache struct {
	lock        sync.Mutex
	metadataMap map[string]*Cluster
}

func newMetadataCache() *MetadataCache {
	return &MetadataCache{
		lock:        sync.Mutex{},
		metadataMap: make(map[string]*Cluster),
	}
}

// getOrCreateMetadata creates or gets the existing broker from the MetadataCache for the given
// cluster nodeAddresses.
func (g *MetadataCache) getOrCreateMetadata(
	clusterName string, nodeAddresses []string, conf ClusterConnectionConf) (*Cluster, error) {
	g.lock.Lock()
	defer g.lock.Unlock()

	log.Infof("Retrieving metadata for cluster %s from LockingMap", clusterName)
	if clusterMetadata, ok := g.metadataMap[clusterName]; ok {
		return clusterMetadata, nil
	}
	log.Infof("Metadata for cluster being created.")

	// Metadata requests are limited to 1 per cluster anyway.
	conf.ConnectionLimit = 1

	clusterMetadata, err := NewCluster(nodeAddresses, conf)
	if err != nil {
		return nil, err
	}
	g.metadataMap[clusterName] = clusterMetadata
	return clusterMetadata, nil
}
