package kafka

import (
	"sync"
)

// Caches connections to a single cluster by ClientID.
type ConnectionPoolCache struct {
	lock              sync.Mutex
	connectionPoolMap map[string]*ConnectionPool
}

// ConnectionPoolCache is a threadsafe cache of ConnectionPool by clientID.  One ConnectionPoolCache
// should have ConnectionPools to only one cluster.  The implementation is similar to MetadataCache,
// except that it also keeps track of a list of the ConnectionPools cached so it can refresh the
// addresses on each of them in reinitializeAddrs.
func newConnPoolCache() *ConnectionPoolCache {
	return &ConnectionPoolCache{
		lock:              sync.Mutex{},
		connectionPoolMap: make(map[string]*ConnectionPool),
	}

}

// getOrCreateConnectionPool creates or gets the existing broker from the ConnectionPoolCache for
// the given (serviceName, clientId) tuple.
func (c *ConnectionPoolCache) getOrCreateConnectionPool(clientId string, conf ClusterConnectionConf, nodeAddresses []string) (*ConnectionPool, error) {
	c.lock.Lock()
	defer c.lock.Unlock()

	log.Infof("Retrieving connection pool for clientId %s from LockingMap", clientId)
	if connectionPool, ok := c.connectionPoolMap[clientId]; ok {
		return connectionPool, nil
	}
	log.Infof("ConnectionPool for cluster %s being created.", nodeAddresses)

	connPool := NewConnectionPool(conf, nodeAddresses)
	c.connectionPoolMap[clientId] = connPool
	return connPool, nil
}

func (c *ConnectionPoolCache) reinitializeAddrs(nodeAddresses []string) {
	c.lock.Lock()
	defer c.lock.Unlock()
	for _, pool := range c.connectionPoolMap {
		pool.InitializeAddrs(nodeAddresses)
	}
}
