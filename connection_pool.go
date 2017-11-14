package kafka

import (
	"errors"
	"sync"
	"time"
)

type NoConnectionsAvailable struct{}

func (NoConnectionsAvailable) Error() string {
	return "[transient] Connection pool is full (did not attempt to create new connection)."
}

// backend stores information about a given backend. All access to this data should be done
// through methods to ensure accurate counting and limiting.
type backend struct {
	conf    ClusterConnectionConf
	addr    string
	channel chan *connection

	// Used for storing links to all connections we ever make, this is a debugging
	// tool to try to help find leaks of connections. All access is protected by mu.
	mu             *sync.Mutex
	conns          []*connection
	counter        int
	debugTime      time.Time
	debugNumHitMax int
}

// getIdleConnection returns a connection if and only if there is an active, idle connection
// that already exists.
func (b *backend) GetIdleConnection() *connection {
	for {
		select {
		case conn := <-b.channel:
			if !conn.IsClosed() {
				return conn
			}
			b.removeConnection(conn)

		default:
			return nil
		}
	}
}

// GetConnection does a full connection logic: attempt to return an idle connection, if
// none are available then wait for up to the IdleConnectionWait time for one, else finally
// establish a new connection if we aren't at the limit. If we are, then continue waiting
// in increments of the idle time for a connection or the limit to come down before making
// a new connection. This could potentially block up to twice the DialTimeout.
//
// If the error returned is NoConnectionsAvailable, the caller should treat it as transient
// and not consider the backend/addr unhealthy.
func (b *backend) GetConnection() (*connection, error) {
	// dialTimeout must be longer than the configured timeout from the user to
	// differentiate the case where 'the pool is full' and 'the remote server is
	// not responding'. Since the b.conf.DialTimeout is used by the underlying
	// newTCPConnection method, we need to still be alive and waiting if it returns
	// an error at that point -- hence waiting for twice the configured timeout
	// in this method.
	dialTimeout := time.After(2 * b.conf.DialTimeout)
	for {
		select {
		// Track the overall GetConnection timeout. This will fire when we've waited
		// for too long and should return. But, if we hit this case, we can reasonably
		// believe we're actually at the connection limit b/c if it was an unhealthy
		// backend then getNewConnection would have returned an error.
		case <-dialTimeout:
			return nil, &NoConnectionsAvailable{}

		// Optimal case: a connection is immediately available in the the channel
		// where we keep idle connections.
		case conn := <-b.channel:
			if !conn.IsClosed() {
				return conn, nil
			}
			b.removeConnection(conn)

		// Wait a small amount of time for an idle connection. If nothing arrives,
		// attempt to make a new connection. This might fail if we're at the connection
		// limit, in which case we'll loop.
		case <-time.After(time.Duration(rndIntn(int(b.conf.IdleConnectionWait)))):
			conn, err := b.getNewConnection()
			if err != nil || conn != nil {
				return conn, err
			}
		}
	}
}

// debugHitMaxConnections will potentially do some debugging output to help diagnose situations
// where we're hitting connection limits.
func (b *backend) debugHitMaxConnections() {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.debugNumHitMax += 1
	now := time.Now()
	if now.Before(b.debugTime) {
		return
	}
	b.debugTime = now.Add(30 * time.Second)

	log.Debugf("DEBUG: hit max connections (%d, %d, now %d times)",
		b.counter, len(b.conns), b.debugNumHitMax)
	for idx, conn := range b.conns {
		log.Debugf("DEBUG: connection %d: addr=%s, closed=%v, age=%s",
			idx, conn.addr, conn.IsClosed(), now.Sub(conn.StartTime()))
	}
}

// getNewConnection establishes a new connection if and only if we haven't hit the limit, else
// it will return nil. If an error is returned, we failed to connect to the server and should
// abort the flow. This takes a lock on the mutex which means we can only have a single new
// connection request in-flight at one time. Takes the mutex.
func (b *backend) getNewConnection() (*connection, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Attempt to determine if we're over quota, if so, first start by cleaning out any
	// connections that are closed, then recheck. We assert that the quota is only meant
	// to count against open/in-use connections, so I don't care about closed ones.
	if b.counter >= b.conf.ConnectionLimit {
		newConns := make([]*connection, 0, b.counter)
		for _, conn := range b.conns {
			if !conn.IsClosed() {
				newConns = append(newConns, conn)
			}
		}

		// If the lengths are the same, we truly hit max connections and there's nothing
		// to do so return
		if len(newConns) == b.counter {
			go b.debugHitMaxConnections()
			return nil, nil
		}

		// We eliminated one or more closed connections, use the new list and reset our
		// counter and move forward with setup
		b.conns = newConns
		b.counter = len(newConns)
	}

	conn, err := newTCPConnection(b.addr, b.conf.DialTimeout)
	if err == nil {
		b.counter++
		b.conns = append(b.conns, conn)
	}
	return conn, err
}

// removeConnection removes the given connection from our tracking. It also decrements the
// open connection count. This takes the mutex.
func (b *backend) removeConnection(conn *connection) {
	b.mu.Lock()
	defer b.mu.Unlock()

	for idx, c := range b.conns {
		if c == conn {
			b.counter--
			b.conns = append(b.conns[0:idx], b.conns[idx+1:]...)
			return
		}
	}
}

// Idle is called when a connection should be returned to the store.
func (b *backend) Idle(conn *connection) {
	// If the connection is closed, throw it away. But if the connection pool is closed, then
	// close the connection.
	if conn.IsClosed() {
		b.removeConnection(conn)
		return
	}

	select {
	case b.channel <- conn:
		// Do nothing, connection was requeued.
	default:
		// In theory this can never happen because it means we allocated more connections than
		// the ConnectionLimit allows.
		log.Warningf("connection pool with excess connections, closing: %s", b.addr)
		b.removeConnection(conn)
		_ = conn.Close()
	}
}

// NumOpenConnections returns a counter of how may connections are open.
func (b *backend) NumOpenConnections() int {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.counter
}

// Close shuts down all connections.
func (b *backend) Close() {
	b.mu.Lock()
	defer b.mu.Unlock()

	for _, conn := range b.conns {
		_ = conn.Close()
	}
	b.counter = 0
}

type ClusterConnectionConf struct {
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

	// Any new connection dial timeout. This must be at least double the
	// IdleConnectionWait.
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
}

func NewClusterConnectionConf() ClusterConnectionConf {
	return ClusterConnectionConf{
		ConnectionLimit:          10,
		IdleConnectionWait:       200 * time.Millisecond,
		DialTimeout:              10 * time.Second,
		DialRetryLimit:           10,
		DialRetryWait:            500 * time.Millisecond,
		MetadataRefreshTimeout:   30 * time.Second,
		MetadataRefreshFrequency: 0,
	}
}

// connectionPool is a way for us to manage multiple connections to a Kafka broker in a way
// that balances out throughput with overall number of connections.
type ConnectionPool struct {
	conf ClusterConnectionConf

	// mu protects the below members of this struct. This mutex must only be used by
	// connectionPool.
	mu *sync.RWMutex
	// The keys of this map is the set of valid connection destinations, as specified by
	// InitializeAddrs. Adding an addr to this map does not initiate a connection.
	// If an addr is removed, any active backend pointing to it will be closed and no further
	// connections can be made.
	backends map[string]*backend
}

// newConnectionPool creates a connection pool and initializes it.
func NewConnectionPool(conf ClusterConnectionConf, nodes []string) *ConnectionPool {
	connPool := ConnectionPool{
		conf:     conf,
		mu:       &sync.RWMutex{},
		backends: make(map[string]*backend),
	}

	connPool.InitializeAddrs(nodes)

	return &connPool
}

// newBackend creates a new backend structure.
func (cp *ConnectionPool) newBackend(addr string) *backend {
	return &backend{
		mu:      &sync.Mutex{},
		conf:    cp.conf,
		addr:    addr,
		channel: make(chan *connection, cp.conf.ConnectionLimit),
	}
}

// getBackend fetches a backend for a given address or nil if none exists.
func (cp *ConnectionPool) getBackend(addr string) *backend {
	cp.mu.RLock()
	defer cp.mu.RUnlock()

	return cp.backends[addr]
}

// GetAllAddrs returns a slice of all addresses we've seen. Can be used for picking a random
// address or iterating the known brokers.
func (cp *ConnectionPool) GetAllAddrs() []string {
	cp.mu.RLock()
	defer cp.mu.RUnlock()

	ret := make([]string, len(cp.backends))
	i := 0
	for addr := range cp.backends {
		ret[i] = addr
		i++
	}
	return ret
}

// InitializeAddrs takes in a set of addresses and just sets up the structures for them. This
// doesn't start any connecting. This is done so that we have a set of addresses for other
// parts of the system to use.
func (cp *ConnectionPool) InitializeAddrs(addrs []string) {
	cp.mu.Lock()
	defer cp.mu.Unlock()

	deletedAddrs := make(map[string]struct{})
	for addr := range cp.backends {
		deletedAddrs[addr] = struct{}{}
	}

	for _, addr := range addrs {
		delete(deletedAddrs, addr)
		if _, ok := cp.backends[addr]; !ok {
			log.Infof("Initializing backend to addr: %s", addr)
			cp.backends[addr] = cp.newBackend(addr)
		}
	}
	for addr := range deletedAddrs {
		log.Warningf("Removing backend for addr: %s", addr)
		if backend, ok := cp.backends[addr]; ok {
			backend.Close()
			delete(cp.backends, addr)
		}
	}
}

// GetIdleConnection returns a random idle connection from the set of connections that we
// happen to have open. If no connections are available or idle, this returns nil.
func (cp *ConnectionPool) GetIdleConnection() *connection {
	addrs := cp.GetAllAddrs()

	for _, idx := range rndPerm(len(addrs)) {
		if be := cp.getBackend(addrs[idx]); be != nil {
			if conn := be.GetIdleConnection(); conn != nil {
				return conn
			}
		}
	}
	return nil
}

// GetConnectionByAddr takes an address and returns a valid/open connection to this server.
// We attempt to reuse connections if we can, but if a connection is not available within
// IdleConnectionWait then we'll establish a new one. This can block a long time.
//
// See comments on GetConnection for details on the error returned.
func (cp *ConnectionPool) GetConnectionByAddr(addr string) (*connection, error) {
	if be := cp.getBackend(addr); be != nil {
		return be.GetConnection()
	}
	return nil, errors.New("no backend for addr")
}

// Idle takes a now idle connection and makes it available for other users. This should be
// called in a goroutine so as not to block the original caller, as this function may take
// some time to return.
func (cp *ConnectionPool) Idle(conn *connection) {
	if conn == nil {
		return
	}

	if be := cp.getBackend(conn.addr); be != nil {
		be.Idle(conn)
	} else {
		_ = conn.Close()
	}
}
