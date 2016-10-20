package kafka

import (
	"errors"
	"sync"
	"time"
)

// backend stores information about a given backend. All access to this data should be done
// through methods to ensure accurate counting and limiting.
type backend struct {
	conf    BrokerConf
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
// a new connection. This could potentially block up to the DialTimeout.
func (b *backend) GetConnection() *connection {
	dialTimeout := time.After(b.conf.DialTimeout)
	for {
		select {
		case <-dialTimeout:
			return nil

		case conn := <-b.channel:
			if !conn.IsClosed() {
				return conn
			}
			b.removeConnection(conn)

		case <-time.After(time.Duration(rndIntn(int(b.conf.IdleConnectionWait)))):
			conn, err := b.getNewConnection()
			if err != nil {
				return nil
			} else if conn != nil {
				return conn
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
		log.Debugf("DEBUG: connection %d: addr=%s, closed=%s, age=%s",
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

	// Be careful about the situation where newTCPConnection could never return, so
	// we want to always make sure getNewConnection eventually returns. Else, we can
	// lose the connection pool.

	type connResult struct {
		conn *connection
		err  error
	}
	connChan := make(chan connResult, 1)

	go func() {
		log.Debugf("making new connection: %s", b.addr)
		if conn, err := newTCPConnection(b.addr, b.conf.DialTimeout); err != nil {
			log.Errorf("cannot connect to %s: %s", b.addr, err)
			connChan <- connResult{nil, err}
		} else {
			connChan <- connResult{conn, nil}
		}
	}()

	select {
	case <-time.After(b.conf.DialTimeout):
		log.Errorf("DEBUG: timeout waiting for dial: %s", b.addr)
		return nil, nil

	case result := <-connChan:
		if result.err != nil {
			return nil, result.err
		} else {
			b.counter++
			b.conns = append(b.conns, result.conn)
			return result.conn, nil
		}
	}

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

	// If we're above the idle connection limit, discard the connection.
	if len(b.channel) >= b.conf.IdleConnectionLimit {
		conn.Close()
		b.removeConnection(conn)
		return
	}

	select {
	case b.channel <- conn:
		// Do nothing, connection was requeued.
	case <-time.After(b.conf.IdleConnectionWait):
		// The queue is full for a while, discard this connection.
		b.removeConnection(conn)
		conn.Close()
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
		conn.Close()
	}
	b.counter = 0
}

// connectionPool is a way for us to manage multiple connections to a Kafka broker in a way
// that balances out throughput with overall number of connections.
type connectionPool struct {
	conf BrokerConf

	// mu protects the below members of this struct. This mutex must only be used by
	// connectionPool.
	mu         *sync.RWMutex
	closed     bool
	closedChan chan struct{}
	// The keys of this map is the set of valid connection destinations, as specified by
	// InitializeAddrs. Adding an addr to this map does not initiate a connection.
	// If an addr is removed, any active backend pointing to it will be closed and no further
	// connections can be made.
	backends map[string]*backend
}

// newConnectionPool creates a connection pool and initializes it.
func newConnectionPool(conf BrokerConf) connectionPool {
	return connectionPool{
		conf:       conf,
		mu:         &sync.RWMutex{},
		closedChan: make(chan struct{}),
		backends:   make(map[string]*backend),
	}
}

// newBackend creates a new backend structure.
func (cp *connectionPool) newBackend(addr string) *backend {
	return &backend{
		mu:      &sync.Mutex{},
		conf:    cp.conf,
		addr:    addr,
		channel: make(chan *connection, cp.conf.IdleConnectionLimit),
	}
}

// getBackend fetches a backend for a given address or nil if none exists.
func (cp *connectionPool) getBackend(addr string) *backend {
	cp.mu.RLock()
	defer cp.mu.RUnlock()

	if cp.closed {
		return nil
	}
	return cp.backends[addr]
}

// GetAllAddrs returns a slice of all addresses we've seen. Can be used for picking a random
// address or iterating the known brokers.
func (cp *connectionPool) GetAllAddrs() []string {
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
func (cp *connectionPool) InitializeAddrs(addrs []string) {
	cp.mu.Lock()
	defer cp.mu.Unlock()

	if cp.closed {
		log.Warning("Cannot InitializeAddrs on closed connectionPool.")
		return
	}

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
func (cp *connectionPool) GetIdleConnection() *connection {
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
func (cp *connectionPool) GetConnectionByAddr(addr string) (*connection, error) {
	if cp.IsClosed() {
		return nil, errors.New("connection pool is closed")
	}

	if be := cp.getBackend(addr); be != nil {
		if conn := be.GetConnection(); conn != nil {
			return conn, nil
		}
	}
	return nil, errors.New("failed to get connection")
}

// Close sets the connection pool's end state, no further connections will be returned
// and any existing connections will be closed out.
func (cp *connectionPool) Close() {
	cp.mu.Lock()
	defer cp.mu.Unlock()

	for _, backend := range cp.backends {
		backend.Close()
	}
	cp.backends = nil
	if !cp.closed {
		close(cp.closedChan)
	}
	cp.closed = true
}

// ClosedChan returns a channel which remains open iff this connectionPool is open. Equivalent
// to polling IsClosed.
func (cp *connectionPool) ClosedChan() <-chan struct{} {
	return cp.closedChan
}

// IsClosed returns whether or not this pool is closed. Equiavelent to checking if the
// result of ClosedChan is closed.
func (cp *connectionPool) IsClosed() bool {
	cp.mu.RLock()
	defer cp.mu.RUnlock()

	return cp.closed
}

// Idle takes a now idle connection and makes it available for other users. This should be
// called in a goroutine so as not to block the original caller, as this function may take
// some time to return.
func (cp *connectionPool) Idle(conn *connection) {
	if conn == nil {
		return
	}

	if be := cp.getBackend(conn.addr); be != nil {
		be.Idle(conn)
	} else {
		conn.Close()
	}
}
