package kafka

import (
	"time"

	"github.com/dropbox/kafka/proto"

	. "gopkg.in/check.v1"
)

var _ = Suite(&ConnectionPoolSuite{})

type ConnectionPoolSuite struct{}

func (s *ConnectionPoolSuite) SetUpTest(c *C) {
	ResetTestLogger(c)
}

func (s *ConnectionPoolSuite) TestConnectionLimit(c *C) {
	srv := NewServer()
	srv.Start()
	defer srv.Close()

	conf := NewBrokerConf("foo")
	conf.ConnectionLimit = 2
	conf.DialTimeout = 1 * time.Second
	cp := newConnectionPool(conf)
	cp.InitializeAddrs([]string{srv.Address()})
	be := cp.getBackend(srv.Address())

	// Get idle - nothing
	c.Assert(cp.GetIdleConnection(), IsNil)
	c.Assert(be.NumOpenConnections(), Equals, 0)

	// Get new connection - works
	conn, err := cp.GetConnectionByAddr(srv.Address())
	c.Assert(err, IsNil)
	c.Assert(conn, NotNil)
	c.Assert(be.NumOpenConnections(), Equals, 1)
	cp.Idle(conn)
	c.Assert(be.NumOpenConnections(), Equals, 1)

	// Get idle - have something
	conn = cp.GetIdleConnection()
	c.Assert(conn.IsClosed(), Equals, false)
	c.Assert(conn, NotNil)
	c.Assert(be.NumOpenConnections(), Equals, 1)

	// Get another idle - nothing
	conn2 := cp.GetIdleConnection()
	c.Assert(conn2, IsNil)
	c.Assert(be.NumOpenConnections(), Equals, 1)

	// Get a new conn - something
	conn2, err = cp.GetConnectionByAddr(srv.Address())
	c.Assert(conn2.IsClosed(), Equals, false)
	c.Assert(err, IsNil)
	c.Assert(conn2, NotNil)
	c.Assert(be.NumOpenConnections(), Equals, 2)

	// Try to get 3rd, it will not work
	conn3, err := cp.GetConnectionByAddr(srv.Address())
	c.Assert(err, NotNil)
	c.Assert(conn3, IsNil)
	c.Assert(be.NumOpenConnections(), Equals, 2)

	// Idle both and get idle twice, both come back.
	c.Assert(conn.IsClosed(), Equals, false)
	cp.Idle(conn)
	c.Assert(be.NumOpenConnections(), Equals, 2)
	c.Assert(conn2.IsClosed(), Equals, false)
	cp.Idle(conn2)
	c.Assert(be.NumOpenConnections(), Equals, 2)
	conn = cp.GetIdleConnection()
	c.Assert(conn, NotNil)
	c.Assert(cp.GetIdleConnection(), NotNil)
	c.Assert(be.NumOpenConnections(), Equals, 2)

	// Close connection and check counts
	conn.Close()
	c.Assert(be.NumOpenConnections(), Equals, 2)
	cp.Idle(conn)
	c.Assert(be.NumOpenConnections(), Equals, 1)
}

func (s *ConnectionPoolSuite) TestGetConnectionError(c *C) {
	srv := NewServer()
	srv.Start()

	conf := NewBrokerConf("foo")
	conf.ConnectionLimit = 1
	conf.DialTimeout = 1 * time.Second
	conf.IdleConnectionWait = 200 * time.Millisecond

	cp := newConnectionPool(conf)
	cp.InitializeAddrs([]string{srv.Address()})
	be := cp.getBackend(srv.Address())

	// First connection should work
	conn, err := cp.GetConnectionByAddr(srv.Address())
	c.Assert(err, IsNil)
	c.Assert(conn, NotNil)
	c.Assert(be.NumOpenConnections(), Equals, 1)

	// Second connection should return NoConnectionsAvailable because
	// we're at the connection limit
	conn2, err := cp.GetConnectionByAddr(srv.Address())
	c.Assert(err, NotNil)
	_, ok := err.(*NoConnectionsAvailable)
	c.Assert(ok, Equals, true)
	c.Assert(conn2, IsNil)
	c.Assert(be.NumOpenConnections(), Equals, 1)

	// Close the server
	srv.Close()
	_, err = conn.Metadata(&proto.MetadataReq{})
	c.Assert(err, NotNil)
	c.Assert(conn.IsClosed(), Equals, true)
	be.Idle(conn)

	// Now getting a connection should return a different error b/c
	// it can't connect -- this must be true so that we trigger a
	// metadata refresh (i.e. when a broker dies)
	conn3, err := cp.GetConnectionByAddr(srv.Address())
	c.Assert(err, NotNil)
	_, ok = err.(*NoConnectionsAvailable)
	c.Assert(ok, Equals, false) // it's not NoConnectionsAvailable
	c.Assert(conn3, IsNil)
}

func (s *ConnectionPoolSuite) TestTrimDeadAddrs(c *C) {
	cp := newConnectionPool(NewBrokerConf("foo"))
	cp.InitializeAddrs([]string{"foo", "bar", "baz"})
	c.Assert(len(cp.GetAllAddrs()), Equals, 3)
	c.Assert(cp.getBackend("foo"), NotNil)
	c.Assert(cp.getBackend("qux"), IsNil)
	cp.InitializeAddrs([]string{"qux"})
	c.Assert(len(cp.GetAllAddrs()), Equals, 1)
	c.Assert(cp.getBackend("qux"), NotNil)
	c.Assert(cp.getBackend("foo"), IsNil)
}
