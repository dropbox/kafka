package kafka

import (
	"time"

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
	conf.IdleConnectionLimit = 1
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

	// Idle both and get idle twice, only saves one
	c.Assert(conn.IsClosed(), Equals, false)
	cp.Idle(conn)
	c.Assert(be.NumOpenConnections(), Equals, 2)
	c.Assert(conn2.IsClosed(), Equals, false)
	cp.Idle(conn2)
	c.Assert(be.NumOpenConnections(), Equals, 1)
	conn = cp.GetIdleConnection()
	c.Assert(conn, NotNil)
	c.Assert(cp.GetIdleConnection(), IsNil)
	c.Assert(be.NumOpenConnections(), Equals, 1)

	// Close connection and check counts
	conn.Close()
	c.Assert(be.NumOpenConnections(), Equals, 1)
	cp.Idle(conn)
	c.Assert(be.NumOpenConnections(), Equals, 0)
}

func (s *ConnectionPoolSuite) TestTrimDeadAddrs(c *C) {
	cp := newConnectionPool(NewBrokerConf("foo"))
	cp.InitializeAddrs([]string{"foo", "bar", "baz"})
	c.Assert(len(cp.GetAllAddrs()), Equals, 3)
	c.Assert(cp.getBackend("foo"), Not(IsNil))
	c.Assert(cp.getBackend("qux"), IsNil)
	cp.InitializeAddrs([]string{"qux"})
	c.Assert(len(cp.GetAllAddrs()), Equals, 1)
	c.Assert(cp.getBackend("qux"), Not(IsNil))
	c.Assert(cp.getBackend("foo"), IsNil)
}
