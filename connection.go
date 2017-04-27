package kafka

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net"
	"time"

	"github.com/dropbox/kafka/proto"
)

// ErrClosed is returned as result of any request made using closed connection.
var ErrClosed = errors.New("closed")

// Low level abstraction over connection to Kafka. This structure is NOT THREAD
// SAFE and must be only owned by one caller at a time.
type connection struct {
	addr      string
	startTime time.Time
	rw        io.ReadWriteCloser
	rd        *bufio.Reader
	rnd       *rand.Rand

	// stopErr is set if and only if this connection has been closed. If set, it indicates
	// the error that closed the connection.
	stopErr error
}

// newConnection returns new, initialized connection or error
func newTCPConnection(address string, timeout time.Duration) (*connection, error) {
	conn, err := net.DialTimeout("tcp", address, timeout)
	if err != nil {
		return nil, err
	}

	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))

	c := &connection{
		addr:      address,
		rw:        conn,
		rd:        bufio.NewReader(conn),
		rnd:       rnd,
		startTime: time.Now(),
	}
	return c, nil
}

// StartTime returns the time the connection was established.
func (c *connection) StartTime() time.Time {
	return c.startTime
}

// Close close underlying transport connection and cancel all pending response
// waiters.
func (c *connection) Close() error {
	if c.stopErr == nil {
		c.stopErr = ErrClosed
	}
	return c.rw.Close()
}

// IsClosed returns whether or not this connection has been stopped/closed.
func (c *connection) IsClosed() bool {
	return c.stopErr != nil
}

// sendRequest handles the raw material of sending a request up to Kafka and
// receiving the response.
func (c *connection) sendRequest(req proto.Request, reqID int32) (*bytes.Reader, error) {
	if _, err := req.WriteTo(c.rw); err != nil {
		log.Errorf("cannot write: %s", err)
		return nil, err
	}

	if correlationID, b, err := proto.ReadResp(c.rd); err != nil {
		return nil, err
	} else {
		if correlationID != reqID {
			c.Close()
			return nil, fmt.Errorf("got unexpected correlation ID %d instead of %d",
				correlationID, reqID)
		}
		return bytes.NewReader(b), nil
	}
}

// Metadata sends given metadata request to kafka node and returns related
// metadata response.
// Calling this method on closed connection will always return ErrClosed.
func (c *connection) Metadata(req *proto.MetadataReq) (*proto.MetadataResp, error) {
	if req.CorrelationID == 0 {
		req.CorrelationID = c.rnd.Int31()
	}
	if b, err := c.sendRequest(req, req.CorrelationID); err != nil {
		return nil, err
	} else {
		return proto.ReadMetadataResp(b)
	}
}

// Produce sends given produce request to kafka node and returns related
// response. Sending request with no ACKs flag will result with returning nil
// right after sending request, without waiting for response.
// Calling this method on closed connection will always return ErrClosed.
func (c *connection) Produce(req *proto.ProduceReq) (*proto.ProduceResp, error) {
	if req.CorrelationID == 0 {
		req.CorrelationID = c.rnd.Int31()
	}

	// This sad, dumb degenerate case is one where the server will never send us
	// a response. We write blindly and return.
	if req.RequiredAcks == proto.RequiredAcksNone {
		_, err := req.WriteTo(c.rw)
		return nil, err
	}

	// Normal workflow
	if b, err := c.sendRequest(req, req.CorrelationID); err != nil {
		return nil, err
	} else {
		return proto.ReadProduceResp(b)
	}
}

// Fetch sends given fetch request to kafka node and returns related response.
// Calling this method on closed connection will always return ErrClosed.
func (c *connection) Fetch(req *proto.FetchReq) (*proto.FetchResp, error) {
	var resp *proto.FetchResp

	if req.CorrelationID == 0 {
		req.CorrelationID = c.rnd.Int31()
	}
	if b, err := c.sendRequest(req, req.CorrelationID); err != nil {
		return nil, err
	} else {
		if resp, err = proto.ReadFetchResp(b); err != nil {
			return nil, err
		}
	}

	// Compressed messages are returned in full batches for efficiency
	// (the broker doesn't need to decompress).
	// This means that it's possible to get some leading messages
	// with a smaller offset than requested. Trim those.
	for ti := range resp.Topics {
		topic := &resp.Topics[ti]
		reqTopic := &req.Topics[ti]
		for pi := range topic.Partitions {
			partition := &topic.Partitions[pi]
			reqPartition := &reqTopic.Partitions[pi]
			i := 0
			for _, msg := range partition.Messages {
				if msg.Offset >= reqPartition.FetchOffset {
					break
				}
				i++
			}
			partition.Messages = partition.Messages[i:]
		}
	}
	return resp, nil
}

// Offset sends given offset request to kafka node and returns related response.
// Calling this method on closed connection will always return ErrClosed.
func (c *connection) Offset(req *proto.OffsetReq) (*proto.OffsetResp, error) {
	if req.CorrelationID == 0 {
		req.CorrelationID = c.rnd.Int31()
	}

	// TODO(husio) documentation is not mentioning this directly, but I assume
	// -1 is for non node clients
	req.ReplicaID = -1

	if b, err := c.sendRequest(req, req.CorrelationID); err != nil {
		return nil, err
	} else {
		return proto.ReadOffsetResp(b)
	}
}

func (c *connection) GroupCoordinator(req *proto.GroupCoordinatorReq) (*proto.GroupCoordinatorResp, error) {
	if req.CorrelationID == 0 {
		req.CorrelationID = c.rnd.Int31()
	}
	if b, err := c.sendRequest(req, req.CorrelationID); err != nil {
		return nil, err
	} else {
		return proto.ReadGroupCoordinatorResp(b)
	}
}

func (c *connection) OffsetCommit(req *proto.OffsetCommitReq) (*proto.OffsetCommitResp, error) {
	if req.CorrelationID == 0 {
		req.CorrelationID = c.rnd.Int31()
	}
	if b, err := c.sendRequest(req, req.CorrelationID); err != nil {
		return nil, err
	} else {
		return proto.ReadOffsetCommitResp(b)
	}
}

func (c *connection) OffsetFetch(req *proto.OffsetFetchReq) (*proto.OffsetFetchResp, error) {
	if req.CorrelationID == 0 {
		req.CorrelationID = c.rnd.Int31()
	}
	if b, err := c.sendRequest(req, req.CorrelationID); err != nil {
		return nil, err
	} else {
		return proto.ReadOffsetFetchResp(b)
	}
}
