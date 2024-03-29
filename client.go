package redis

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"time"
)

// DialDelayMax is the idle limit for automated reconnect attempts.
// Sequential failure with connection establisment increases the retry
// delay in steps from 0 to 500 ms.
const DialDelayMax = time.Second / 2

// Fixed Settings
const (
	// Number of pending requests limit per network protocol.
	queueSizeTCP  = 128
	queueSizeUnix = 512
)

// ErrConnLost signals connection loss on pending request.
var errConnLost = errors.New("redis: connection lost while awaiting response")

// ClientConfig defines a Client setup.
type ClientConfig struct {
	// The host defaults to localhost, and the port defaults to 6379.
	// Thus, the empty string defaults to "localhost:6379". Use an
	// absolute file path (e.g. "/var/run/redis.sock") for Unix
	// domain sockets.
	Addr string

	// Limit execution duration when nonzero. Expiry causes a reconnect
	// to prevent stale connections and a net.Error with Timeout() true.
	CommandTimeout time.Duration

	// Limit the duration for network connection establishment. Expiry
	// causes an abort plus retry. See net.Dialer Timeout for details.
	// Zero defaults to one second.
	//
	// Command submission blocks during the first dial attempt. When the
	// connect fails, then command submission receives the error of the last
	// connect attempt until the connection restores.
	DialTimeout time.Duration

	// AUTH when not nil.
	Password []byte

	// SELECT when not zero.
	DB int64
}

// Client manages a connection to a Redis node until Close. Broken connection
// states cause automated reconnects.
//
// Multiple goroutines may invoke methods on a Client simultaneously. Command
// invocation applies <https://redis.io/topics/pipelining> on concurrency.
type Client[Key, Value String] struct {
	ClientConfig // read-only attributes

	noCopy noCopy

	// The connection semaphore is used as a write lock.
	connSem chan *redisConn

	// Requests are send with a redisConn. The buffering reader attached to
	// a redisConn is used to read the response. Requests enqueue a callback
	// channel to parse the response in pipeline order. A nil Reader receive
	// implies connection loss.
	// Insertion must hold the write lock (connSem).
	readQueue chan chan<- *bufio.Reader

	// A send/receive halts the read routine. The bufio.Reader is discarded.
	// No more consumption on ReadQueue.
	// Insertion must hold the write lock (connSem).
	readTerm chan struct{}
}

// NewDefaultClient launches a managed connection to a node (address).
// Both CommandTimeout and DialTimeout are set to one second.
func NewDefaultClient[Key, Value String](addr string) *Client[Key, Value] {
	return NewClient[Key, Value](ClientConfig{
		Addr:           addr,
		CommandTimeout: time.Second,
		DialTimeout:    time.Second,
	})
}

// NewClient launches a managed connection to a node (address).
func NewClient[Key, Value String](config ClientConfig) *Client[Key, Value] {
	config.Addr = normalizeAddr(config.Addr)
	if config.DialTimeout == 0 {
		config.DialTimeout = time.Second
	}

	queueSize := queueSizeTCP
	if isUnixAddr(config.Addr) {
		queueSize = queueSizeUnix
	}

	c := &Client[Key, Value]{
		ClientConfig: config,

		connSem:   make(chan *redisConn, 1),
		readQueue: make(chan chan<- *bufio.Reader, queueSize),
		readTerm:  make(chan struct{}),
	}

	go c.connectOrClosed()

	return c
}

type redisConn struct {
	net.Conn       // nil when offline
	offline  error // reason for connection absence

	// The token is nil when a read routine is using it.
	idle *bufio.Reader
}

// Close terminates the connection establishment.
// Command submission is stopped with ErrClosed.
// All pending commands are dealt with on return.
// Calling Close more than once has no effect.
func (c *Client[Key, Value]) Close() error {
	conn := <-c.connSem // lock write
	if conn.offline == ErrClosed {
		// redundant invocation
		c.connSem <- conn // unlock write
		return nil
	}

	if conn.offline == nil && conn.idle == nil {
		// must hold write lock for insertion:
		c.readTerm <- struct{}{}
		// race unlikely yet possible
		c.cancelQueue()
	}

	// stop command submission (unlocks write)
	c.connSem <- &redisConn{offline: ErrClosed}

	if conn.Conn != nil {
		return conn.Close()
	}
	return nil
}

// connectOrClosed populates the connection semaphore.
func (c *Client[Key, Value]) connectOrClosed() {
	var retryDelay time.Duration
	for {
		conn, reader, err := c.connect(conservativeMSS)
		if err != nil {
			retry := time.NewTimer(retryDelay)

			// remove previous connect error unless closed
			if retryDelay != 0 {
				current := <-c.connSem
				if current.offline == ErrClosed {
					c.connSem <- current // restore
					retry.Stop()         // cleanup
					return               // abandon
				}
			}
			// propagate current connect error
			c.connSem <- &redisConn{offline: fmt.Errorf("redis: offline due %w", err)}

			retryDelay = 2*retryDelay + time.Millisecond
			if retryDelay > DialDelayMax {
				retryDelay = DialDelayMax
			}
			<-retry.C
			continue
		}

		// remove previous connect error unless closed
		if retryDelay != 0 {
			current := <-c.connSem
			if current.offline == ErrClosed {
				c.connSem <- current // restore
				conn.Close()         // discard
				return               // abandon
			}
		}

		// release
		c.connSem <- &redisConn{Conn: conn, idle: reader}
		return
	}
}

func (c *Client[Key, Value]) cancelQueue() {
	for {
		select {
		case ch := <-c.readQueue:
			// signal connection loss
			ch <- (*bufio.Reader)(nil)
		default:
			return
		}
	}
}

// Exchange sends a request, and then it awaits its turn (in the pipeline) for
// response receiption.
func (c *Client[Key, Value]) exchange(req *request) (*bufio.Reader, error) {
	conn := <-c.connSem // lock write

	// validate connection state
	if err := conn.offline; err != nil {
		c.connSem <- conn // unlock write
		return nil, err
	}

	// apply time-out if set
	var deadline time.Time
	if c.CommandTimeout != 0 {
		deadline = time.Now().Add(c.CommandTimeout)
		conn.SetWriteDeadline(deadline)
	}

	// send command
	if _, err := conn.Write(req.buf); err != nil {
		// write remains locked (until connectOrClosed)
		go func() {
			if conn.idle == nil {
				// read routine running
				// must hold write lock for insertion:
				c.readTerm <- struct{}{}
				c.cancelQueue()
			}
			conn.Close()
			c.connectOrClosed()
		}()
		return nil, err
	}

	reader := conn.idle
	if reader != nil {
		// clear idle state; we're the read routine now
		conn.idle = nil
		// receive channel not used as first in line
		req.free()
	} else {
		// read routine is running; wait in line
		// must hold write lock for insertion:
		c.readQueue <- req.receive
	}

	c.connSem <- conn // unlock write

	if reader == nil {
		// await response turn in pipeline
		reader = <-req.receive
		req.free()
		if reader == nil {
			// queue abandonment
			return nil, errConnLost
		}
	}

	if !deadline.IsZero() {
		conn.SetReadDeadline(deadline)
	}

	return reader, nil
}

func (c *Client[Key, Value]) commandOK(req *request) error {
	r, err := c.exchange(req)
	if err != nil {
		return err
	}
	err = readOK(r)
	c.passRead(r, err)
	return err
}

func (c *Client[Key, Value]) commandOKOrReconnect(req *request) error {
	r, err := c.exchange(req)
	if err != nil {
		return err
	}
	err = readOK(r)
	if err != nil {
		c.dropConnFromRead()
	} else {
		c.passRead(r, nil)
	}
	return err
}

func (c *Client[Key, Value]) commandInteger(req *request) (int64, error) {
	r, err := c.exchange(req)
	if err != nil {
		return 0, err
	}
	integer, err := readInteger(r)
	c.passRead(r, err)
	return integer, err
}

func (c *Client[Key, Value]) commandBulk(req *request) (bulk Value, _ error) {
	r, err := c.exchange(req)
	if err != nil {
		return bulk, err
	}
	bulk, err = readBulk[Value](r)
	c.passRead(r, err)
	if err == errNull {
		err = nil
	}
	return bulk, err
}

func (c *Client[Key, Value]) commandArray(req *request) ([]Value, error) {
	r, err := c.exchange(req)
	if err != nil {
		return nil, err
	}
	array, err := readArray[Value](r)
	c.passRead(r, err)
	if err == errNull {
		err = nil
	}
	return array, err
}

// PassRead hands over the buffered reader to the following command in line. It
// goes in idle mode (on the redisConn from connSem) when all requests are done
// for.
func (c *Client[Key, Value]) passRead(r *bufio.Reader, err error) {
	switch err {
	case nil, errNull:
		break
	default:
		_, ok := err.(ServerError)
		if !ok {
			// got an I/O error on response
			c.dropConnFromRead()
			return
		}
	}

	// pass r to enqueued
	select {
	case next := <-c.readQueue:
		next <- r // direct pass
		return
	default:
		break
	}

	// go idle
	select {
	case next := <-c.readQueue:
		// request enqueued while awaiting lock
		next <- r // pass after all

	// Acquire write lock to make the idle decision atomic, as
	// readQueue insertion (in exchange) operates within the lock.
	case conn := <-c.connSem:
		// write locked
		select {
		case next := <-c.readQueue:
			// lost race while awaiting lock
			next <- r // pass after all
		default:
			conn.idle = r // go idle mode
		}
		c.connSem <- conn // unlock write

	case <-c.readTerm:
		break // accept halt; discard r
	}
}

// DropConnFromRead disconnects with Redis.
func (c *Client[Key, Value]) dropConnFromRead() {
	for {
		select {
		case <-c.readTerm:
			// accept halt; let sender drop conn
			return

		// A write (lock owner) blocks on a full queue,
		// so include discard here to prevent deadlock.
		case next := <-c.readQueue:
			// signal connection loss
			next <- (*bufio.Reader)(nil)

		case conn := <-c.connSem:
			// write locked
			if conn.offline != nil {
				c.connSem <- conn // unlock write
			} else {
				// write remains locked (until connectOrClosed)
				go func() {
					conn.Close()
					c.cancelQueue()
					c.connectOrClosed()
				}()
			}

			return
		}
	}
}

func (c *ClientConfig) connect(readBufferSize int) (net.Conn, *bufio.Reader, error) {
	network := "tcp"
	if isUnixAddr(c.Addr) {
		network = "unix"
	}
	conn, err := net.DialTimeout(network, c.Addr, c.DialTimeout)
	if err != nil {
		return nil, nil, err
	}

	// connection tuning
	if tcp, ok := conn.(*net.TCPConn); ok {
		tcp.SetNoDelay(false)
		tcp.SetLinger(0)
	}
	reader := bufio.NewReaderSize(conn, readBufferSize)

	// apply sticky settings
	if c.Password != nil {
		req := requestWithString("*2\r\n$4\r\nAUTH\r\n$", c.Password)
		defer req.free()

		if c.CommandTimeout != 0 {
			conn.SetDeadline(time.Now().Add(c.CommandTimeout))
			defer conn.SetDeadline(time.Time{})
		}
		_, err := conn.Write(req.buf)
		// ⚠️ reverse/delayed error check
		if err == nil {
			err = readOK(reader)
		}
		if err != nil {
			conn.Close()
			return nil, nil, fmt.Errorf("redis: AUTH on new connection: %w", err)
		}
	}

	if c.DB != 0 {
		req := requestWithDecimal("*2\r\n$6\r\nSELECT\r\n$", c.DB)
		defer req.free()

		if c.CommandTimeout != 0 {
			conn.SetDeadline(time.Now().Add(c.CommandTimeout))
			defer conn.SetDeadline(time.Time{})
		}
		_, err := conn.Write(req.buf)
		// ⚠️ reverse/delayed error check
		if err == nil {
			err = readOK(reader)
		}
		if err != nil {
			conn.Close()
			return nil, nil, fmt.Errorf("redis: SELECT on new connection: %w", err)
		}
	}

	return conn, reader, nil
}

// noCopy may be embedded into structs which must not be copied
// after the first use.
//
// See https://golang.org/issues/8005#issuecomment-190753527
// for details.
type noCopy struct{}

// Lock is a no-op used by -copylocks checker from `go vet`.
func (*noCopy) Lock()   {}
func (*noCopy) Unlock() {}
