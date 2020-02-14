package redis

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"sync/atomic"
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

// ErrConnLost signals connection loss to response queue.
var errConnLost = errors.New("redis: connection lost while awaiting response")

// Client manages a connection to a Redis node until Close. Broken connection
// states cause automated reconnects.
//
// Multiple goroutines may invoke methods on a Client simultaneously. Command
// invocation applies <https://redis.io/topics/pipelining> on concurrency.
type Client struct {
	// Normalized node address in use. This field is read-only.
	Addr string

	noCopy noCopy

	// sticky AUTH(entication)
	password atomic.Value

	// sticky database SELECT
	db int64

	// optional execution expiry
	commandTimeout time.Duration

	// network establishment expiry
	dialTimeout time.Duration

	// The connection semaphore is used as a write lock.
	connSem chan *redisConn

	// The buffering reader from redisConn is used as a read lock.
	// Command submission holds the write lock [connSem] when sending
	// to readQueue.
	readQueue chan chan<- *bufio.Reader

	// The read routine stops on receive: no more readQueue receives
	// nor network use. The idle state is not set/restored.
	readInterrupt chan struct{}
}

// NewClient launches a managed connection to a node (address).
// The host defaults to localhost, and the port defaults to 6379.
// Thus, the empty string defaults to "localhost:6379". Use an
// absolute file path (e.g. "/var/run/redis.sock") for Unix
// domain sockets.
//
// A command time-out limits execution duration when nonzero. Expiry causes a
// reconnect (to prevent stale connections) and a net.Error with Timeout() true.
//
// The dial time-out limits the duration for network connection establishment.
// Expiry causes an abort + retry. Zero defaults to one second. Any command
// submission blocks on the first attempt. When connection establishment fails,
// then command submission receives the error of the last attempt, until the
// connection restores.
func NewClient(addr string, commandTimeout, dialTimeout time.Duration) *Client {
	addr = normalizeAddr(addr)
	if dialTimeout == 0 {
		dialTimeout = time.Second
	}
	queueSize := queueSizeTCP
	if isUnixAddr(addr) {
		queueSize = queueSizeUnix
	}

	c := &Client{
		Addr:           addr,
		commandTimeout: commandTimeout,
		dialTimeout:    dialTimeout,

		connSem:       make(chan *redisConn, 1),
		readQueue:     make(chan chan<- *bufio.Reader, queueSize),
		readInterrupt: make(chan struct{}),
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
func (c *Client) Close() error {
	conn := <-c.connSem
	if conn.offline == ErrClosed {
		// redundant invocation
		c.connSem <- conn // restore
		return nil
	}

	// stop command submission
	c.connSem <- &redisConn{offline: ErrClosed}

	c.haltReceive(conn)
	c.cancelQueue()

	if conn.Conn != nil {
		return conn.Close()
	}
	return nil
}

// connectOrClosed populates the connection semaphore.
func (c *Client) connectOrClosed() {
	var retryDelay time.Duration
	for {
		config := connConfig{
			BufferSize:     conservativeMSS,
			Addr:           c.Addr,
			DB:             atomic.LoadInt64(&c.db),
			CommandTimeout: c.commandTimeout,
			DialTimeout:    c.dialTimeout,
		}
		config.Password, _ = c.password.Load().([]byte)
		conn, reader, err := connect(config)
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

// CancelQueue signals connection loss to all pending commands.
func (c *Client) cancelQueue() {
	for n := len(c.readQueue); n > 0; n-- {
		(<-c.readQueue) <- (*bufio.Reader)(nil)
	}
}

// Submit sends a request, and deals with response ordering.
func (c *Client) submit(req *request) (*bufio.Reader, error) {
	// operate in write lock
	conn := <-c.connSem

	// validate connection state
	if err := conn.offline; err != nil {
		c.connSem <- conn // restore
		return nil, err
	}

	// apply time-out if set
	var deadline time.Time
	if c.commandTimeout != 0 {
		deadline = time.Now().Add(c.commandTimeout)
		conn.SetWriteDeadline(deadline)
	}

	// send command
	if _, err := conn.Write(req.buf); err != nil {
		// write remains locked
		go func() {
			c.haltReceive(conn)
			c.cancelQueue()
			conn.Close()
			c.connectOrClosed()
		}()
		return nil, err
	}

	reader := conn.idle
	if reader != nil {
		// Own the virtual read lock by clearing the idle state.
		conn.idle = nil
		// The receive channel is not used, as we're next in line.
		req.free()
	} else {
		// The virtual read lock is processing the queue.
		c.readQueue <- req.receive
	}

	c.connSem <- conn // release write lock

	if reader == nil {
		// await handover of virtual read lock
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

func (c *Client) commandOK(req *request) error {
	r, err := c.submit(req)
	if err != nil {
		return err
	}
	err = decodeOK(r)
	c.pass(r, err)
	return err
}

func (c *Client) commandOKOrReconnect(req *request) error {
	r, err := c.submit(req)
	if err != nil {
		return err
	}
	err = decodeOK(r)
	if err != nil {
		c.dropConn()
	} else {
		c.pass(r, nil)
	}
	return err
}

func (c *Client) commandOKAndReconnect(req *request) error {
	r, err := c.submit(req)
	if err != nil {
		return err
	}
	err = decodeOK(r)
	c.dropConn()
	return err
}

func (c *Client) commandInteger(req *request) (int64, error) {
	r, err := c.submit(req)
	if err != nil {
		return 0, err
	}
	integer, err := decodeInteger(r)
	c.pass(r, err)
	return integer, err
}

func (c *Client) commandBlobBytes(req *request) ([]byte, error) {
	r, err := c.submit(req)
	if err != nil {
		return nil, err
	}
	bytes, err := decodeBlobBytes(r)
	c.pass(r, err)
	if err == errNull {
		return nil, nil
	}
	return bytes, err
}

func (c *Client) commandBlobString(req *request) (string, bool, error) {
	r, err := c.submit(req)
	if err != nil {
		return "", false, err
	}
	s, err := decodeBlobString(r)
	c.pass(r, err)
	if err == errNull {
		return "", false, nil
	}
	return s, true, err
}

func (c *Client) commandBytesArray(req *request) ([][]byte, error) {
	r, err := c.submit(req)
	if err != nil {
		return nil, err
	}
	array, err := decodeBytesArray(r)
	c.pass(r, err)
	if err == errNull {
		return nil, nil
	}
	return array, err
}

func (c *Client) commandStringArray(req *request) ([]string, error) {
	r, err := c.submit(req)
	if err != nil {
		return nil, err
	}
	array, err := decodeStringArray(r)
	c.pass(r, err)
	if err == errNull {
		return nil, nil
	}
	return array, err
}

// Pass over the virtual read lock to the following command in line.
// If there are no routines waiting for response, then go in idle mode.
func (c *Client) pass(r *bufio.Reader, err error) {
	switch err {
	case nil, errNull:
		break
	default:
		if _, ok := err.(ServerError); !ok {
			c.dropConn()
			return
		}
	}

	// The high-traffic scenario has the optimal flow.
	select {
	case next := <-c.readQueue:
		next <- r // pass read lock
		return

	default:
		break
	}

	select {
	case next := <-c.readQueue:
		next <- r // pass read lock

	// Write is locked to make the idle decision atomic,
	// as readQueue is fed while holding the write lock.
	case conn := <-c.connSem:
		select {
		case next := <-c.readQueue:
			// lost race recovery
			next <- r // pass read lock

		default:
			// set read lock to idle
			conn.idle = r
		}
		c.connSem <- conn // unlock write

	case <-c.readInterrupt:
		// halt accepted
		break // read lock discard
	}
}

func (c *Client) dropConn() {
	for {
		select {
		case <-c.readInterrupt:
			return // accept halt

		// A write (lock owner) blocks on a full queue,
		// so include discard here to prevent deadlock.
		case next := <-c.readQueue:
			// signal connection loss
			next <- (*bufio.Reader)(nil)

		case conn := <-c.connSem:
			// write locked
			if conn.offline != nil {
				if conn.offline == ErrClosed {
					// confirm by accept
					<-c.readInterrupt
				}
				c.connSem <- conn // restore
			} else {
				// write remains locked
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

func (c *Client) haltReceive(writeLock *redisConn) {
	if writeLock.offline != nil || writeLock.idle != nil {
		// read routine not running
		return
	}
	// Read routine needs the write lock to idle.

	readHandover := make(chan *bufio.Reader)
	select {
	case c.readInterrupt <- struct{}{}:
		// The read routine accepted the halt,
		// while awaiting the write lock.
		break

	case c.readQueue <- readHandover:
		select {
		case c.readInterrupt <- struct{}{}:
			// The read routine accepted the halt,
			// while awaiting the write lock.
			break

		case <-readHandover:
			// All reads are done. We have the read lock.
			break
		}
	}
}

type connConfig struct {
	BufferSize     int
	Addr           string
	Password       []byte
	DB             int64
	CommandTimeout time.Duration
	DialTimeout    time.Duration
}

func connect(c connConfig) (net.Conn, *bufio.Reader, error) {
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
	reader := bufio.NewReaderSize(conn, c.BufferSize)

	// apply sticky settings
	if c.Password != nil {
		req := newRequest("*2\r\n$4\r\nAUTH\r\n$")
		defer req.free()
		req.addBytes(c.Password)

		if c.CommandTimeout != 0 {
			conn.SetDeadline(time.Now().Add(c.CommandTimeout))
			defer conn.SetDeadline(time.Time{})
		}
		_, err := conn.Write(req.buf)
		if err == nil {
			err = decodeOK(reader)
		}
		if err != nil {
			return nil, nil, fmt.Errorf("redis: AUTH with %w", err)
		}
	}
	if c.DB != 0 {
		req := newRequest("*2\r\n$6\r\nSELECT\r\n$")
		defer req.free()
		req.addDecimal(c.DB)

		if c.CommandTimeout != 0 {
			conn.SetDeadline(time.Now().Add(c.CommandTimeout))
			defer conn.SetDeadline(time.Time{})
		}
		_, err := conn.Write(req.buf)
		if err == nil {
			err = decodeOK(reader)
		}
		if err != nil {
			return nil, nil, fmt.Errorf("redis: SELECT with %w", err)
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
