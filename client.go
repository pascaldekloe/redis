package redis

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"sync/atomic"
	"time"
)

// Fixed Settings
const (
	// Number of pending requests limit per network protocol.
	queueSizeTCP  = 128
	queueSizeUnix = 512
)

// ErrClosed rejects command execution after Client.Close.
var ErrClosed = errors.New("redis: client closed")

// ErrConnLost signals connection loss to response queue.
var errConnLost = errors.New("redis: connection lost while awaiting response")

// Client manages a connection to a Redis node until Close. Broken connection
// states cause automated reconnects.
//
// Multiple goroutines may invoke methods on a Client simultaneously. Command
// invocation applies <https://redis.io/topics/pipelining> on concurrency.
type Client struct {
	// Normalized service address in use. This field is read-only.
	Addr string

	// password for Redis auth
	password string

	// database SELECT
	db int64

	// network establishment expiry
	connectTimeout time.Duration

	// optional execution expiry
	commandTimeout time.Duration

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

// NewClient launches a managed connection to a service address.
// The host defaults to localhost, and the port defaults to 6379.
// Thus, the empty string defaults to "localhost:6379". Use an
// absolute file path (e.g. "/var/run/redis.sock") for Unix
// domain sockets.
//
// A command timeout limits the execution duration when nonzero. Expiry causes a
// reconnect (to prevent stale connections) and a net.Error with Timeout() true.
// The connect timeout limits the duration for connection establishment. Command
// submission blocks on the first attempt. When connection establishment fails,
// then command submissions receive the error of the last attempt, until the
// connection restores. A zero connectTimeout defaults to one second.
func NewClient(addr string, commandTimeout, connectTimeout time.Duration, password ...string) *Client {
	return NewClientWithAuth(addr, commandTimeout, connectTimeout, "")
}

// NewClientWithAuth launches a managed connection to a service
// address just like NewClient but also uses password authentication.
func NewClientWithAuth(addr string, commandTimeout, connectTimeout time.Duration, password string) *Client {
	addr = normalizeAddr(addr)
	if connectTimeout == 0 {
		connectTimeout = time.Second
	}
	queueSize := queueSizeTCP
	if isUnixAddr(addr) {
		queueSize = queueSizeUnix
	}

	c := &Client{
		Addr:           addr,
		password:       password,
		commandTimeout: commandTimeout,
		connectTimeout: connectTimeout,

		connSem:       make(chan *redisConn, 1),
		readQueue:     make(chan chan<- *bufio.Reader, queueSize),
		readInterrupt: make(chan struct{}),
	}

	go c.connect()

	return c
}

type redisConn struct {
	net.Conn       // nil when offline
	offline  error // reason for connection absence

	// The token is nil when a read routine is using it.
	idle *bufio.Reader
}

// Close stops command submission with ErrClosed.
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

// Connect populates the connection semaphore.
func (c *Client) connect() {
	network := "tcp"
	if isUnixAddr(c.Addr) {
		network = "unix"
	}

	var reconnectDelay time.Duration
	for {
		conn, err := net.DialTimeout(network, c.Addr, c.connectTimeout)
		if err != nil {
			// closed loop protection:
			retry := time.NewTimer(reconnectDelay)

			// remove previous connect error unless closed
			if reconnectDelay != 0 {
				current := <-c.connSem
				if current.offline == ErrClosed {
					c.connSem <- current // restore
					retry.Stop()         // cleanup
					return               // abandon
				}
			}
			// propagate connection failure
			c.connSem <- &redisConn{
				offline: fmt.Errorf("redis: offline due %w", err),
			}

			// increase retry delay
			if reconnectDelay < 512*time.Millisecond {
				reconnectDelay = 2*reconnectDelay + time.Millisecond
			}
			<-retry.C
			continue
		}

		// remove any connect error unless closed
		if reconnectDelay != 0 {
			reconnectDelay = 0

			current := <-c.connSem
			if current.offline == ErrClosed {
				c.connSem <- current // restore
				conn.Close()         // discard
				return               // abandon
			}
		}

		// connection tuning
		if tcp, ok := conn.(*net.TCPConn); ok {
			tcp.SetNoDelay(false)
			tcp.SetLinger(0)
		}
		reader := bufio.NewReaderSize(conn, conservativeMSS)

		// attempt to auth if applicable
		if err := authenticate(c.password, conn, reader); err != nil {
			c.connSem <- &redisConn{
				offline: fmt.Errorf("redis: failed to auth: %w", err),
			}
			reconnectDelay = 2*reconnectDelay + time.Millisecond
			continue
		}

		// apply DB selection
		if db := atomic.LoadInt64(&c.db); db != 0 {
			req := newRequest("*2\r\n$6\r\nSELECT\r\n$")
			req.addDecimal(db)
			if c.commandTimeout != 0 {
				conn.SetDeadline(time.Now().Add(c.commandTimeout))
			}
			if _, err := conn.Write(req.buf); err != nil {
				c.connSem <- &redisConn{
					offline: fmt.Errorf("redis: offline due %w", err),
				}
				reconnectDelay = time.Millisecond
				continue
			}
			if err = decodeOK(reader); err != nil {
				c.connSem <- &redisConn{
					offline: fmt.Errorf("redis: offline due SELECT; %w", err),
				}
				reconnectDelay = time.Millisecond
				continue
			}
		}

		// release
		c.connSem <- &redisConn{Conn: conn, idle: reader}
		return
	}
}

// authenticate passes AUTH to redis if c.password is set.
func authenticate(password string, conn net.Conn, reader *bufio.Reader) error {
	if len(password) == 0 {
		return nil
	}
	req := newRequest(fmt.Sprintf("*2\r\n$4\r\nAUTH\r\n$%v\r\n%s\r\n", len(password), password))
	conn.SetDeadline(time.Now().Add(time.Second))
	if _, err := conn.Write(req.buf); err != nil {
		return err
	}
	if err := decodeOK(reader); err != nil {
		return err
	}
	return nil
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

	// apply timeout if set
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
			c.connect()
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

func (c *Client) commandRequireOK(req *request) error {
	r, err := c.submit(req)
	if err != nil {
		return err
	}
	err = decodeOK(r)
	if err != nil {
		c.onReceiveError()
	} else {
		c.pass(r, nil)
	}
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
			c.onReceiveError()
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

func (c *Client) onReceiveError() {
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
					c.connect()
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
