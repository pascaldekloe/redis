// Package redis provides Redis service access. The implementation utilises a
// single network connection. Client applies asynchronous I/O to optimize
// concurrent workflows. See <https://redis.io/topics/pipelining> for details.
package redis

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"time"
)

// Server Limits
const (
	// A string value can be at most 512 MiB in length.
	SizeMax = 512 << 20

	// Redis can handle up to 2³² keys.
	KeyMax = 1 << 32

	// Every hash, list, set, and sorted set, can hold 2³² − 1 elements.
	ElementMax = 1<<32 - 1
)

// Fixed Settings
const (
	// IPv6 minimum MTU of 1280 bytes, minus a 40 byte IP header,
	// minus a 32 byte TCP header (with timestamps).
	conservativeMSS = 1208

	// Number of pending requests per network protocol.
	queueSizeTCP  = 128
	queueSizeUnix = 512

	reconnectDelay = 500 * time.Microsecond
)

// ErrTerminated rejects execution after Client.Terminate.
var ErrTerminated = errors.New("redis: client terminated")

// ErrConnLost signals connection loss to response queue.
var errConnLost = errors.New("redis: connection lost while awaiting response")

// ErrProtocol signals invalid RESP reception.
var errProtocol = errors.New("redis: protocol violation")

// ErrNull represents the null response.
var errNull = errors.New("redis: null")

// ServerError is a message send by the server.
type ServerError string

// Error honors the error interface.
func (e ServerError) Error() string {
	return fmt.Sprintf("redis: server error %q", string(e))
}

// Prefix returns the first word, which represents the error kind.
func (e ServerError) Prefix() string {
	s := string(e)
	for i, r := range s {
		if r == ' ' {
			return s[:i]
		}
	}
	return s
}

// ParseInt assumes a valid decimal string—no validation.
// The empty string returns zero.
func ParseInt(bytes []byte) int64 {
	if len(bytes) == 0 {
		return 0
	}
	u := uint64(bytes[0])

	neg := false
	if u == '-' {
		neg = true
		u = 0
	} else {
		u -= '0'
	}

	for i := 1; i < len(bytes); i++ {
		u = u*10 + uint64(bytes[i]-'0')
	}

	value := int64(u)
	if neg {
		value = -value
	}
	return value
}

func isUnixAddr(s string) bool {
	return len(s) != 0 && s[0] == '/'
}

func normalizeAddr(s string) string {
	if isUnixAddr(s) {
		return filepath.Clean(s)
	}

	host, port, err := net.SplitHostPort(s)
	if err != nil {
		host = s
	}
	if host == "" {
		host = "localhost"
	}
	if port == "" {
		port = "6379"
	}
	return net.JoinHostPort(host, port)
}

// Client provides command execution for a Redis service.
// Multiple goroutines may invoke methods on a Client simultaneously.
type Client struct {
	// Normalized server address in use. This field is read-only.
	Addr string

	// network establishment expiry
	connectTimeout time.Duration

	// optional execution expiry
	commandTimeout time.Duration

	// The connection semaphore is used as a write lock.
	connSem chan *redisConn

	// The buffering reader from redisConn is used as a read lock.
	// Command submission holds the write lock [connSem] when sending
	// to the response queue.
	readQueue chan chan<- *bufio.Reader

	// The read routine stops on receive: no more readQueue receives
	// nor network use.
	readInterrupt chan struct{}
}

// NewClient launches a managed connection to a server address.
// The host defaults to localhost, and the port defaults to 6379.
// Thus, the emtpy string defaults to "localhost:6379". Use an
// absolute file path (e.g. "/var/run/redis.sock") to use Unix
// domain sockets.
//
// A command timeout limits the execution duration when nonzero. Expiry causes a
// reconnect (to prevent stale connections) and a net.Error with Timeout() true.
// The connect timeout limits the duration for connection establishment. Command
// submission blocks on the first attempt. A zero connectTimeout defaults to one
// second. When connection establishment fails, then command submission receives
// the error of the last attempt, until the connection restores.
func NewClient(addr string, commandTimeout, connectTimeout time.Duration) *Client {
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
	offline  error // reason for connection absense

	// The token is nil when a read routine is using it.
	idle *bufio.Reader
}

// Terminate stops command submission with ErrTerminated.
// The network connection is closed after all pending commands are dealt with.
func (c *Client) Terminate() {
	conn := <-c.connSem
	if conn.offline == ErrTerminated {
		// redundant invokation
		c.connSem <- conn // restore
		return
	}

	// stop command submission
	c.connSem <- &redisConn{offline: ErrTerminated}

	c.haltReceive(conn)
	c.cancelQueue()

	if conn.Conn != nil {
		conn.Close()
	}
}

// Connect populates the connection semaphore.
func (c *Client) connect() {
	network := "tcp"
	if isUnixAddr(c.Addr) {
		network = "unix"
	}

	for firstAttempt := true; ; firstAttempt = false {
		conn, err := net.DialTimeout(network, c.Addr, c.connectTimeout)
		if err != nil {
			// closed loop protection:
			retry := time.NewTimer(reconnectDelay)

			if !firstAttempt {
				// remove previous error; unless terminated
				current := <-c.connSem
				if current.offline == ErrTerminated {
					c.connSem <- current // restore
					return               // abandon
				}
			}

			// propagate connection failure
			c.connSem <- &redisConn{
				offline: fmt.Errorf("redis: offline due %w", err),
			}

			<-retry.C
			continue
		}

		if !firstAttempt {
			// clear previous error; unless terminated
			current := <-c.connSem
			if current.offline == ErrTerminated {
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

		// apply
		c.connSem <- &redisConn{
			Conn: conn,
			idle: bufio.NewReaderSize(conn, conservativeMSS),
		}
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
	userErr, err := decodeOK(r)
	if err != nil {
		c.onReceiveError()
		return err
	}
	c.pass(r)
	return userErr
}

func (c *Client) commandInteger(req *request) (int64, error) {
	r, err := c.submit(req)
	if err != nil {
		return 0, err
	}
	integer, userErr, err := decodeInteger(r)
	if err != nil {
		c.onReceiveError()
		return 0, err
	}
	c.pass(r)
	return integer, userErr
}

func (c *Client) commandBulk(req *request) ([]byte, error) {
	r, err := c.submit(req)
	if err != nil {
		return nil, err
	}
	bulk, userErr, err := decodeBulk(r)
	if err != nil {
		c.onReceiveError()
		return nil, err
	}
	c.pass(r)
	return bulk, userErr
}

func (c *Client) commandArray(req *request) ([][]byte, error) {
	r, err := c.submit(req)
	if err != nil {
		return nil, err
	}
	array, userErr, err := decodeArray(r)
	if err != nil {
		c.onReceiveError()
		return nil, err
	}
	c.pass(r)
	return array, userErr
}

// Pass over the virtual read lock to the following command in line.
// If there are no routines waiting for response, then go in idle mode.
func (c *Client) pass(r *bufio.Reader) {
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
				if conn.offline == ErrTerminated {
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
