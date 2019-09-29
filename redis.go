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
	KeyMax = 2 << 32

	// Every hash, list, set, and sorted set, can hold 2³² − 1 elements.
	ElementMax = 2<<32 - 1
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

// ErrTerminated means that the Client is no longer in use.
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

	commandTimeout, connectTimeout time.Duration

	// write lock
	connSem chan *redisConn

	// pending commands: request send, awaiting response
	queue chan *resp
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

		connSem: make(chan *redisConn, 1), // one shared instance
		queue:   make(chan *resp, queueSize),
	}

	go c.connect(nil)

	return c
}

type redisConn struct {
	*bufio.Reader
	net.Conn
	err  error // fatal connection failure or ErrTerminated
	idle bool
}

// Terminate stops command submission with ErrTerminated.
// The network connection is closed on return.
func (c *Client) Terminate() {
	// aquire write lock
	conn := <-c.connSem
	if conn.Conn != nil {
		conn.Close()
	}

	// stop command submission & read routines
	c.connSem <- &redisConn{err: ErrTerminated}
}

// Connect populates the connection semaphore.
func (c *Client) connect(previous *redisConn) {
	// cleanup
	if previous != nil {
		if previous.Conn != nil {
			previous.Close()
		}

		// flush pending reads
		for len(c.queue) != 0 {
			(<-c.queue).receive <- errConnLost
		}
	}

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
				if current.err == ErrTerminated {
					c.connSem <- current // restore
					return               // abandon
				}
			}

			// propagate connection failure
			c.connSem <- &redisConn{
				err: fmt.Errorf("redis: offline due %w", err),
			}

			<-retry.C
			continue
		}

		if !firstAttempt {
			// clear previous error; unless terminated
			current := <-c.connSem
			if current.err == ErrTerminated {
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
			Conn:   conn,
			Reader: bufio.NewReaderSize(conn, conservativeMSS),
			idle:   true,
		}
		return
	}
}

func (c *Client) send(r *resp) error {
	// operate in write lock
	conn := <-c.connSem

	// validate connection state
	if err := conn.err; err != nil {
		c.connSem <- conn // restore
		return err
	}

	// apply timeout, if any
	var deadline time.Time
	if c.commandTimeout != 0 {
		deadline = time.Now().Add(c.commandTimeout)
		conn.SetWriteDeadline(deadline)
	}

	// send command
	if _, err := conn.Write(r.buf); err != nil {
		// tries to prevent redundant error reporting
		conn.Conn.(interface{ CloseWrite() error }).CloseWrite()

		go c.connect(conn)
		return err
	}

	r.conn = conn

	direct := conn.idle
	if direct {
		conn.idle = false
	} else {
		c.queue <- r // enque response
	}

	c.connSem <- conn // release write lock

	if !direct {
		// await handover of virtual read lock
		if err := <-r.receive; err != nil {
			// queue abandonment
			return err
		}
	}

	if !deadline.IsZero() {
		r.conn.SetReadDeadline(deadline)
	}

	return nil
}

func (c *Client) commandOK(r *resp) error {
	if err := c.send(r); err != nil {
		return err
	}
	userErr, fatalErr := decodeOK(r.conn.Reader)
	if err := c.pass(fatalErr, r); err != nil {
		return err
	}
	return userErr
}

func (c *Client) commandInteger(r *resp) (int64, error) {
	if err := c.send(r); err != nil {
		return 0, err
	}
	integer, userErr, fatalErr := decodeInteger(r.conn.Reader)
	if err := c.pass(fatalErr, r); err != nil {
		return 0, err
	}
	return integer, userErr
}

func (c *Client) commandBulk(r *resp) ([]byte, error) {
	if err := c.send(r); err != nil {
		return nil, err
	}
	bulk, userErr, fatalErr := decodeBulk(r.conn.Reader)
	if err := c.pass(fatalErr, r); err != nil {
		return nil, err
	}
	return bulk, userErr
}

func (c *Client) commandArray(r *resp) ([][]byte, error) {
	if err := c.send(r); err != nil {
		return nil, err
	}
	array, userErr, fatalErr := decodeArray(r.conn.Reader)
	if err := c.pass(fatalErr, r); err != nil {
		return nil, err
	}
	return array, userErr
}

// Pass over the virtual read lock to the following command in line.
// If there are no routines waiting for response, then go in idle mode.
func (c *Client) pass(err error, r *resp) error {
	if err != nil {
		conn := <-c.connSem // write lock
		if conn.err == ErrTerminated {
			c.connSem <- conn // restore
			return ErrTerminated
		}
		if conn != r.conn {
			// new connection already in use
			c.connSem <- conn // restore
			return err
		}

		go c.connect(conn)
		return fmt.Errorf("redis: connection lost: %w", err)
	}

	select {
	case next := <-c.queue:
		// The high-traffic scenario has the optimal flow.
		next.receive <- nil

	default:
		select {
		// optimizes case when multiple routines are awaiting the lock:
		case next := <-c.queue:
			// Another routine used the write lock to enqueue a read.
			next.receive <- nil

		case conn := <-c.connSem:
			// Write is locked to make the idle decision atomic.
			if conn == r.conn {
				select {
				case next := <-c.queue:
					// recover from lost race
					next.receive <- nil

				default:
					// signals no read routine
					conn.idle = true
				}
			}
			c.connSem <- conn // restore
		}
	}

	respPool.Put(r)

	return nil
}
