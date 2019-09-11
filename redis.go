// Package redis provides Redis service access. The implementation utilises a
// single network connection. Redis supports asynchronous I/O to optimize
// concurrent workflows. See <https://redis.io/topics/pipelining> for details.
// Use a separate Client when executing commands that may block, like FLUSHDB.
package redis

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"time"
)

// Fixed Settings
const (
	// IPv6 minimum MTU of 1280 bytes, minus a 40 byte IP header,
	// minus a 32 byte TCP header (with timestamps).
	conservativeMSS = 1208

	// Number of pending requests per network protocol.
	queueSizeTCP  = 128
	queueSizeUnix = 512
)

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

	timeout, connectTimeout time.Duration

	// Commands lock the semaphore to enqueue the response handler.
	writeSem chan net.Conn
	// Fatal write error submission keeps the semaphore locked.
	writeErr chan struct{}

	// Pending commands: request send, awaiting response.
	queue chan *codec

	// Receives errors when the connection is unavailable.
	offline chan error
}

// NewClient launches a managed connection to a server address.
// The host defaults to localhost, and the port defaults to 6379.
// Thus, the emtpy string defaults to "localhost:6379". Use an
// absolute file path (e.g. "/var/run/redis.sock") to use Unix
// domain sockets.
//
// Timeout limits the command duration. Expiry causes a reconnect,
// to prevent stale connections. Timeout is disabled with zero.
//
// ConnectTimeout limits the duration for connection establishment,
// including reconnects. Once expired, commands receive the timeout
// error until the connection restores. Client methods block during
// connect. Zero defaults to one second.
func NewClient(addr string, timeout, connectTimeout time.Duration) *Client {
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
		timeout:        timeout,
		connectTimeout: connectTimeout,

		writeSem: make(chan net.Conn, 1), // one shared instance
		writeErr: make(chan struct{}, 1), // may not block
		queue:    make(chan *codec, queueSize),
		offline:  make(chan error),
	}

	go c.manage()

	return c
}

func (c *Client) manage() {
	for {
		// connect
		network := "tcp"
		if isUnixAddr(c.Addr) {
			network = "unix"
		}
		conn, err := net.DialTimeout(network, c.Addr, c.connectTimeout)
		if err != nil {
			for {
				select {
				case c.offline <- err:
					continue // notified a command request
				default:
					break // no more blocked commands
				}
				break
			}
			continue
		}

		// TCP parameter tuning
		if tcp, ok := conn.(*net.TCPConn); ok {
			tcp.SetNoDelay(false)
			tcp.SetLinger(0)
		}

		// Release the command submission instance.
		c.writeSem <- conn

		r := bufio.NewReaderSize(conn, conservativeMSS)
		for {
			select {
			case codec := <-c.queue:
				if c.timeout != 0 {
					conn.SetReadDeadline(time.Now().Add(c.timeout))
				}
				ok := codec.decode(r)
				codec.received <- struct{}{}
				if ok {
					continue // command done
				}
				// fatal read error

				select {
				case <-c.writeSem:
					break // semaphore hijack
				case <-c.writeErr:
					break // error already detected
				}
			case <-c.writeErr:
				break // fatal write error
			}
			break
		}
		// The command submission is blocked now.
		// Both writeSem and writeErr are empty.

		conn.Close()

		// flush queue with errConnLost
		for len(c.queue) != 0 {
			r.Reset(connLostReader{})
			(<-c.queue).decode(r)
		}
	}
}

type connLostReader struct{}

func (r connLostReader) Read([]byte) (int, error) {
	return 0, errConnLost
}

func (c *Client) send(codec *codec) error {
	var conn net.Conn
	select {
	case conn = <-c.writeSem:
		break // lock aquired
	case err := <-c.offline:
		return err
	}

	// send command
	if c.timeout != 0 {
		conn.SetWriteDeadline(time.Now().Add(c.timeout))
	}
	if _, err := conn.Write(codec.buf); err != nil {
		// The write semaphore is not released.
		c.writeErr <- struct{}{} // does not block
		return err
	}

	// await response (in line)
	c.queue <- codec

	// release lock
	c.writeSem <- conn

	return nil
}

func (c *Client) commandOK(codec *codec) error {
	codec.resultType = okResult

	err := c.send(codec)
	if err != nil {
		codecPool.Put(codec)
		return err
	}

	<-codec.received // await response

	err = codec.result.err
	codec.result.err = nil
	codecPool.Put(codec)
	return err
}

func (c *Client) commandInteger(codec *codec) (int64, error) {
	codec.resultType = integerResult

	err := c.send(codec)
	if err != nil {
		codecPool.Put(codec)
		return 0, err
	}

	<-codec.received // await response

	integer, err := codec.result.integer, codec.result.err
	codec.result.integer, codec.result.err = 0, nil
	codecPool.Put(codec)
	return integer, err
}

func (c *Client) commandBulk(codec *codec) ([]byte, error) {
	codec.resultType = bulkResult

	err := c.send(codec)
	if err != nil {
		codecPool.Put(codec)
		return nil, err
	}

	<-codec.received // await response

	bulk, err := codec.result.bulk, codec.result.err
	codec.result.bulk, codec.result.err = nil, nil
	codecPool.Put(codec)
	return bulk, err
}

func (c *Client) commandArray(codec *codec) ([][]byte, error) {
	codec.resultType = arrayResult

	err := c.send(codec)
	if err != nil {
		codecPool.Put(codec)
		return nil, err
	}

	<-codec.received // await response

	array, err := codec.result.array, codec.result.err
	codec.result.array, codec.result.err = nil, nil
	codecPool.Put(codec)
	return array, err
}
