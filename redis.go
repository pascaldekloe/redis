// Package redis provides Redis service access. All communication is fully
// asynchronous. See https://redis.io/topics/pipelining for details.
package redis

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"sync"
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
	queue chan parser

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

		writeSem: make(chan net.Conn, 1),
		writeErr: make(chan struct{}, 1),
		queue:    make(chan parser, queueSize),
		offline:  make(chan error),
	}
	go c.manage()
	return c
}

func (c *Client) manage() {
	var notify chan error
	for {
		// connect
		network := "tcp"
		if isUnixAddr(c.Addr) {
			network = "unix"
		}
		dialer := net.Dialer{Timeout: c.connectTimeout}
		conn, err := dialer.Dial(network, c.Addr)
		if err != nil {
			if notify == nil {
				notify = make(chan error)
				go c.notifyOffline(notify)
			}
			notify <- err
			continue
		}
		if notify != nil {
			close(notify)
			notify = nil
		}

		// release command submission
		c.writeSem <- conn

		r := bufio.NewReaderSize(conn, conservativeMSS)
		for {
			select {
			case response := <-c.queue:
				if c.timeout != 0 {
					conn.SetReadDeadline(time.Now().Add(c.timeout))
				}
				if response.parse(r) {
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
				break
			}
			break
		}
		// command submission blocked

		for len(c.queue) != 0 {
			r.Reset(connLostReader{})
			(<-c.queue).parse(r)
		}
	}
}

func (c *Client) notifyOffline(ch chan error) {
	err := <-ch

	for {
		select {
		case c.offline <- err:
			continue // informed a request

		case err = <-ch:
			if err == nil {
				return
			}
			// error change/update
		}
	}
}

type connLostReader struct{}

func (r connLostReader) Read([]byte) (int, error) {
	return 0, errConnLost
}

type parser interface {
	parse(*bufio.Reader) bool
}

var okParsers = sync.Pool{New: func() interface{} { return make(okParser) }}
var intParsers = sync.Pool{New: func() interface{} { return make(intParser) }}
var bulkParsers = sync.Pool{New: func() interface{} { return make(bulkParser) }}
var arrayParsers = sync.Pool{New: func() interface{} { return make(arrayParser) }}

type okParser chan error
type intParser chan intResponse
type bulkParser chan bulkResponse
type arrayParser chan arrayResponse

func (p okParser) parse(r *bufio.Reader) bool {
	first, line, err := readCRLF(r)
	switch {
	case err != nil:
		p <- err
		return false
	case first == '+' && len(line) == 2 && line[0] == 'O' && line[1] == 'K':
		p <- nil
		return true
	case first == '$' && len(line) == 2 && line[0] == '-' && line[1] == '1':
		p <- errNull
		return true
	case first == '-':
		p <- ServerError(line)
		return true
	default:
		p <- fmt.Errorf("%w; unexpected line %.40q", errProtocol, append([]byte{first}, line...))
		return false
	}
}

type intResponse struct {
	Int int64
	Err error
}

func (p intParser) parse(r *bufio.Reader) bool {
	first, line, err := readCRLF(r)
	switch {
	case err != nil:
		p <- intResponse{Err: err}
		return false
	case first == ':':
		p <- intResponse{Int: ParseInt(line)}
		return true
	case first == '-':
		p <- intResponse{Err: ServerError(line)}
		return true
	default:
		p <- intResponse{Err: firstByteError(first, line)}
		return false
	}
}

type bulkResponse struct {
	Bytes []byte
	Err   error
}

func (p bulkParser) parse(r *bufio.Reader) bool {
	first, line, err := readCRLF(r)
	switch {
	case err != nil:
		p <- bulkResponse{Err: err}
		return false
	case first == '$':
		bytes, err := readBulk(r, line)
		p <- bulkResponse{Bytes: bytes, Err: err}
		return err == nil
	case first == '-':
		p <- bulkResponse{Err: ServerError(line)}
		return true
	default:
		p <- bulkResponse{Err: firstByteError(first, line)}
		return false
	}
}

type arrayResponse struct {
	Array [][]byte
	Err   error
}

func (p arrayParser) parse(r *bufio.Reader) bool {
	first, line, err := readCRLF(r)
	switch {
	case err != nil:
		p <- arrayResponse{Err: err}
		return false
	case first == '*':
		break
	case first == '-':
		p <- arrayResponse{Err: ServerError(line)}
		return true
	default:
		p <- arrayResponse{Err: firstByteError(first, line)}
		return false
	}

	var array [][]byte
	// negative means null–zero must be non-nil
	if size := ParseInt(line); size >= 0 {
		array = make([][]byte, size)
	}

	// parse elements
	for i := range array {
		first, line, err := readCRLF(r)
		switch {
		case err != nil:
			p <- arrayResponse{Err: err}
			return false
		case first == '$':
			array[i], err = readBulk(r, line)
			if err != nil {
				p <- arrayResponse{Err: err}
				return false
			}
		default:
			p <- arrayResponse{Err: firstByteError(first, line)}
			return false
		}
	}

	p <- arrayResponse{Array: array}
	return true
}

func firstByteError(first byte, line []byte) error {
	return fmt.Errorf("%w; unexpected first byte %#x in line %.40q", errProtocol, first, append([]byte{first}, line...))
}

// WARNING: line only valid until the next read on r.
func readCRLF(r *bufio.Reader) (first byte, line []byte, err error) {
	line, err = r.ReadSlice('\n')
	if err != nil {
		if err == bufio.ErrBufferFull {
			err = fmt.Errorf("%w; CRLF line exceeds %d bytes: %.40q…", errProtocol, r.Size(), line)
		}
		return 0, nil, err
	}

	end := len(line) - 2
	if end <= 0 || line[end] != '\r' {
		return 0, nil, fmt.Errorf("%w; CRLF empty or preceded by LF %q", errProtocol, line)
	}
	return line[0], line[1:end], nil
}

// ReadBulk continues with line after a '$' was read.
func readBulk(r *bufio.Reader, line []byte) ([]byte, error) {
	size := ParseInt(line)
	if size < 0 {
		return nil, nil
	}

	buf := make([]byte, size)
	if size != 0 {
		done, err := r.Read(buf)
		for done < len(buf) && err == nil {
			var more int
			more, err = r.Read(buf[done:])
			done += more
		}
		if err != nil {
			return nil, err
		}
	}

	_, err := r.Discard(2) // skip CRLF
	return buf, err
}
