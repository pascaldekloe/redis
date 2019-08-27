// Package redis provides Redis serice access.
package redis

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"
)

// Timeout limits the command duration.
// Expiry causes a reconnect, to prevent stale connections.
var Timeout = time.Second

// ConnectTimeout limits the duration for connection establishment,
// including reconnects. Once expired, commands receive the timeout
// error until the connection restores. Client methods block during
// connect.
var ConnectTimeout = time.Second

// ErrConnLost signals connection loss on pending commands.
// The execution state is unknown.
var ErrConnLost = errors.New("redis: connection lost")

// errProtocol signals invalid RESP reception.
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

func normalizeAddr(s string) string {
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

// Client provides command exectuion for a Redis service.
// Multiple goroutines may invoke methods on a Client simultaneously.
type Client struct {
	// Server location in use. This field is read-only.
	Addr string

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
func NewClient(addr string) *Client {
	c := &Client{
		Addr:     normalizeAddr(addr),
		writeSem: make(chan net.Conn, 1),
		writeErr: make(chan struct{}, 1),
		queue:    make(chan parser, 128),
		offline:  make(chan error),
	}
	go c.manage()
	return c
}

func (c *Client) manage() {
	var notify chan error
	for {
		// connect
		dialer := net.Dialer{Timeout: ConnectTimeout}
		conn, err := dialer.Dial("tcp", c.Addr)
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

		r := bufio.NewReader(conn)
		for {
			select {
			case response := <-c.queue:
				conn.SetReadDeadline(time.Now().Add(Timeout))
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
			r.Reset(errorReader{ErrConnLost})
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

type errorReader struct{ error }

func (r errorReader) Read([]byte) (int, error) {
	return 0, r.error
}

type parser interface {
	parse(*bufio.Reader) bool
}

var okParsers = sync.Pool{New: func() interface{} { return make(okParser, 1) }}
var intParsers = sync.Pool{New: func() interface{} { return make(intParser, 1) }}
var bulkParsers = sync.Pool{New: func() interface{} { return make(bulkParser, 1) }}
var arrayParsers = sync.Pool{New: func() interface{} { return make(arrayParser, 1) }}

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
		p <- fmt.Errorf("%w; unexpected first byte %q on line %q", errProtocol, first, line)
		return false
	}
}

type intResponse struct {
	Int int64
	Err error
}

func (p intParser) parse(r *bufio.Reader) bool {
	first, line, err := readCRLF(r)
	if err != nil {
		p <- intResponse{Err: err}
		return false
	}

	switch first {
	case ':':
		p <- intResponse{Int: ParseInt(line)}
		return true

	case '-':
		p <- intResponse{Err: ServerError(line)}
		return true

	default:
		p <- intResponse{Err: fmt.Errorf("%w; unexpected first byte %q on line %q", errProtocol, first, line)}
		return false
	}
}

type bulkResponse struct {
	Bytes []byte
	Err   error
}

func (p bulkParser) parse(r *bufio.Reader) bool {
	first, line, err := readCRLF(r)
	if err != nil {
		p <- bulkResponse{Err: err}
		return false
	}

	switch first {
	case '$':
		break
	case '-':
		p <- bulkResponse{Err: ServerError(line)}
		return true
	default:
		p <- bulkResponse{Err: fmt.Errorf("%w; unexpected first byte %q on line %q", errProtocol, first, line)}
		return false
	}

	size := ParseInt(line)
	if size < 0 {
		p <- bulkResponse{Bytes: nil}
		return true
	}

	bytes, err := readNCRLF(r, size)
	if err != nil {
		p <- bulkResponse{Err: err}
		return false
	}
	p <- bulkResponse{Bytes: bytes, Err: err}
	return true
}

type arrayResponse struct {
	Array [][]byte
	Err   error
}

func (p arrayParser) parse(r *bufio.Reader) bool {
	first, line, err := readCRLF(r)
	if err != nil {
		p <- arrayResponse{Err: err}
		return false
	}

	switch first {
	case '*':
		break
	case '-':
		p <- arrayResponse{Err: ServerError(line)}
		return true
	default:
		p <- arrayResponse{Err: fmt.Errorf("%w; unexpected first byte %q on line %q", errProtocol, first, line)}
		return false
	}

	size := ParseInt(line)
	if size < 0 {
		p <- arrayResponse{Array: nil}
		return true
	}
	array := make([][]byte, size)

	// parse elements
	for i := range array {
		first, line, err := readCRLF(r)
		if err != nil {
			p <- arrayResponse{Err: err}
			return false
		}

		switch first {
		case '$':
			size := ParseInt(line)
			if size < 0 {
				break // null
			}
			array[i], err = readNCRLF(r, size)
			if err != nil {
				p <- arrayResponse{Err: err}
				return false
			}
		case ':':
			// copy line slice
			array[i] = append(make([]byte, 0, len(line)), line...)
		default:
			p <- arrayResponse{Err: fmt.Errorf("%w; unexpected first byte %q on array element %d line %q", errProtocol, first, i, line)}
			return false
		}
	}

	p <- arrayResponse{Array: array}
	return true
}

// WARNING: line valid only until the next read on r.
func readCRLF(r *bufio.Reader) (first byte, line []byte, err error) {
	line, err = r.ReadSlice('\n')
	if err != nil {
		if err == bufio.ErrBufferFull {
			err = fmt.Errorf("%w; CRLF string exceeds %d bytes: %q", errProtocol, r.Size(), line)
		}
		return
	}

	end := len(line) - 2
	if end <= 0 || line[end] != '\r' {
		return 0, nil, fmt.Errorf("%w; got line %q", errProtocol, line)
	}
	return line[0], line[1:end], nil
}

func readNCRLF(r *bufio.Reader, n int64) ([]byte, error) {
	buf := make([]byte, n)
	if n == 0 {
		return buf, nil
	}
	done, err := r.Read(buf)
	for done < len(buf) && err == nil {
		var more int
		more, err = r.Read(buf[done:])
		done += more
	}
	if err == nil {
		_, err = r.Discard(2) // skip CRLF
	}
	return buf, err
}
