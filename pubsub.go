package redis

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"
)

// PUBLISH executes <https://redis.io/commands/publish>.
func (c *Client) PUBLISH(channel string, message []byte) (clientCount int64, err error) {
	r := newRequest("*3\r\n$7\r\nPUBLISH\r\n$")
	r.addStringBytes(channel, message)
	return c.commandInteger(r)
}

// PUBLISHString executes <https://redis.io/commands/publish>.
func (c *Client) PUBLISHString(channel, message string) (clientCount int64, err error) {
	r := newRequest("*3\r\n$7\r\nPUBLISH\r\n$")
	r.addStringString(channel, message)
	return c.commandInteger(r)
}

// ListenerConfig defines a Listener setup.
type ListenerConfig struct {
	// Func is the callback interface for both push messages and error
	// events. Implementations must not retain message—make a copy if the
	// bytes are used after return. Message invocation is guaranteed to
	// match the Redis submission order. Slow or blocking receivers should
	// spawn of in a separate routine.
	Func func(channel string, message []byte, err error)

	// Upper boundary for the number of bytes in a message payload.
	// Larger messages are skipped with an io.ErrShortBuffer to Func.
	// Zero defaults to 32 KiB. Values larger than SizeMax are capped
	// to SizeMax.
	BufferSize int

	// The host defaults to localhost, and the port defaults to 6379.
	// Thus, the empty string defaults to "localhost:6379". Use an
	// absolute file path (e.g. "/var/run/redis.sock") for Unix
	// domain sockets.
	Addr string

	// Limits the execution time for AUTH, QUIT, SUBSCRIBE, PSUBSCRIBE,
	// UNSUBSCRIBE, PUNSUBSCRIBE and PING. The network connection is closed
	// upon expiry, which causes the automated reconnect attempts.
	// Zero defaults to one second.
	CommandTimeout time.Duration

	// Upper boundary for network connection establishment. See the
	// net.Dialer Timeout for details. Zero defaults to one second.
	DialTimeout time.Duration

	// AUTH when not nil.
	Password []byte
}

func (c *ListenerConfig) normalize() {
	if c.Func == nil {
		panic("redis: missing callback function")
	}
	if c.BufferSize == 0 {
		c.BufferSize = 32 * 1024
	}
	if c.BufferSize > SizeMax {
		c.BufferSize = SizeMax
	}
	c.Addr = normalizeAddr(c.Addr)
	if c.CommandTimeout == 0 {
		c.CommandTimeout = time.Second
	}
	if c.DialTimeout == 0 {
		c.DialTimeout = time.Second
	}
}

// Listener manages a connection to a Redis node until Close. Broken connection
// states cause automated reconnects, including resubscribes when applicable.
//
// Multiple goroutines may invoke methods on a Listener simultaneously.
type Listener struct {
	mutex sync.Mutex

	ListenerConfig // read-only attributes

	// current connection, which may be nil when offline
	conn net.Conn

	// Subs maps SUBSCRIBE patterns to their request timestamp.
	// The timestamp is zeroed once the server confirmed subscription.
	subs map[string]time.Time

	// Unsubs maps UNSUBSCRIBE patterns to their request timestamp.
	// Entries are removed once confirmed.
	unsubs map[string]time.Time

	// shutdown signaling
	quit, closed chan struct{}
}

// NewListener launches a managed connection.
func NewListener(config ListenerConfig) *Listener {
	config.normalize()

	l := &Listener{
		ListenerConfig: config,
		subs:           make(map[string]time.Time),
		unsubs:         make(map[string]time.Time),
		quit:           make(chan struct{}),
		closed:         make(chan struct{}),
	}

	// launch connection management
	go l.connectLoop()

	return l
}

// Close terminates the connection establishment. The Listener Func is called
// with ErrClosed before return, and after the network connection was closed.
// Calling Close more than once just blocks until the first call completed.
func (l *Listener) Close() error {
	l.mutex.Lock()
	select {
	case <-l.quit:
		break // already invoked
	default:
		close(l.quit)

	}
	l.conn = nil
	l.mutex.Unlock()

	// await completion
	<-l.closed
	return nil
}

func (l *Listener) connectLoop() {
	defer func() {
		// confirmed shutdown
		l.Func("", nil, ErrClosed)
		// Close awaits complition
		close(l.closed)
	}()

	var retryDelay time.Duration
	for {
		// l.conn is zero; check l.halt for shutdown requests
		select {
		case <-l.quit:
			return // accept shutdown
		default:
			break
		}

		config := ClientConfig{
			Addr:           l.Addr,
			CommandTimeout: l.CommandTimeout,
			DialTimeout:    l.DialTimeout,
			Password:       l.Password,
		}
		conn, reader, err := config.connect(l.BufferSize)
		if err != nil {
			retry := time.NewTimer(retryDelay)

			// propagate error
			l.Func("", nil, fmt.Errorf("redis: listener offline: %w", err))

			retryDelay = 2*retryDelay + time.Millisecond
			if retryDelay > DialDelayMax {
				retryDelay = DialDelayMax
			}
			<-retry.C
			continue
		}
		// connect success
		retryDelay = 0

		subs := l.releaseConn(conn)
		if len(subs) != 0 {
			go func(conn net.Conn) {
				req := newRequestSize(1+len(subs), "\r\n$9\r\nSUBSCRIBE")
				req.addStringList(subs)
				l.submit(conn, req)
			}(conn)
		}
		l.manageConn(conn, reader)
		l.mutex.Lock()
		l.conn = nil
		l.mutex.Unlock()
		conn.Close()
	}
}

func (l *Listener) releaseConn(conn net.Conn) (subs []string) {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	l.conn = conn

	// clear pendig unsubscribes
	for name := range l.unsubs {
		delete(l.unsubs, name)
		delete(l.subs, name)
	}

	// init subscription requests
	reqTime := time.Now()
	for name := range l.subs {
		l.subs[name] = reqTime
		subs = append(subs, name)
	}
	return
}

var (
	errQUITTimeout        = errors.New("redis: QUIT expired by timeout")
	errSUBSCRIBETimeout   = errors.New("redis: SUBSCRIBE expired by timeout")
	errUNSUBSCRIBETimeout = errors.New("redis: UNSUBSCRIBE expired by timeout")
)

func (l *Listener) manageConn(conn net.Conn, reader *bufio.Reader) {
	// read input
	errChan := make(chan error, 1)
	go func() {
		errChan <- l.readLoop(reader)
	}()

	monitorInterval := l.CommandTimeout / 4
	if monitorInterval < time.Millisecond {
		monitorInterval = time.Millisecond
	}
	monitorTicker := time.NewTicker(monitorInterval)
	defer monitorTicker.Stop()

	for {
		select {
		case <-l.quit:
			// try graceful shutdown with QUIT command
			req := newRequest("*1\r\n$4\r\nQUIT\r\n")
			l.submit(conn, req)

			// Await read routine to stop, idealy by and EOF from QUIT.
			conn.SetReadDeadline(time.Now().Add(l.CommandTimeout))
			err := <-errChan
			var e net.Error
			switch {
			case errors.Is(err, io.EOF):
				break // correct QUIT result
			case errors.As(err, &e) && e.Timeout():
				l.Func("", nil, errQUITTimeout)
			case err != nil:
				l.Func("", nil, err)
			}
			return // accept shutdown

		case err := <-errChan:
			if !errors.Is(err, net.ErrClosed) {
				l.Func("", nil, err)
			} // else terminated by write error
			return

		case t := <-monitorTicker.C:
			expire := t.Add(-l.CommandTimeout)

			l.mutex.Lock()
			for _, timestamp := range l.subs {
				if !timestamp.IsZero() && timestamp.Before(expire) {
					l.Func("", nil, errSUBSCRIBETimeout)
					l.mutex.Unlock()
					return // any error from read loop gets discarded
				}
			}
			for _, timestamp := range l.unsubs {
				if !timestamp.IsZero() && timestamp.Before(expire) {
					l.Func("", nil, errUNSUBSCRIBETimeout)
					l.mutex.Unlock()
					return // any error from read loop gets discarded
				}
			}
			l.mutex.Unlock()
		}
	}
}

func (l *Listener) readLoop(reader *bufio.Reader) error {
	// confirmed state as message channel mapping
	confirmedSubs := make(map[string]string)

	for {
		head, err := reader.Peek(16)
		if err != nil {
			return err
		}

		head1 := binary.LittleEndian.Uint64(head)
		head2 := binary.LittleEndian.Uint64(head[8:])
		switch {
		case head1 == '*'|'3'<<8|'\r'<<16|'\n'<<24|'$'<<32|'7'<<40|'\r'<<48|'\n'<<56 &&
			head2 == 'm'|'e'<<8|'s'<<16|'s'<<24|'a'<<32|'g'<<40|'e'<<48|'\r'<<56:
			err = l.onMessage(reader, confirmedSubs)
			if err != nil {
				return err
			}

		case head1 == '*'|'3'<<8|'\r'<<16|'\n'<<24|'$'<<32|'9'<<40|'\r'<<48|'\n'<<56 &&
			head2 == 's'|'u'<<8|'b'<<16|'s'<<24|'c'<<32|'r'<<40|'i'<<48|'b'<<56:
			_, err := reader.Discard(19)
			if err != nil {
				return fmt.Errorf("redis: subscribe array-reply: %w", err)
			}

			channel, err := readBulkString(reader)
			if err != nil {
				return fmt.Errorf("redis: subscribe array-reply channel: %w", err)
			}
			// subscription count is useless with concurrency
			if _, err := readInteger(reader); err != nil {
				return fmt.Errorf("redis: subscribe array-reply count: %w", err)
			}

			l.mutex.Lock()
			l.subs[channel] = time.Time{}
			l.mutex.Unlock()
			confirmedSubs[channel] = channel

		case head1 == '*'|'3'<<8|'\r'<<16|'\n'<<24|'$'<<32|'1'<<40|'1'<<48|'\r'<<56 &&
			head2 == '\n'|'u'<<8|'n'<<16|'s'<<24|'u'<<32|'b'<<40|'s'<<48|'c'<<56:
			if _, err := reader.Discard(22); err != nil {
				return fmt.Errorf("redis: unsubscribe array-reply: %w", err)
			}

			channel, err := readBulkString(reader)
			if err != nil {
				return fmt.Errorf("redis: unsubscribe array-reply channel: %w", err)
			}
			// subscription count is useless with concurrency
			if _, err := readInteger(reader); err != nil {
				return fmt.Errorf("redis: unsubscribe array-reply count: %w", err)
			}

			l.mutex.Lock()
			delete(l.subs, channel)
			delete(l.unsubs, channel)
			l.mutex.Unlock()
			delete(confirmedSubs, channel)

		case head[0] == '-':
			line, err := reader.ReadString('\n')
			if err != nil {
				return fmt.Errorf("redis: server error %q: %w", line, err)
			}
			if len(line) < 3 || line[len(line)-2] != '\r' {
				return fmt.Errorf("%w; error %.40q without CRLF", errProtocol, line)
			}
			l.Func("", nil, ServerError(line[1:len(line)-2]))

		default:
			return fmt.Errorf("%w; received %q", errProtocol, head)
		}
	}
}

func (l *Listener) onMessage(r *bufio.Reader, confirmedSubs map[string]string) error {
	_, err := r.Discard(17)
	if err != nil {
		return fmt.Errorf("redis: message array-reply: %w", err)
	}

	// parse channel
	line, err := readLine(r)
	if err != nil {
		return fmt.Errorf("redis: message array-reply channel-size: %w", err)
	}
	if len(line) < 4 || line[0] != '$' {
		return fmt.Errorf("redis: message array-reply channel-size %.40q", line)
	}
	channelSize := ParseInt(line[1 : len(line)-2])
	if channelSize < 0 || channelSize > SizeMax {
		return fmt.Errorf("redis: message array-reply channel-size %.40q", line)
	}
	channelSlice, err := r.Peek(int(channelSize))
	if err != nil {
		return fmt.Errorf("redis: message array-reply channel: %w", err)
	}
	channel, ok := confirmedSubs[string(channelSlice)] // no malloc
	if !ok {
		// fishy, yet it could happen with engines like DragonflyDB
		channel = string(channelSlice) // malloc
	}
	_, err = r.Discard(len(channelSlice) + 2) // skip CRLF
	if err != nil {
		return fmt.Errorf("redis: message array-reply channel-CRLF: %w", err)
	}

	// parse payload
	line, err = readLine(r)
	if err != nil {
		return err
	}
	if len(line) < 4 || line[0] != '$' {
		return fmt.Errorf("redis: message array-reply payload-size %.40q", line)
	}
	payloadSize := ParseInt(line[1 : len(line)-2])
	if payloadSize < 0 || payloadSize > SizeMax {
		return fmt.Errorf("redis: message array-reply payload-size %.40q", line)
	}
	if payloadSize > int64(l.BufferSize) {
		l.Func(channel, nil, io.ErrShortBuffer)
	} else {
		payloadSlice, err := r.Peek(int(payloadSize))
		if err != nil {
			return fmt.Errorf("redis: message array-reply payload: %w", err)
		}
		l.Func(channel, payloadSlice, nil)
	}
	_, err = r.Discard(int(payloadSize) + 2) // skip CRLF
	if err != nil {
		return fmt.Errorf("redis: message array-reply payload-CRLF: %w", err)
	}

	return nil
}

// submit either sends a request or it closes the connection.
func (l *Listener) submit(conn net.Conn, req *request) {
	defer req.free()
	conn.SetWriteDeadline(time.Now().Add(l.CommandTimeout))
	_, err := conn.Write(req.buf)
	if err != nil {
		conn.Close()
		if !errors.Is(err, net.ErrClosed) {
			l.Func("", nil, err)
		}
	}
}

// SUBSCRIBE executes <https://redis.io/commands/subscribe> in a persistent way.
// Subscription confirmation is subject to the CommandTimeout configuration.
// If a Listener encounters an error, including a time-out, then its network
// connection will reset with automated attempts to reach the requested state.
// Invocation with zero arguments has no effect.
func (l *Listener) SUBSCRIBE(channels ...string) {
	var todo []string
	l.mutex.Lock()
	reqTime := time.Now()
	for _, s := range channels {
		if len(s) > SizeMax {
			go l.Func(s, nil, fmt.Errorf("%d-byte subscribe channel dropped", len(s)))
			continue
		}
		if _, ok := l.subs[s]; ok {
			continue // redundant
		}
		l.subs[s] = reqTime
		todo = append(todo, s)
	}
	conn := l.conn
	l.mutex.Unlock()

	if conn != nil && len(todo) != 0 {
		r := newRequestSize(len(todo)+1, "\r\n$9\r\nSUBSCRIBE")
		r.addStringList(todo)
		l.submit(conn, r)
	}
}

// UNSUBSCRIBE executes <https://redis.io/commands/unsubscribe> in a persistent
// way. Unsubscription confirmation is subject to the CommandTimeout
// configuration. If a Listener encounters an error, including a time-out,
// then its network connection will reset, causing the unsubscribe with
// immediate effect. Invocation with zero arguments is not covered by the error
// recovery due to limitations in the protocol.
func (l *Listener) UNSUBSCRIBE(channels ...string) {
	var todo []string

	l.mutex.Lock()
	reqTime := time.Now()
	for _, s := range channels {
		if len(s) > SizeMax {
			go l.Func(s, nil, fmt.Errorf("%d-byte unsubscribe channel dropped", len(s)))
			continue
		}
		if _, ok := l.unsubs[s]; ok {
			continue // redundant
		}
		l.unsubs[s] = reqTime
		todo = append(todo, s)
	}
	conn := l.conn
	l.mutex.Unlock()

	if conn != nil && (len(todo) != 0 || len(channels) == 0) {
		r := newRequestSize(len(todo)+1, "\r\n$11\r\nUNSUBSCRIBE")
		r.addStringList(todo)
		l.submit(conn, r)
	}
}
