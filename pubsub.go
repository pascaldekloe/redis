package redis

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
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

	// Upper boundary for network connection establishment. See the
	// net.Dialer Timeout for details. Zero defaults to one second.
	DialTimeout time.Duration

	// Optional AUTH [command] value applied to the connection.
	Password []byte

	// Limits the execution time for AUTH, QUIT, SUBSCRIBE, PSUBSCRIBE,
	// UNSUBSCRIBE, PUNSUBSCRIBE and PING. The network connection is closed
	// upon expiry, which causes the automated reconnect attempts.
	// Zero defaults to one second.
	CommandTimeout time.Duration
}

func (c *ListenerConfig) clean() {
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

	// requested subscription state with their submission moment
	subs map[string]time.Time
	// pending unsubscriptions with their submission moment
	unsubs map[string]time.Time

	// shutdown signaling
	quit, closed chan struct{}
}

// NewListener launches a managed connection.
func NewListener(config ListenerConfig) *Listener {
	config.clean()

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

	// await shutdown
	<-l.closed
	return nil
}

func (l *Listener) connectLoop() {
	defer func() {
		// confirmed shutdown
		l.Func("", nil, ErrClosed)
		// Close awaits ErrClosed propagation
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
			Addr:           normalizeAddr(l.Addr),
			DialTimeout:    l.DialTimeout,
			CommandTimeout: l.CommandTimeout,
			Password:       l.Password,
		}
		conn, reader, err := config.connect(l.BufferSize)
		if err != nil {
			retry := time.NewTimer(retryDelay)

			// propagate error
			l.Func("", nil, fmt.Errorf("redis: listener offline due %w", err))

			retryDelay = 2*retryDelay + time.Millisecond
			if retryDelay > DialDelayMax {
				retryDelay = DialDelayMax
			}
			<-retry.C
			continue
		}
		// connect success
		retryDelay = 0

		if subscribed, ok := l.releaseConn(conn); ok {
			l.launchConn(conn, reader, subscribed...)

			// retract after releaseConn
			l.mutex.Lock()
			l.conn = nil
			l.mutex.Unlock()
		}
		conn.Close()
	}
}

func (l *Listener) releaseConn(conn net.Conn) (subscribed []string, ok bool) {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	select {
	case <-l.quit:
		return nil, false
	default:
		break
	}

	l.conn = conn

	// apply pendig unsubscribes
	for name := range l.unsubs {
		delete(l.unsubs, name)
		delete(l.subs, name)
	}

	// collect subscription state
	now := time.Now()
	for name := range l.subs {
		l.subs[name] = now // reset timestamp
		subscribed = append(subscribed, name)
	}

	return subscribed, true
}

var (
	errQUITTimeout        = errors.New("redis: QUIT expired by timeout")
	errSUBSCRIBETimeout   = errors.New("redis: SUBSCRIBE expired by timeout")
	errUNSUBSCRIBETimeout = errors.New("redis: UNSUBSCRIBE expired by timeout")
)

func (l *Listener) launchConn(conn net.Conn, reader *bufio.Reader, channels ...string) {
	// resubscribe if any
	if len(channels) > 0 {
		go func() {
			req := newRequestSize(1+len(channels), "\r\n$9\r\nSUBSCRIBE")
			req.addStringList(channels)
			l.submit(conn, req)
		}()
	}

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
			if !isClosed(err) {
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
	subscriptions := make(map[string]string)

	for {
		head, err := reader.Peek(16)
		if len(head) != 16 {
			return err
		}

		head1 := binary.LittleEndian.Uint64(head)
		head2 := binary.LittleEndian.Uint64(head[8:])
		switch {
		default:
			reader.Discard(16)
			return readError(reader, head, "push array")

		case head1 == '*'|'3'<<8|'\r'<<16|'\n'<<24|'$'<<32|'7'<<40|'\r'<<48|'\n'<<56 &&
			head2 == 'm'|'e'<<8|'s'<<16|'s'<<24|'a'<<32|'g'<<40|'e'<<48|'\r'<<56:
			_, err := reader.Discard(17)
			if err != nil {
				return err
			}

			line, err := readLine(reader)
			if err != nil {
				return err
			}
			if len(line) < 4 || line[0] != '$' {
				return readError(reader, line, "push message channel")
			}

			// range protected by bufio.ErrBufferFull:
			slice, err := reader.Peek(int(ParseInt(line[1 : len(line)-2])))
			if err != nil {
				return fmt.Errorf("redis: message channel got %w", err)
			}
			channel, ok := subscriptions[string(slice)] // no malloc
			if !ok {
				return fmt.Errorf("redis: message for channel %q while not subscribed", slice)
			}
			_, err = reader.Discard(len(slice) + 2) // skip CRLF
			if err != nil {
				return fmt.Errorf("redis: message channel CRLF got %w", err)
			}

			line, err = readLine(reader)
			if err != nil {
				return err
			}
			if len(line) < 4 || line[0] != '$' {
				return readError(reader, line, "push message payload length")
			}
			payloadLen := ParseInt(line[1 : len(line)-2])
			if payloadLen < 0 || payloadLen > int64(l.BufferSize) {
				if payloadLen < 0 || payloadLen > SizeMax {
					return fmt.Errorf("redis: message payload length got %.40q", line)
				}
				l.Func(channel, nil, io.ErrShortBuffer)
			} else {
				payloadSlice, err := reader.Peek(int(payloadLen))
				if err != nil {
					return err
				}
				l.Func(channel, payloadSlice, nil)
			}
			_, err = reader.Discard(int(payloadLen) + 2) // skip CRLF
			if err != nil {
				return fmt.Errorf("redis: message payload CRLF got %w", err)
			}

		case head1 == '*'|'3'<<8|'\r'<<16|'\n'<<24|'$'<<32|'9'<<40|'\r'<<48|'\n'<<56 &&
			head2 == 's'|'u'<<8|'b'<<16|'s'<<24|'c'<<32|'r'<<40|'i'<<48|'b'<<56:
			_, err := reader.Discard(19)
			if err != nil {
				return err
			}

			channel, err := decodeBlobString(reader)
			if err != nil {
				return fmt.Errorf("redis: subscribe channel got %w", err)
			}
			// subscription count is useless with concurrency
			if _, err := decodeInteger(reader); err != nil {
				return fmt.Errorf("redis: subscription count got %w", err)
			}

			l.mutex.Lock()
			// zero submission timestamp stops expiry check
			l.subs[channel] = time.Time{}
			l.mutex.Unlock()
			subscriptions[channel] = channel

		case head1 == '*'|'3'<<8|'\r'<<16|'\n'<<24|'$'<<32|'1'<<40|'1'<<48|'\r'<<56 &&
			head2 == '\n'|'u'<<8|'n'<<16|'s'<<24|'u'<<32|'b'<<40|'s'<<48|'c'<<56:
			if _, err := reader.Discard(22); err != nil {
				return err
			}

			channel, err := decodeBlobString(reader)
			if err != nil {
				return fmt.Errorf("redis: unsubscribe channel got %w", err)
			}
			// subscription count is useless with concurrency
			if _, err := decodeInteger(reader); err != nil {
				return fmt.Errorf("redis: unsubscription count got %w", err)
			}

			l.mutex.Lock()
			delete(l.subs, channel)
			delete(l.unsubs, channel)
			l.mutex.Unlock()
			delete(subscriptions, channel)
		}
	}
}

// submit either sends a request or it closes the connection.
func (l *Listener) submit(conn net.Conn, req *request) {
	defer req.free()
	conn.SetWriteDeadline(time.Now().Add(l.CommandTimeout))
	_, err := conn.Write(req.buf)
	if err != nil {
		conn.Close()
		if !isClosed(err) {
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
	now := time.Now()
	for _, name := range channels {
		if _, ok := l.subs[name]; !ok {
			if len(name) > SizeMax {
				go l.Func(name, nil, fmt.Errorf("%w; %d byte channel name %.40q…", errProtocol, len(name), name))
				continue
			}
			l.subs[name] = now
			todo = append(todo, name)
		}
	}
	conn := l.conn
	l.mutex.Unlock()

	if conn != nil && len(todo) != 0 {
		r := newRequestSize(len(todo)+1, "\r\n$9\r\nSUBSCRIBE")
		r.addStringList(todo)
		l.submit(conn, r)
	}

	return
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
	now := time.Now()
	for _, name := range channels {
		if _, ok := l.unsubs[name]; !ok {
			l.unsubs[name] = now
			todo = append(todo, name)
		}
	}
	conn := l.conn
	l.mutex.Unlock()

	if conn != nil && (len(todo) != 0 || len(channels) == 0) {
		r := newRequestSize(len(todo)+1, "\r\n$11\r\nUNSUBSCRIBE")
		r.addStringList(todo)
		l.submit(conn, r)
	}
}

// isClosed works around https://github.com/golang/go/issues/4373 🤬
func isClosed(err error) bool {
	e := new(*net.OpError)
	return errors.As(err, e) && strings.Contains((*e).Err.Error(), "use of closed network connection")
}
