package redis

import (
	"bufio"
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
	// Zero defaults to 64 KiB. Values larger than SizeMax have no
	// effect.
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

// Listener manages a connection to a Redis node until Close. Broken connection
// states cause automated reconnects, including resubscribes when applicable.
//
// Multiple goroutines may invoke methods on a Listener simultaneously.
type Listener struct {
	sync.Mutex

	ListenerConfig // read-only attributes

	// current connection, which may be nil when offline
	conn net.Conn

	// requested subscription state with their submission moment
	subs map[string]time.Time
	// pending unsubscriptions with their submission moment
	unsubs map[string]time.Time
	// shutdown request flag with the submission moment
	halt time.Time
	// shutdown completion
	closed chan struct{}
}

// NewListener launches a managed connection.
func NewListener(config ListenerConfig) *Listener {
	l := &Listener{
		ListenerConfig: config,
		subs:           make(map[string]time.Time),
		unsubs:         make(map[string]time.Time),
		closed:         make(chan struct{}),
	}
	// apply configuration defaults
	if l.BufferSize == 0 {
		l.BufferSize = 1 << 16
	}
	if l.CommandTimeout == 0 {
		l.CommandTimeout = time.Second
	}
	if l.DialTimeout == 0 {
		l.DialTimeout = time.Second
	}

	// launch connection management
	go l.connectLoop()

	return l
}

// Close terminates the connection establishment. The Listener Func is called
// with ErrClosed before return, and after the network connection was closed.
// Calling Close more than once just blocks until the first call completed.
func (l *Listener) Close() error {
	l.Lock()
	if l.halt.IsZero() {
		l.halt = time.Now()
		// monitorExpiry closes the net.Conn after CommandTimeout
	}
	conn := l.conn
	l.conn = nil
	l.Unlock()

	if conn != nil {
		// try graceful shutdown with QUIT command
		req := newRequest("*1\r\n$4\r\nQUIT\r\n")
		l.submit(conn, req)
		// readLoop stops after OK response and
		// connectLoop aborts on non-zero halt
	}

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
		l.Lock()
		halt := l.halt
		l.Unlock()
		if !halt.IsZero() {
			return // accept shutdown
		}

		conn, reader, err := connect(connConfig{
			BufferSize:     l.BufferSize,
			Addr:           normalizeAddr(l.Addr),
			DialTimeout:    l.DialTimeout,
			CommandTimeout: l.CommandTimeout,
			Password:       l.Password,
		})
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
			if len(subscribed) > 0 {
				// resubscribe
				r := newRequestSize(1+len(subscribed), "\r\n$9\r\nSUBSCRIBE")
				r.addStringList(subscribed)
				l.submit(conn, r)
			}

			cancel := make(chan struct{})
			go l.monitorExpiry(conn, cancel)
			l.readLoop(reader)
			close(cancel)

			// retract after releaseConn
			l.Lock()
			l.conn = nil
			l.Unlock()
		}
		conn.Close()
	}
}

func (l *Listener) releaseConn(conn net.Conn) (subscribed []string, ok bool) {
	l.Lock()
	defer l.Unlock()

	if !l.halt.IsZero() {
		return nil, false
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

var errPushArrayEmpty = errors.New("redis: got push array with 0 elements")

func (l *Listener) readLoop(reader *bufio.Reader) error {
	// confirmed state as message channel mapping
	subscriptions := make(map[string]string)

	for {
		// receive push array
		elementCount, err := readArrayLen(reader)
		switch {
		case err == nil:
			break
		case errors.Is(err, errOK):
			continue // assume QUIT response
		case errors.Is(err, io.EOF) || isClosed(err):
			return nil
		default:
			return fmt.Errorf("redis: push array got %w", err)
		}
		if elementCount == 0 {
			l.Func("", nil, errPushArrayEmpty)
			continue
		}

		kindLen, err := readBlobLen(reader)
		if err != nil {
			return fmt.Errorf("redis: push kind length got %w", err)
		}
		// skip actual label; length is enough
		if _, err := reader.Discard(kindLen + 2); err != nil {
			return fmt.Errorf("redis: push kind string got %w", err)
		}
		switch {
		default:
			return fmt.Errorf("redis: push array with %d elements and %d B kind label", elementCount, kindLen)

		case kindLen == len("message") && elementCount == 3:
			channel, err := decodeBlobToken(reader, subscriptions)
			switch err {
			case nil:
				break
			case errTokenDict:
				return fmt.Errorf("redis: message for channel %q while not subscribed", channel)
			default:
				return fmt.Errorf("redis: message channel got %w", err)
			}

			payloadLen, err := readBlobLen(reader)
			if err != nil {
				return fmt.Errorf("redis: message payload length got %w", err)
			}
			payloadSlice, err := reader.Peek(int(payloadLen))
			switch err {
			case nil:
				l.Func(channel, payloadSlice, nil)
			case bufio.ErrBufferFull:
				l.Func(channel, nil, io.ErrShortBuffer)
			default:
				return fmt.Errorf("redis: message payload got %w", err)
			}
			if _, err := reader.Discard(int(payloadLen) + 2); err != nil {
				return fmt.Errorf("redis: message payload got %w", err)
			}

		case kindLen == len("subscribe") && elementCount == 3:
			channel, err := decodeBlobString(reader)
			if err != nil {
				return fmt.Errorf("redis: subscribe channel got %w", err)
			}

			// subscription count is useless with concurrency
			if _, err := decodeInteger(reader); err != nil {
				return fmt.Errorf("redis: subscription count got %w", err)
			}

			l.Lock()
			// zero submission timestamp stops expiry check
			l.subs[channel] = time.Time{}
			l.Unlock()
			subscriptions[channel] = channel

		case kindLen == len("unsubscribe") && elementCount == 3:
			// skip actual kind label
			if _, err := reader.Discard(13); err != nil {
				return fmt.Errorf("redis: unsubscribe kind got %w", err)
			}

			channel, err := decodeBlobToken(reader, subscriptions)
			if err != nil && err != errTokenDict {
				return fmt.Errorf("redis: unsubscribe channel got %w", err)
			}

			// subscription count is useless with concurrency
			if _, err := decodeInteger(reader); err != nil {
				return fmt.Errorf("redis: subscription count got %w", err)
			}

			l.Lock()
			delete(l.subs, channel)
			delete(l.unsubs, channel)
			l.Unlock()
			delete(subscriptions, channel)
		}
	}
}

var (
	errQUITTimeout        = errors.New("redis: QUIT expired by timeout")
	errSUBSCRIBETimeout   = errors.New("redis: SUBSCRIBE expired by timeout")
	errUNSUBSCRIBETimeout = errors.New("redis: UNSUBSCRIBE expired by timeout")
)

func (l *Listener) monitorExpiry(conn net.Conn, cancel <-chan struct{}) {
	interval := l.CommandTimeout / 4
	if interval < time.Millisecond {
		interval = time.Millisecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-cancel:
			return

		case t := <-ticker.C:
			expire := t.Add(-l.CommandTimeout)

			var timeout bool
			l.Lock()
			if !l.halt.IsZero() && l.halt.Before(expire) {
				l.Func("", nil, errQUITTimeout)
				timeout = true
			}
			for _, timestamp := range l.subs {
				if !timestamp.IsZero() && timestamp.Before(expire) {
					l.Func("", nil, errSUBSCRIBETimeout)
					timeout = true
				}
			}
			for _, timestamp := range l.unsubs {
				if !timestamp.IsZero() && timestamp.Before(expire) {
					l.Func("", nil, errUNSUBSCRIBETimeout)
					timeout = true
				}
			}
			l.Unlock()

			if timeout {
				conn.Close()
				return
			}
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

	l.Lock()
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
	l.Unlock()

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

	l.Lock()
	now := time.Now()
	for _, name := range channels {
		if _, ok := l.unsubs[name]; !ok {
			l.unsubs[name] = now
			todo = append(todo, name)
		}
	}
	conn := l.conn
	l.Unlock()

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
