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
func (c *Client[Key, Value]) PUBLISH(channel Key, message Value) (clientCount int64, err error) {
	return c.commandInteger(requestWith2Strings("*3\r\n$7\r\nPUBLISH\r\n$", channel, message))
}

// PUBLISHString executes <https://redis.io/commands/publish>.
func (c *Client[Key, Value]) PUBLISHString(channel Key, message Value) (clientCount int64, err error) {
	return c.commandInteger(requestWith2Strings("*3\r\n$7\r\nPUBLISH\r\n$", channel, message))
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

	// Limit execution duration of AUTH, QUIT, SUBSCRIBE & UNSUBSCRIBE.
	// Expiry causes a reconnect to prevent stale connections.
	// Zero defaults to one second.
	CommandTimeout time.Duration

	// Limit the duration for network connection establishment. Expiry
	// causes an abort plus retry. See net.Dialer Timeout for details.
	// Zero defaults to one second.
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

	// Interval for command expiry check.
	expireTimer *time.Timer

	// shutdown signaling
	quited time.Time
	closed chan struct{}
}

// NewListener launches a managed connection.
func NewListener(config ListenerConfig) *Listener {
	config.normalize()

	l := &Listener{
		ListenerConfig: config,
		subs:           make(map[string]time.Time),
		unsubs:         make(map[string]time.Time),
		closed:         make(chan struct{}),
	}

	// launch connection management
	go l.connectLoop()

	return l
}

// Expire must be called with a new l.expireTimer only. The timer will be used
// to terminate the connection on l.CommandTimeout. Evaluation continues until
// all pending commands completed. Once done, l.expireTimer is set back to nil.
func (l *Listener) expire(timer *time.Timer) {
	for {
		var notBefore time.Time
		select {
		case <-l.closed:
			timer.Stop()
			return
		case now := <-timer.C:
			notBefore = now.Add(-l.CommandTimeout)
		}

		// evaluate expiry in lock
		l.mutex.Lock()
		conn := l.conn
		if conn == nil {
			// clear to exit
			l.expireTimer = nil
			l.mutex.Unlock()
			return
		}
		// continue in lock

		oldest := l.quited
		for _, reqTime := range l.subs {
			if !reqTime.IsZero() && (oldest.IsZero() || reqTime.Before(oldest)) {
				oldest = reqTime
			}
		}
		for _, reqTime := range l.unsubs {
			if !reqTime.IsZero() && (oldest.IsZero() || reqTime.Before(oldest)) {
				oldest = reqTime
			}
		}
		// continue in lock

		allDone := oldest.IsZero()
		expired := !allDone && oldest.Before(notBefore)
		if allDone || expired {
			// clear to exit
			l.expireTimer = nil
		}
		l.mutex.Unlock()

		if allDone {
			return
		}
		if expired {
			l.Func("", nil, errors.New("redis: listener connection reset due to command expiry"))
			l.closeConn(conn)
			return
		}

		// evaluate again
		timer.Reset(l.CommandTimeout / 2)
	}
}

// Close terminates the connection establishment. The Listener Func is called
// with ErrClosed before return, and after the network connection was closed.
// Calling Close more than once just blocks until the first call completed.
func (l *Listener) Close() error {
	var conn net.Conn
	l.mutex.Lock()
	if l.quited.IsZero() {
		l.quited = time.Now()
		if l.expireTimer == nil {
			l.expireTimer = time.NewTimer(l.CommandTimeout)
			go l.expire(l.expireTimer)
		}
		conn = l.conn
	}
	l.mutex.Unlock()

	if conn != nil {
		l.submit(conn, requestFix("*1\r\n$4\r\nQUIT\r\n"))
	}

	// await completion
	<-l.closed
	return nil
}

func (l *Listener) closeConn(conn net.Conn) {
	err := conn.Close()
	if err != nil && !errors.Is(err, net.ErrClosed) {
		l.Func("", nil, fmt.Errorf("redis: connection leak: %w", err))
	}
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

			l.mutex.Lock()
			quited := l.quited
			l.mutex.Unlock()
			if !quited.IsZero() {
				return
			}

			continue
		}
		// connect success
		retryDelay = 0

		// install
		subs, ok := l.releaseConn(conn)
		if !ok {
			return // accept exit
		}
		// resubscribe
		if len(subs) != 0 {
			go func(conn net.Conn) {
				l.submit(conn, requestWithList("\r\n$9\r\nSUBSCRIBE", subs))
			}(conn)

		}

		// operate
		err = l.readLoop(reader)
		if err != nil {
			l.Func("", nil, err)
		} else {
			return
		}
		l.closeConn(conn)

		// retract
		l.mutex.Lock()
		l.conn = nil
		quited := l.quited
		l.mutex.Unlock()
		if !quited.IsZero() {
			return
		}
	}
}

func (l *Listener) releaseConn(conn net.Conn) (subs []string, ok bool) {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	if !l.quited.IsZero() {
		return nil, false
	}

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

	if len(subs) != 0 {
		l.expireTimer = time.NewTimer(l.CommandTimeout)
		go l.expire(l.expireTimer)
	}

	return subs, true
}

func (l *Listener) readLoop(reader *bufio.Reader) error {
	// confirmed state as message channel mapping
	confirmedSubs := make(map[string]string)

	for {
		head, err := reader.Peek(16)
		if err != nil {
			// QUIT makes "+OK\r\n" + EOF
			if err == io.EOF && len(head) > 4 && string(head[:5]) == "+OK\r\n" {
				return nil
			}
			return err
		}

		head1 := binary.LittleEndian.Uint64(head[:8])
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

			channel, err := readBulk[string](reader)
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

			channel, err := readBulk[string](reader)
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
		return fmt.Errorf("redis: message array-reply payload-size: %w", err)
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
	_, err := conn.Write(req.buf)
	if err != nil {
		l.closeConn(conn)
	}
}

// SUBSCRIBE executes <https://redis.io/commands/subscribe> in a persistent
// manner. New connections automatically re-subscribe (until UNSUBSCRIBE).
func (l *Listener) SUBSCRIBE(channels ...string) {
	var channelN int

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
		// rewrite & count
		channels[channelN] = s
		channelN++
	}

	conn := l.conn
	if conn != nil && channelN != 0 && l.expireTimer == nil {
		l.expireTimer = time.NewTimer(l.CommandTimeout)
		go l.expire(l.expireTimer)
	}
	l.mutex.Unlock()

	if conn != nil && channelN != 0 {
		l.submit(conn, requestWithList("\r\n$9\r\nSUBSCRIBE", channels[:channelN]))
	}
}

// UNSUBSCRIBE executes <https://redis.io/commands/unsubscribe>, yet never with
// zero arguments.
func (l *Listener) UNSUBSCRIBE(channels ...string) {
	var channelN int

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
		// rewrite & count
		channels[channelN] = s
		channelN++
	}

	conn := l.conn
	if conn != nil && channelN != 0 && l.expireTimer == nil {
		l.expireTimer = time.NewTimer(l.CommandTimeout)
		go l.expire(l.expireTimer)
	}
	l.mutex.Unlock()

	if conn != nil && channelN != 0 {
		l.submit(conn, requestWithList("\r\n$11\r\nUNSUBSCRIBE", channels[:channelN]))
	}
}
