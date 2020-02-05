package redis

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
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

type subscription struct {
	messages    chan []byte
	unsubscribe func()
}

// Listener is a registry for <https://redis.io/topics/pubsub>.
// The Errs channel MUST be read continuously until closed.
// Broken connection states cause automated reconnects.
// Multiple goroutines may invoke methods on a Listener simultaneously.
type Listener struct {
	// Connection error propagation is closed uppon Close.
	Errs <-chan error
	// hidden copy of Errs for send
	errs   chan error
	closed chan struct{}
	ctx    context.Context
	cancel func()

	// client settings (copy)
	connConfig

	mutex sync.Mutex
	conn  net.Conn
	// requested subscription state
	subs   map[string]subscription
	unsubs map[string]struct{}
	// actual subscription state is only modified from read routine
	channels map[string]chan []byte
}

// NewListener launches a managed connection.
//
// Any following SELECT on c does not affect the database of the Listener.
// The same goes for password changes with AUTH.
func (c *Client) NewListener() *Listener {
	errs := make(chan error)
	l := &Listener{
		Errs:   errs,
		errs:   errs,
		closed: make(chan struct{}),
		connConfig: connConfig{
			Addr:           c.Addr,
			DB:             atomic.LoadInt64(&c.db),
			CommandTimeout: c.commandTimeout,
			ConnectTimeout: c.connectTimeout,
		},
		subs:     make(map[string]subscription),
		unsubs:   make(map[string]struct{}),
		channels: make(map[string]chan []byte),
	}
	l.Password, _ = c.password.Load().([]byte)
	l.ctx, l.cancel = context.WithCancel(context.Background())

	go l.connectLoop()

	return l
}

// Close terminates connection establishment.
// All subscription/message channels are closed, and so is Listener.Errs.
func (l *Listener) Close() error {
	l.mutex.Lock()
	l.cancel()
	conn := l.conn
	l.mutex.Unlock()

	var err error
	if conn != nil {
		err = conn.Close()
	}

	// await shutdown
	<-l.closed

	return err
}

func (l *Listener) connectLoop() {
	defer func() {
		close(l.errs)
		for _, sub := range l.subs {
			close(sub.messages)
		}
		close(l.closed)
	}()

	var reconnectDelay time.Duration
	for {
		conn, reader, err := connect(l.connConfig)
		if err != nil {
			// workaround https://github.com/golang/go/issues/36208
			if l.ctx.Err() != nil {
				return // terminated by Close
			}

			// closed loop protection
			retry := time.NewTimer(reconnectDelay)

			// propagate error
			l.errs <- fmt.Errorf("redis: listener offline due %w", err)

			reconnectDelay = 2*reconnectDelay + time.Millisecond
			if reconnectDelay > time.Second/2 {
				reconnectDelay = time.Second / 2
			}
			<-retry.C
			continue
		}

		reconnectDelay = 0

		l.mutex.Lock()
		if l.ctx.Err() != nil {
			// terminated by Close
			l.mutex.Unlock()
			conn.Close() // discard
			return
		}
		l.conn = conn

		// apply pendig unsubscribes
		for name := range l.unsubs {
			delete(l.unsubs, name)
			if sub, ok := l.subs[name]; ok {
				delete(l.subs, name)
				close(sub.messages)
			}
		}
		l.mutex.Unlock()

		if len(l.subs) != 0 {
			// resubscribe
			channels := make([]string, 0, len(l.subs))
			for name := range l.subs {
				channels = append(channels, name)
			}
			r := newRequestSize(1+len(channels), "\r\n$9\r\nSUBSCRIBE")
			r.addStringList(channels)
			l.submit(conn, r)
		}

		err = l.receiveLoop(reader)
		l.mutex.Lock()
		l.conn = nil
		l.mutex.Unlock()
		if l.ctx.Err() == nil {
			l.errs <- err
		}

		conn.Close()

		// reset subscription state
		for name := range l.channels {
			delete(l.channels, name)
		}
	}
}

func (l *Listener) receiveLoop(reader *bufio.Reader) error {
	for {
		pushType, dest, message, err := decodePushArray(reader)
		if err != nil {
			return err
		}

		switch pushType {
		case "message":
			// the hot path is lock free
			ch, ok := l.channels[dest]
			if ok {
				ch <- message
			}

		case "subscribe":
			if _, ok := l.channels[dest]; !ok {
				l.mutex.Lock()
				if sub, ok := l.subs[dest]; ok {
					l.channels[dest] = sub.messages
				}
				l.mutex.Unlock()
			}

		case "unsubscribe":
			delete(l.channels, dest)

			l.mutex.Lock()
			sub, ok := l.subs[dest]
			delete(l.subs, dest)
			delete(l.unsubs, dest)
			l.mutex.Unlock()

			if ok {
				close(sub.messages)
			}
		}
	}
}

// Submit ether sends a request, or causes a reconnect.
func (l *Listener) submit(conn net.Conn, req *request) {
	// apply timeout if set
	if l.CommandTimeout != 0 {
		conn.SetWriteDeadline(time.Now().Add(l.CommandTimeout))
	}

	// send command
	_, err := conn.Write(req.buf)
	if err != nil {
		if l.ctx.Err() == nil {
			l.errs <- err
			conn.Close()
		}
		return
	}
}

// SUBSCRIBE executes <https://redis.io/commands/subscribe>. Listener will
// automatically resubscribe (until UNSUBSCRIBE) in case of an error.
// UNSUBSCRIBE executes <https://redis.io/commands/unsubscribe>. The messages
// channel is closed after confirmation or connection loss.
//
// Publications to the provided channel (name) are send to the messages in a
// sequential manner. The reception order is guaranteed to match the Redis
// submission. Blocking sends on messages hog the connection.
func (l *Listener) SUBSCRIBE(channel string) (messages <-chan []byte, UNSUBSCRIBE func()) {
	sub := subscription{
		messages: make(chan []byte),
		unsubscribe: func() {
			l.mutex.Lock()
			l.unsubs[channel] = struct{}{}
			conn := l.conn
			l.mutex.Unlock()

			if conn != nil {
				r := newRequest("*2\r\n$11\r\nUNSUBSCRIBE\r\n$")
				r.addString(channel)
				l.submit(conn, r)
			}
		},
	}

	l.mutex.Lock()
	if current, ok := l.subs[channel]; ok {
		sub = current
	} else {
		l.subs[channel] = sub
	}
	conn := l.conn
	l.mutex.Unlock()

	if conn != nil {
		r := newRequest("*2\r\n$9\r\nSUBSCRIBE\r\n$")
		r.addString(channel)
		l.submit(conn, r)
	}

	return sub.messages, sub.unsubscribe
}
