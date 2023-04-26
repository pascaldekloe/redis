package redis

import (
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"
)

// listenerCall defines a ListenerConfig.Func invocation.
type listenerCall struct {
	channel string
	message string
	err     error
}

// newTestListener closes the channel upon ErrClosed, or test-time-out.
func newTestListener(t *testing.T) (*Listener, <-chan *listenerCall) {
	calls := make(chan *listenerCall, 99)
	closed := make(chan struct{})
	l := NewListener(ListenerConfig{
		Func: func(channel string, message []byte, err error) {
			if err == ErrClosed {
				select {
				case <-closed:
					t.Error("Listener called with ErrClosed again")
				default:
					close(closed)
				}
				return
			}

			select {
			case <-closed:
				t.Error("Listener call after ErrClosed")
			default:
				break
			}

			select {
			case calls <- &listenerCall{channel, string(message), err}:
				break
			default:
				t.Error("Listener recording capacity reached")
			}
		},

		Addr:           testConfig.Addr,
		CommandTimeout: testConfig.CommandTimeout,
		DialTimeout:    testConfig.DialTimeout,
		Password:       testConfig.Password,
	})

	t.Cleanup(func() {
		timeout := time.NewTimer(time.Second)

		if err := l.Close(); err != nil {
			t.Error("Listener Close error:", err)
		}

		select {
		case <-timeout.C:
			t.Error("timeout awaiting Listener shutdown")
		case <-closed:
			timeout.Stop()
		}

		for {
			select {
			case call := <-calls:
				if call.err != nil {
					t.Error("Listener called with error after Close: ", call.err)
				} else {
					t.Errorf("Listener called with %q@%q after Close", call.message, call.channel)
				}
			default:
				return
			}
		}
	})

	return l, calls
}

func TestSubscribe(t *testing.T) {
	t.Parallel()

	channel := randomKey("channel")
	const message1, message2 = "first", "second"

	// publish both messages
	go func() {
		start := time.Now()

		// publish until confirmed receiver
		var clientN int64
		for clientN == 0 {
			var err error
			clientN, err = testClient.PUBLISH(channel, message1)
			switch {
			case err != nil:
				t.Error("publish error:", err)
				return
			case time.Now().Sub(start) > time.Second/10:
				t.Error("timeout: no publish receiver yet")
				return
			}
		}
		if clientN != 1 {
			t.Errorf("publish got %d clients, want 1", clientN)
		}

		// follow up with second message
		clientN, err := testClient.PUBLISHString(channel, message2)
		if err != nil {
			t.Error("followup publish error:", err)
			return
		}
		if clientN != 1 {
			t.Errorf("flowup publish got %d clients, want 1", clientN)
		}
	}()

	l, calls := newTestListener(t)
	l.SUBSCRIBE(channel)
	timeout := time.NewTimer(time.Second)
	defer timeout.Stop()

	select {
	case c := <-calls:
		if c.err != nil {
			t.Fatal("first call got error:", c.err)
		}
		if c.channel != channel || c.message != message1 {
			t.Errorf("first call got message %q@%q, want %q@%q",
				c.message, c.channel, message1, channel)
		}
	case <-timeout.C:
		t.Fatal("test timeout while awaiting first call")
	}

	select {
	case c := <-calls:
		if c.err != nil {
			t.Fatal("second call got error:", c.err)
		}
		if c.channel != channel || c.message != message2 {
			t.Errorf("second call got message %q@%q, want %q@%q",
				c.message, c.channel, message2, channel)
		}
	case <-timeout.C:
		t.Fatal("test timeout while awaiting second call")
	}
}

func TestUnsubscribe(t *testing.T) {
	t.Parallel()
	l, _ := newTestListener(t)

	channel := randomKey("channel")
	l.SUBSCRIBE(channel)
	awaitExecution()

	l.UNSUBSCRIBE(channel)
	awaitExecution()

	clientCount, err := testClient.PUBLISHString(channel, "ping")
	if err != nil {
		t.Error("publish got error:", err)
	} else if clientCount != 0 {
		t.Errorf("publish got %d clients, want 0", clientCount)
	}
}

func TestUnsubscribeRace(t *testing.T) {
	t.Parallel()
	l, _ := newTestListener(t)

	channel := randomKey("channel")
	l.SUBSCRIBE(channel)
	// don't await execution
	l.UNSUBSCRIBE(channel)
	awaitExecution()

	clientCount, err := testClient.PUBLISHString(channel, "ping")
	if err != nil {
		t.Error("publish got error:", err)
	} else if clientCount != 0 {
		t.Errorf("publish got %d clients, want 0", clientCount)
	}
}

func TestSubscriptionRace(t *testing.T) {
	t.Parallel()
	l, _ := newTestListener(t)

	channels := make([]string, 9)
	for i := range channels {
		channels[i] = randomKey("channel")
	}

	const routineCount = 4
	var wg sync.WaitGroup
	wg.Add(routineCount)
	for routine := 0; routine < routineCount; routine++ {
		go func(routine int) {
			defer wg.Done()
			for round := 0; round < 1000; round++ {
				i, j := round%len(channels), (round+3)%len(channels)
				if routine&1 == 0 {
					l.SUBSCRIBE(channels[i], channels[j])
				} else {
					l.UNSUBSCRIBE(channels[i], channels[j])
				}
			}
		}(routine)
	}
	wg.Wait()
}

func TestListenerClose(t *testing.T) {
	t.Parallel()
	l, _ := newTestListener(t)

	channel1 := randomKey("channel")
	channel2 := randomKey("channel")
	l.SUBSCRIBE(channel1)
	awaitExecution()
	l.SUBSCRIBE(channel2)
	// don't await execution
	l.Close()

	if n, err := testClient.PUBLISHString(channel1, "ping"); err != nil {
		t.Error("publish error:", err)
	} else if n != 0 {
		t.Errorf("publish got %d clients, want 0", n)
	}
	if n, err := testClient.PUBLISHString(channel2, "ping"); err != nil {
		t.Error("publish subscribe error:", err)
	} else if n != 0 {
		t.Errorf("publish subscribe got %d clients, want 0", n)
	}

	// because we can
	l.UNSUBSCRIBE(channel1)
	l.UNSUBSCRIBE(channel1)
	l.SUBSCRIBE(channel1)
	l.SUBSCRIBE(channel2)
	l.UNSUBSCRIBE(channel2)
}

func TestListenerBufferLimit(t *testing.T) {
	t.Parallel()
	l, calls := newTestListener(t)

	channel := randomKey("channel")
	l.SUBSCRIBE(channel)
	awaitExecution()

	if n, err := testClient.PUBLISH(channel, strings.Repeat("A", l.BufferSize+1)); err != nil {
		t.Error("publish error:", err)
	} else if n != 1 {
		t.Errorf("publish got %d clients, want 1", n)
	}
	if n, err := testClient.PUBLISH(channel, strings.Repeat("A", l.BufferSize)); err != nil {
		t.Error("publish error:", err)
	} else if n != 1 {
		t.Errorf("publish got %d clients, want 1", n)
	}

	timeout := time.NewTimer(time.Second)
	defer timeout.Stop()

	select {
	case call := <-calls:
		if call.err != io.ErrShortBuffer {
			t.Errorf("got error %q, want io.ErrShortBuffer", call.err)
		}
	case <-timeout.C:
		t.Fatal("test timeout while awaiting second call")
	}

	select {
	case call := <-calls:
		if call.err != nil {
			t.Error("second message should pass; got error:", call.err)
		}
	case <-timeout.C:
		t.Fatal("test timeout while awaiting second call")
	}
}

func BenchmarkPubSub(b *testing.B) {
	for _, size := range []int{8, 800, 24000} {
		b.Run(fmt.Sprintf("%dB", size), func(b *testing.B) {
			for _, routineN := range []int{1, 2, 16} {
				b.Run(fmt.Sprintf("%dpublishers", routineN), func(b *testing.B) {
					benchmarkPubSub(b, size, routineN)
				})
			}
		})
	}
}

func benchmarkPubSub(b *testing.B, size, routineN int) {
	channel := randomKey("channel")
	b.SetBytes(int64(size))

	// closed after reception of b.N messages
	done := make(chan struct{})
	var messageCount int
	l := NewListener(ListenerConfig{
		Func: func(_ string, message []byte, err error) {
			if err != nil {
				if err != ErrClosed {
					b.Error("called with error:", err)
				}
				return
			}

			if l := len(message); l != size {
				b.Errorf("called with %d bytes, want %d", l, size)
			}

			messageCount++
			if messageCount == b.N {
				close(done)
			}
		},
		Addr:           testConfig.Addr,
		CommandTimeout: testConfig.CommandTimeout,
		DialTimeout:    testConfig.DialTimeout,
		Password:       testConfig.Password,
	})
	defer l.Close()

	l.SUBSCRIBE(channel)
	awaitExecution()
	b.ResetTimer()

	// publish
	message := strings.Repeat("B", size)
	b.SetParallelism(routineN)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			n, err := benchClient.PUBLISH(channel, message)
			if err != nil {
				b.Fatal("PUBLISH error:", err)
			}
			if n != 1 {
				b.Fatalf("PUBLISH to %d clients, want 1", n)
			}
		}

		<-done // await receival
	})
}

// AwaitExecution gives the server a reasonable amount of time to complete
// command(s).
func awaitExecution() {
	time.Sleep(100 * time.Millisecond)
}
