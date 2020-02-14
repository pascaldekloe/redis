package redis

import (
	"encoding/binary"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// listenerCall is a test recording.
type listenerCall struct {
	channel string
	message string
	err     error
}

// newTestListener closes the channel upon ErrClosed, or test-time-out.
func newTestListener(t *testing.T) (*Listener, <-chan *listenerCall) {
	record := make(chan *listenerCall, 99)
	l := NewListener(ListenerConfig{
		Func: func(channel string, message []byte, err error) {
			select {
			case record <- &listenerCall{channel, string(message), err}:
				break
			default:
				t.Error("Listener recording capacity reached")
			}
		},
		Addr:           testClient.Addr,
		Password:       password,
		CommandTimeout: 10 * time.Millisecond,
	})

	out := make(chan *listenerCall)
	go func() {
		defer close(out)

		timeout := time.NewTimer(time.Second)
		defer timeout.Stop()
		for {
			select {
			case <-timeout.C:
				t.Fatal("Listener recording time-out")
			case call := <-record:
				if call.err == ErrClosed {
					t.Log("Listener recording stop on ErrClosed")
					go func() {
						for call := range record {
							if call.err != nil {
								t.Error("Listener error after ErrClosed:", call.err)
							} else {
								t.Errorf("Listener message %q on %q after ErrClosed", call.message, call.channel)
							}
						}
					}()
					return
				}

				select {
				case <-timeout.C:
					if call.err != nil {
						t.Fatal("Unwanted Listener error:", call.err)
					} else {
						t.Fatalf("Unwanted Listener message %q on %q", call.message, call.channel)
					}
				case out <- call:
					break
				}
			}
		}
	}()
	return l, out
}

func TestSubscribe(t *testing.T) {
	t.Parallel()

	channel := randomKey("channel")
	const message1, message2 = "first", "second"

	// publish both messages
	go func() {
		start := time.Now()

		// wait for first message to land
		clientCount, err := testClient.PUBLISH(channel, []byte(message1))
		for err == nil && clientCount == 0 {
			if time.Now().Sub(start) > time.Second/10 {
				t.Fatal("publish timeout")
			}
			clientCount, err = testClient.PUBLISH(channel, []byte(message1))
		}
		if err != nil {
			t.Fatal("publish error:", err)
		}
		if clientCount != 1 {
			t.Errorf("publish got %d clients, want 1", clientCount)
		}

		// follow up with second message
		clientCount, err = testClient.PUBLISHString(channel, message2)
		if err != nil {
			t.Fatal("publish error:", err)
		}
		if clientCount != 1 {
			t.Errorf("publish got %d clients, want 1", clientCount)
		}
	}()

	l, calls := newTestListener(t)
	defer l.Close()

	l.SUBSCRIBE(channel)
	call1 := <-calls
	if call1.err != nil {
		t.Fatal("called with error:", call1.err)
	} else if call1.channel != channel || call1.message != message1 {
		t.Errorf("got message %q@%q, want %q@%q", call1.message, call1.channel, message1, channel)
	}
	call2 := <-calls
	if call2.err != nil {
		t.Fatal("called with error:", call2.err)
	} else if call2.channel != channel || call2.message != message2 {
		t.Errorf("got message %q@%q, want %q@%q", call2.message, call2.channel, message2, channel)
	}
}

func TestUnsubscribe(t *testing.T) {
	t.Parallel()

	l, _ := newTestListener(t)
	defer l.Close()

	channel := randomKey("channel")
	l.SUBSCRIBE(channel)
	// await execution
	time.Sleep(l.CommandTimeout)
	l.UNSUBSCRIBE(channel)
	// await execution
	time.Sleep(l.CommandTimeout)

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
	defer l.Close()

	channel := randomKey("channel")
	l.SUBSCRIBE(channel)
	// don't await execution
	l.UNSUBSCRIBE(channel)
	// await execution
	time.Sleep(l.CommandTimeout)

	clientCount, err := testClient.PUBLISHString(channel, "ping")
	if err != nil {
		t.Error("publish got error:", err)
	} else if clientCount != 0 {
		t.Errorf("publish got %d clients, want 0", clientCount)
	}
}

func TestSubscriptionConcurrency(t *testing.T) {
	t.Parallel()

	l, _ := newTestListener(t)
	defer l.Close()

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
	// await execution
	time.Sleep(l.CommandTimeout)
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
	defer l.Close()

	channel := randomKey("channel")
	l.SUBSCRIBE(channel)
	// await execution
	time.Sleep(l.CommandTimeout)

	if n, err := testClient.PUBLISH(channel, make([]byte, l.BufferSize+1)); err != nil {
		t.Error("publish error:", err)
	} else if n != 1 {
		t.Errorf("publish got %d clients, want 1", n)
	}
	if n, err := testClient.PUBLISH(channel, make([]byte, l.BufferSize)); err != nil {
		t.Error("publish error:", err)
	} else if n != 1 {
		t.Errorf("publish got %d clients, want 1", n)
	}

	if call := <-calls; call.err != io.ErrShortBuffer {
		t.Errorf("got error %q, want io.ErrShortBuffer", call.err)
	}
	if call := <-calls; call.err != nil {
		t.Error("second message should pass; got error:", call.err)
	}
}

func BenchmarkPubSub(b *testing.B) {
	channel := randomKey("channel")

	for _, size := range []int{8, 800, 24000} {
		b.Run(fmt.Sprintf("%dB", size), func(b *testing.B) {
			for _, routines := range []int{1, 2, 16} {
				b.Run(fmt.Sprintf("%dpublishers", routines), func(b *testing.B) {
					b.SetBytes(int64(size))

					// closed after reception of b.N messages
					done := make(chan struct{})
					var messageCount int
					var delayNS uint64
					l := NewListener(ListenerConfig{
						Func: func(_ string, message []byte, err error) {
							if err == ErrClosed {
								return
							}
							if err != nil {
								b.Fatal("called with error:", err)
							}

							if l := len(message); l != size {
								b.Fatalf("called with %d bytes, want %d", l, size)
							}
							timestamp := binary.LittleEndian.Uint64(message)
							delayNS += uint64(time.Now().UnixNano()) - timestamp
							messageCount++
							if messageCount >= b.N {
								b.ReportMetric(float64(delayNS)/float64(b.N), "ns/delay")
								close(done)
							}
						},
						Addr:     testClient.Addr,
						Password: password,
					})
					defer l.Close()

					l.SUBSCRIBE(channel)
					// await execution
					time.Sleep(10 * time.Millisecond)
					b.ResetTimer()

					// publish
					var pubTimeNS int64
					b.SetParallelism(routines)
					b.RunParallel(func(pb *testing.PB) {
						message := make([]byte, size)

						start := time.Now().UnixNano()
						for pb.Next() {
							timestamp := uint64(time.Now().UnixNano())
							binary.LittleEndian.PutUint64(message, timestamp)

							n, err := benchClient.PUBLISH(channel, message)
							if err != nil {
								b.Fatal("PUBLISH error:", err)
							}
							if n != 1 {
								b.Fatalf("PUBLISH to %d clients, want 1", n)
							}
						}
						atomic.AddInt64(&pubTimeNS, time.Now().UnixNano()-start)

						<-done // await receival
					})
					b.ReportMetric(float64(atomic.LoadInt64(&pubTimeNS))/float64(b.N), "ns/publish")
				})
			}
		})
	}
}
