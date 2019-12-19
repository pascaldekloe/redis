package redis

import (
	"encoding/binary"
	"flag"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

func newTestListener(t *testing.T) (l *Listener, timeout *time.Timer, done chan struct{}) {
	l = testClient.NewListener()
	timeout = time.NewTimer(time.Second)
	done = make(chan struct{})

	go func() {
		defer close(done)
		defer timeout.Stop()

		for {
			select {
			case err, ok := <-l.Errs:
				if !ok {
					return
				}
				t.Error("listener error:", err)

			case <-timeout.C:
				t.Error("shutdown timeout")
				return
			}
		}
	}()

	return
}

func TestSubscribe(t *testing.T) {
	t.Parallel()

	l, timeout, done := newTestListener(t)
	defer func() {
		go l.Close()
		<-done
	}()

	channel := randomKey("channel")
	messages, _ := l.SUBSCRIBE(channel)
	// await subscription
	time.Sleep(20 * time.Millisecond)

	clientCount, err := testClient.PUBLISH(channel, []byte("hello"))
	if err != nil {
		t.Error("first publish got error:", err)
	} else if clientCount != 1 {
		t.Errorf("first publish got %d clients, want 1", clientCount)
	}
	select {
	case <-timeout.C:
		t.Fatal("message channel timeout")
	case got, ok := <-messages:
		if !ok {
			t.Fatal("message channel closed")
		}
		if string(got) != "hello" {
			t.Errorf(`got message %q, want "hello"`, got)
		}
	}

	clientCount, err = testClient.PUBLISHString(channel, "world")
	if err != nil {
		t.Error("second publish got error:", err)
	} else if clientCount != 1 {
		t.Errorf("second publish got %d clients, want 1", clientCount)
	}
	select {
	case <-timeout.C:
		t.Fatal("message channel timeout")
	case got, ok := <-messages:
		if !ok {
			t.Fatal("message channel closed")
		}
		if string(got) != "world" {
			t.Errorf(`got message %q, want "world"`, got)
		}
	}
}

func TestUnsubscribe(t *testing.T) {
	t.Parallel()

	l, timeout, done := newTestListener(t)
	defer func() {
		go l.Close()
		<-done
	}()

	channel := randomKey("channel")
	messages, UNSUBSCRIBE := l.SUBSCRIBE(channel)
	// await subscription
	time.Sleep(20 * time.Millisecond)
	UNSUBSCRIBE()

	select {
	case <-timeout.C:
		t.Fatal("message channel close timeout")
	case m, ok := <-messages:
		if ok {
			t.Errorf("received message %q", m)
		}
	}

	clientCount, err := testClient.PUBLISHString(channel, "ping")
	if err != nil {
		t.Error("publish got error:", err)
	} else if clientCount != 0 {
		t.Errorf("publish got %d clients, want 0", clientCount)
	}
}

func TestUnsubscribeRace(t *testing.T) {
	t.Parallel()

	l, timeout, done := newTestListener(t)
	defer func() {
		go l.Close()
		<-done
	}()

	channel := randomKey("channel")
	messages, UNSUBSCRIBE := l.SUBSCRIBE(channel)
	// don't await subscription
	UNSUBSCRIBE()

	select {
	case <-timeout.C:
		t.Fatal("message channel close timeout")
	case m, ok := <-messages:
		if ok {
			t.Errorf("received message %q", m)
		}
	}

	clientCount, err := testClient.PUBLISHString(channel, "ping")
	if err != nil {
		t.Error("publish got error:", err)
	} else if clientCount != 0 {
		t.Errorf("publish got %d clients, want 0", clientCount)
	}
}

func TestListenerClose(t *testing.T) {
	t.Parallel()

	l, timeout, done := newTestListener(t)
	defer func() {
		go l.Close()
		<-done
	}()

	messages, UNSUBSCRIBE := l.SUBSCRIBE(randomKey("channel"))
	// await subscription
	time.Sleep(20 * time.Millisecond)
	l.Close()

	select {
	case <-timeout.C:
		t.Fatal("error channel timeout")
	case err, ok := <-l.Errs:
		if ok {
			t.Error("received error:", err)
		}
	}

	select {
	case <-timeout.C:
		t.Fatal("message channel timeout")
	case m, ok := <-messages:
		if ok {
			t.Errorf("received message %q", m)
		}
	}

	UNSUBSCRIBE() // because we can
	UNSUBSCRIBE() // because we can
}

func BenchmarkPubSub(b *testing.B) {
	testing.Init()
	benchTime, err := time.ParseDuration(flag.Lookup("test.benchtime").Value.String())
	if err != nil {
		b.Fatal("benchtime flag:", err)
	}

	l := benchClient.NewListener()
	defer l.Close()
	go func() {
		for err := range l.Errs {
			b.Error("listener error:", err)
		}
	}()

	channel := randomKey("channel")
	messages, _ := l.SUBSCRIBE(channel)
	// await subscription
	time.Sleep(20 * time.Millisecond)

	for _, size := range []int{8, 800, 24000} {
		b.Run(fmt.Sprintf("%dB", size), func(b *testing.B) {
			for _, routines := range []int{1, 2, 16} {
				b.Run(fmt.Sprintf("%dpublishers", routines), func(b *testing.B) {
					b.SetBytes(int64(size))

					timeout := time.NewTimer(benchTime + time.Second)
					defer timeout.Stop()

					// publish
					ready := make(chan struct{})
					pubN := int64(b.N)
					var pubTimeNS int64
					pubFunc := func() {
						message := make([]byte, size)
						ready <- struct{}{}

						start := time.Now().UnixNano()
						for atomic.AddInt64(&pubN, -1) >= 0 {
							binary.LittleEndian.PutUint64(message, uint64(time.Now().UnixNano()))
							benchClient.PUBLISH(channel, message)
						}
						atomic.AddInt64(&pubTimeNS, time.Now().UnixNano()-start)
					}
					for i := 0; i < routines; i++ {
						go pubFunc()
					}
					// await routines blocking on ready
					time.Sleep(time.Millisecond)

					// launch
					b.ResetTimer()
					for i := 0; i < routines; i++ {
						<-ready
					}
					var delayNS uint64
					for i := 0; i < b.N; i++ {
						select {
						case <-timeout.C:
							b.Fatalf("got %d out of %d messages", i, b.N)

						case m := <-messages:
							if len(m) != size {
								b.Fatalf("got %d bytes, want %d", len(m), size)
							}
							delayNS += uint64(time.Now().UnixNano()) - binary.LittleEndian.Uint64(m)
						}
					}

					b.ReportMetric(float64(delayNS)/float64(b.N), "ns/delay")
					b.ReportMetric(float64(pubTimeNS)/float64(b.N), "ns/publish")
				})
			}
		})
	}
}
