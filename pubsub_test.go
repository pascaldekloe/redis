package redis

import (
	"testing"
	"time"
)

func newTestListener(t *testing.T) (l *Listener, timeout *time.Timer, done chan struct{}) {
	l = testClient.NewListener()
	timeout = time.NewTimer(time.Second)
	done = make(chan struct{})

	go func() {
		defer close(done)

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
