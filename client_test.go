package redis

import (
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"os"
	"runtime"
	"testing"
	"time"
)

var testClient, benchClient *Client

func init() {
	addr := os.Getenv("TEST_REDIS_ADDR")
	if addr == "" {
		log.Fatal("Need TEST_REDIS_ADDR evironment variable with an address of a test server.\nCAUTION! Tests insert, modify and delete data.")
	}
	testClient = NewClient(addr, time.Second, time.Second)
	benchClient = NewClient(addr, 0, 0)

	// make random keys vary
	rand.Seed(time.Now().UnixNano())
}

func randomKey(prefix string) string {
	return fmt.Sprintf("%s-%d", prefix, rand.Uint64())
}

func TestClose(t *testing.T) {
	t.Parallel()
	c := NewClient(testClient.Addr, 0, 0)
	if err := c.Close(); err != nil {
		t.Fatal("close got error:", err)
	}

	if _, err := c.GET("arbitrary"); err != ErrClosed {
		t.Errorf("command got error %q, want %q", err, ErrClosed)
	}

	if err := c.Close(); err != nil {
		t.Fatal("second close got error:", err)
	}
}

func TestCloseBussy(t *testing.T) {
	t.Parallel()
	c := NewClient(testClient.Addr, 0, 0)
	key := randomKey("counter")

	timeout := time.NewTimer(time.Second)

	// launch command loops
	exit := make(chan error, runtime.GOMAXPROCS(0))
	for routines := cap(exit); routines > 0; routines-- {
		go func() {
			for {
				_, err := c.INCR(key)
				if err != nil {
					exit <- err
					return
				}
			}
		}()
	}

	// await full I/O activity
	time.Sleep(2 * time.Millisecond)
	t.Log(len(c.readQueue), "pending commands")

	if err := c.Close(); err != nil {
		t.Fatal("close got error:", err)
	}
	for i := 0; i < cap(exit); i++ {
		select {
		case <-timeout.C:
			t.Fatalf("%d out of %d command routines stopped before timeout", i, cap(exit))
		case err := <-exit:
			if err != ErrClosed {
				t.Errorf("got exit error %q, want %q", err, ErrClosed)
			}
		}
	}
}

func TestUnavailable(t *testing.T) {
	t.Parallel()

	connectTimeout := 100 * time.Millisecond

	c := NewClient("doesnotexist.example.com:70", 0, connectTimeout)
	defer func() {
		if err := c.Close(); err != nil {
			t.Error("close got error:", err)
		}

		if _, err := c.GET("arbitrary"); err != ErrClosed {
			t.Errorf("command after close got error %q, want %q", err, ErrClosed)
		}
	}()

	_, err := c.GET("arbitrary")
	if e := new(net.OpError); !errors.As(err, &e) {
		t.Errorf("got error %v, want a net.OpError", err)
	} else if e.Op != "dial" {
		t.Errorf(`got error for opperation %q, want "dial"`, e.Op)
	}

	// let the Client retry…
	time.Sleep(2 * connectTimeout)

	_, err = c.GET("arbitrary")
	if e := new(net.OpError); !errors.As(err, &e) {
		t.Errorf("after connect retry, got error %v, want a net.OpError", err)
	} else if e.Op != "dial" {
		t.Errorf(`after connect retry, got error for opperation %q, want "dial"`, e.Op)
	}
}

// Note that testClient must recover for the next test to pass.
func TestWriteError(t *testing.T) {
	timeout := time.After(time.Second)
	select {
	case conn := <-testClient.connSem:
		if conn.Conn != nil {
			conn.Close()
		}

		select {
		case testClient.connSem <- conn:
			break
		case <-timeout:
			t.Fatal("connection sempahore release timeout")
		}

		if conn.Conn == nil {
			t.Fatal("no connection")
		}
	case <-timeout:
		t.Fatal("connection sempahore acquire timeout")
	}

	_, err := testClient.DEL("key")
	var e *net.OpError
	if !errors.As(err, &e) {
		t.Fatalf("got error %v, want a net.OpError", err)
	}
	if e.Op != "write" {
		t.Errorf(`got error for opperation %q, want "write"`, e.Op)
	}
}

// Note that testClient must recover for the next test to pass.
func TestReadError(t *testing.T) {
	timeout := time.After(time.Second)
	select {
	case conn := <-testClient.connSem:
		c, ok := conn.Conn.(interface{ CloseRead() error })
		if ok {
			c.CloseRead()
		}

		select {
		case testClient.connSem <- conn:
			if !ok {
				t.Skip("no CloseRead method on connection")
			}

		case <-timeout:
			t.Fatal("connection sempahore release timeout")
		}
	case <-timeout:
		t.Fatal("connection sempahore acquire timeout")
	}

	_, err := testClient.DEL("key")
	if !errors.Is(err, io.EOF) {
		t.Errorf("got error %v, want a EOF", err)
	}
}

func TestRedisError(t *testing.T) {
	// server errors may not interfear with other commands
	t.Parallel()

	key, value := randomKey("test"), []byte("abc")
	newLen, err := testClient.APPEND(key, value)
	if err != nil {
		t.Fatalf("APPEND %q %q error: %s", key, value, err)
	}
	if newLen != int64(len(value)) {
		t.Errorf("APPEND %q %q got length %d, want %d", key, value, newLen, len(value))
	}

	_, err = testClient.BytesDELArgs()
	switch e := err.(type) {
	default:
		t.Errorf("DEL without arguments got error %v, want a RedisError", err)
	case ServerError:
		t.Log("DEL without arguments got error:", e)
		if got := e.Prefix(); got != "ERR" {
			t.Errorf(`error %q got prefix %q, want "ERR"`, err, got)
		}
	}

	_, err = testClient.LINDEX(key, 42)
	switch e := err.(type) {
	default:
		t.Errorf("LINDEX %q got error %v, want a RedisError", key, err)
	case ServerError:
		t.Log("LINDEX on string got error:", e)
		if got := e.Prefix(); got != "WRONGTYPE" {
			t.Errorf(`LINDEX %q error %q got prefix %q, want "WRONGTYPE"`, key, err, got)
		}
	}

	_, err = testClient.LRANGE(key, 42, 99)
	switch e := err.(type) {
	default:
		t.Errorf("LRANGE %q got error %v, want a RedisError", key, err)
	case ServerError:
		t.Log("LRANGE on string got error:", e)
		if got := e.Prefix(); got != "WRONGTYPE" {
			t.Errorf(`LRANGE %q error %q got prefix %q, want "WRONGTYPE"`, key, err, got)
		}
	}
}

func BenchmarkSimpleString(b *testing.B) {
	key := randomKey("bench")
	defer func() {
		if _, err := benchClient.DEL(key); err != nil {
			b.Error("cleanup error:", err)
		}
	}()

	value := make([]byte, 8)
	b.Run("sequential", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			if err := benchClient.SET(key, value); err != nil {
				b.Fatal("error:", err)
			}
		}
	})
	b.Run("parallel", func(b *testing.B) {
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				if err := benchClient.SET(key, value); err != nil {
					b.Fatal("error:", err)
				}
			}
		})
	})
}

func BenchmarkInteger(b *testing.B) {
	key := randomKey("bench")
	defer func() {
		if _, err := benchClient.DEL(key); err != nil {
			b.Error("cleanup error:", err)
		}
	}()

	b.Run("sequential", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			if _, err := benchClient.DEL(key); err != nil {
				b.Fatal("error:", err)
			}
		}
	})
	b.Run("parallel", func(b *testing.B) {
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				if _, err := benchClient.DEL(key); err != nil {
					b.Fatal("error:", err)
				}
			}
		})
	})
}

func BenchmarkBulk(b *testing.B) {
	key := randomKey("bench")
	defer func() {
		if _, err := benchClient.DEL(key); err != nil {
			b.Error("cleanup error:", err)
		}
	}()

	getBytes := func(b *testing.B, size int) {
		bytes, err := benchClient.GET(key)
		if err != nil {
			b.Fatal("error:", err)
		}
		if len(bytes) != size {
			b.Fatalf("got %d bytes, want %d", len(bytes), size)
		}
	}
	getString := func(b *testing.B, size int) {
		s, _, err := benchClient.GETString(key)
		if err != nil {
			b.Fatal("error:", err)
		}
		if len(s) != size {
			b.Fatalf("got %d bytes, want %d", len(s), size)
		}
	}

	for _, size := range []int{8, 800, 24000} {
		b.Run(fmt.Sprintf("%dB", size), func(b *testing.B) {
			if err := benchClient.SET(key, make([]byte, size)); err != nil {
				b.Fatal("population error:", err)
			}

			b.Run("sequential", func(b *testing.B) {
				b.Run("bytes", func(b *testing.B) {
					b.SetBytes(int64(size))
					for i := 0; i < b.N; i++ {
						getBytes(b, size)
					}
				})
				b.Run("string", func(b *testing.B) {
					b.SetBytes(int64(size))
					for i := 0; i < b.N; i++ {
						getString(b, size)
					}
				})
			})

			b.Run("parallel", func(b *testing.B) {
				b.Run("bytes", func(b *testing.B) {
					b.SetBytes(int64(size))
					b.RunParallel(func(pb *testing.PB) {
						for pb.Next() {
							getBytes(b, size)
						}
					})
				})
				b.Run("string", func(b *testing.B) {
					b.SetBytes(int64(size))
					b.RunParallel(func(pb *testing.PB) {
						for pb.Next() {
							getString(b, size)
						}
					})
				})
			})
		})
	}
}

func BenchmarkArray(b *testing.B) {
	key := randomKey("bench")
	defer func() {
		if _, err := benchClient.DEL(key); err != nil {
			b.Error("cleanup error:", err)
		}
	}()

	getBytes := func(b *testing.B, size int64) {
		values, err := benchClient.LRANGE(key, 0, size-1)
		if err != nil {
			b.Fatal("error:", err)
		}
		if int64(len(values)) != size {
			b.Fatalf("got %d values", len(values))
		}
	}
	getString := func(b *testing.B, size int64) {
		values, err := benchClient.LRANGEString(key, 0, size-1)
		if err != nil {
			b.Fatal("error:", err)
		}
		if int64(len(values)) != size {
			b.Fatalf("got %d values", len(values))
		}
	}

	for _, size := range []int64{1, 12, 144} {
		b.Run(fmt.Sprintf("%d×8B", size), func(b *testing.B) {
			for {
				n, err := benchClient.RPUSH(key, make([]byte, 8))
				if err != nil {
					b.Fatal("population error:", err)
				}
				if n >= size {
					break
				}
			}

			b.Run("sequential", func(b *testing.B) {
				b.Run("bytes", func(b *testing.B) {
					b.SetBytes(size * 8)
					for i := 0; i < b.N; i++ {
						getBytes(b, size)
					}
				})
				b.Run("string", func(b *testing.B) {
					b.SetBytes(size * 8)
					for i := 0; i < b.N; i++ {
						getString(b, size)
					}
				})
			})

			b.Run("parallel", func(b *testing.B) {
				b.Run("bytes", func(b *testing.B) {
					b.SetBytes(size * 8)
					b.RunParallel(func(pb *testing.PB) {
						for pb.Next() {
							getBytes(b, size)
						}
					})
				})
				b.Run("string", func(b *testing.B) {
					b.SetBytes(size * 8)
					b.RunParallel(func(pb *testing.PB) {
						for pb.Next() {
							getString(b, size)
						}
					})
				})
			})
		})
	}
}
