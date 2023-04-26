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
	"strings"
	"testing"
	"time"
)

var testConfig ClientConfig[string, string]
var testClient, benchClient *Client[string, string]

func init() {
	addr, ok := os.LookupEnv("TEST_REDIS_ADDR")
	if !ok {
		log.Fatal("Need TEST_REDIS_ADDR evironment variable with an address of a test server.\nCAUTION! Tests insert, modify and delete data.")
	}
	testConfig.Addr = addr

	if s, ok := os.LookupEnv("TEST_REDIS_PASSWORD"); ok {
		testConfig.Password = []byte(s)
	}

	benchClient = testConfig.NewClient()

	testConfig.CommandTimeout = time.Second
	testClient = testConfig.NewClient()

	// make random keys vary
	rand.Seed(time.Now().UnixNano())
}

func byteValueClient(t testing.TB) *Client[string, []byte] {
	config := ClientConfig[string, []byte]{
		Addr:     testConfig.Addr,
		Password: testConfig.Password,
	}
	c := config.NewClient()
	t.Cleanup(func() {
		err := c.Close()
		if err != nil {
			t.Error("close error:", err)
		}
	})
	return c
}

func randomKey(prefix string) string {
	return fmt.Sprintf("%s-%d", prefix, rand.Uint64())
}

func TestClose(t *testing.T) {
	t.Parallel()
	c := NewClient[string, string](testClient.Addr, 0, 0)
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
	c := testConfig.NewClient()
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

	c := NewClient[string, string]("doesnotexist.example.com:70", 0, connectTimeout)
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

	// break connection
	select {
	case conn := <-testClient.connSem:
		if conn.Conn != nil {
			conn.Conn.Close()
		}

		// replace with closed pipe
		c, _ := net.Pipe()
		c.Close()
		conn.Conn = c
		select {
		case testClient.connSem <- conn:
			break // write unlocked
		case <-timeout:
			t.Fatal("connection sempahore release timeout")
		}
	case <-timeout:
		t.Fatal("connection sempahore acquire timeout")
	}

	_, err := testClient.DEL("key")
	if !errors.Is(err, io.ErrClosedPipe) {
		t.Errorf("DEL got error %q, want %q", err, io.ErrClosedPipe)
	}
}

func TestRedisError(t *testing.T) {
	// server errors may not interfear with other commands
	t.Parallel()

	key, value := randomKey("test"), "abc"
	newLen, err := testClient.APPEND(key, value)
	if err != nil {
		t.Fatalf("APPEND %q %q error: %s", key, value, err)
	}
	if newLen != int64(len(value)) {
		t.Errorf("APPEND %q %q got length %d, want %d", key, value, newLen, len(value))
	}

	_, err = testClient.DELArgs()
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

	value := "01234567"
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

	for _, size := range []int{8, 800, 4000} {
		b.Run(fmt.Sprintf("%dB", size), func(b *testing.B) {
			if err := benchClient.SET(key, strings.Repeat("B", size)); err != nil {
				b.Fatal("population error:", err)
			}

			b.Run("sequential", func(b *testing.B) {
				b.SetBytes(int64(size))
				for i := 0; i < b.N; i++ {
					v, err := benchClient.GET(key)
					if err != nil {
						b.Fatal("error:", err)
					}
					if len(v) != size {
						b.Fatalf("got %d bytes, want %d", len(v), size)
					}
				}
			})

			b.Run("parallel", func(b *testing.B) {
				b.RunParallel(func(pb *testing.PB) {
					b.SetBytes(int64(size))
					for pb.Next() {
						v, err := benchClient.GET(key)
						if err != nil {
							b.Fatal("error:", err)
						}
						if len(v) != size {
							b.Fatalf("got %d bytes, want %d", len(v), size)
						}
					}
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

	for _, size := range []int64{1, 12, 144} {
		b.Run(fmt.Sprintf("%d×8B", size), func(b *testing.B) {
			for {
				n, err := benchClient.RPUSH(key, strings.Repeat("B", 8))
				if err != nil {
					b.Fatal("population error:", err)
				}
				if n >= size {
					break
				}
			}

			b.Run("sequential", func(b *testing.B) {
				b.SetBytes(size * 8)
				for i := 0; i < b.N; i++ {
					values, err := benchClient.LRANGE(key, 0, size-1)
					if err != nil {
						b.Fatal("error:", err)
					}
					if int64(len(values)) != size {
						b.Fatalf("got %d values, want %d", len(values), size)
					}
				}
			})

			b.Run("parallel", func(b *testing.B) {
				b.RunParallel(func(pb *testing.PB) {
					b.SetBytes(int64(size) * 8)
					for pb.Next() {
						values, err := benchClient.LRANGE(key, 0, size-1)
						if err != nil {
							b.Fatal("error:", err)
						}
						if int64(len(values)) != size {
							b.Fatalf("got %d values, want %d", len(values), size)
						}
					}
				})
			})
		})
	}
}

func TestNoAllocation(t *testing.T) {
	// both too large for stack:
	key := randomKey(strings.Repeat("k", 10e6))
	value := strings.Repeat("v", 10e6)

	f := func() {
		if _, err := testClient.INCR(key); err != nil {
			t.Fatal(err)
		}
		if err := testClient.SET(key, value); err != nil {
			t.Fatal(err)
		}
		if _, err := testClient.APPEND(key, value); err != nil {
			t.Fatal(err)
		}
		if _, err := testClient.DEL(key); err != nil {
			t.Fatal(err)
		}

		if err := testClient.HMSET(key, []string{key}, []string{value}); err != nil {
			t.Fatal(err)
		}
		if _, err := testClient.DELArgs(key); err != nil {
			t.Fatal(err)
		}
	}

	perRun := testing.AllocsPerRun(1, f)
	if perRun != 0 {
		t.Errorf("did %f memory allocations, want 0", perRun)
	}
}
