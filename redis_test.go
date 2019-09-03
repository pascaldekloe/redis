package redis

import (
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"math/rand"
	"net"
	"os"
	"strconv"
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

func TestParseInt(t *testing.T) {
	for _, v := range []int64{0, -1, 1, math.MinInt64, math.MaxInt64} {
		got := ParseInt([]byte(strconv.FormatInt(v, 10)))
		if got != v {
			t.Errorf("got %d, want %d", got, v)
		}
	}
	if got := ParseInt(nil); got != 0 {
		t.Errorf("got %d for the empty string, want 0", got)
	}
}

func TestNormalizeAddr(t *testing.T) {
	golden := []struct{ Addr, Normal string }{
		{"", "localhost:6379"},
		{":", "localhost:6379"},
		{"test.host", "test.host:6379"},
		{"test.host:", "test.host:6379"},
		{":99", "localhost:99"},
		{"/var/redis/../run/redis.sock", "/var/run/redis.sock"},
	}
	for _, gold := range golden {
		if got := normalizeAddr(gold.Addr); got != gold.Normal {
			t.Errorf("got %q for %q, want %q", got, gold.Addr, gold.Normal)
		}
	}
	for _, v := range []int64{0, -1, 1, math.MinInt64, math.MaxInt64} {
		got := ParseInt([]byte(strconv.FormatInt(v, 10)))
		if got != v {
			t.Errorf("got %d, want %d", got, v)
		}
	}
}

func TestUnavailable(t *testing.T) {
	c := NewClient("doesnotexist.example.com:70", 100*time.Millisecond, 100*time.Millisecond)
	_, err := c.INCRBY(randomKey("test"), 42)
	var e *net.OpError
	if !errors.As(err, &e) {
		t.Fatalf("got error %v, want a net.OpError", err)
	}
	if e.Op != "dial" {
		t.Errorf(`got error for opperation %q, want "dial"`, e.Op)
	}
}

// Note that testClient must recover for the next test to pass.
func TestWriteError(t *testing.T) {
	timeout := time.After(time.Second)
	select {
	case conn := <-testClient.writeSem:
		conn.Close()

		select {
		case testClient.writeSem <- conn:
			break
		case <-timeout:
			t.Fatal("write sempahore release timeout")
		}
	case <-timeout:
		t.Fatal("write sempahore aquire timeout")
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
	case conn := <-testClient.writeSem:
		c, ok := conn.(interface{ CloseRead() error })
		if ok {
			c.CloseRead()
		}

		select {
		case testClient.writeSem <- conn:
			if !ok {
				t.Skip("no CloseRead method on connection")
			}

		case <-timeout:
			t.Fatal("write sempahore release timeout")
		}
	case <-timeout:
		t.Fatal("write sempahore aquire timeout")
	}

	_, err := testClient.DEL("key")
	if !errors.Is(err, io.EOF) {
		t.Errorf("got error %v, want a EOF", err)
	}
}

func TestRedisError(t *testing.T) {
	key, value := randomKey("test"), []byte("abc")
	newLen, err := testClient.APPEND(key, value)
	if err != nil {
		t.Fatalf("APPEND %q %q error: %s", key, value, err)
	}
	if newLen != int64(len(value)) {
		t.Errorf("APPEND %q %q got length %d, want %d", key, value, newLen, len(value))
	}

	_, err = testClient.INCR(key)
	switch e := err.(type) {
	default:
		t.Errorf("INC %q got error %v, want a RedisError", key, err)
	case ServerError:
		t.Log("got:", e)
		if got := e.Prefix(); got != "ERR" {
			t.Errorf(`error %q got prefix %q, want "ERR"`, err, got)
		}
	}

	_, err = testClient.LINDEX(key, 42)
	switch e := err.(type) {
	default:
		t.Errorf("LINDEX %q got error %v, want a RedisError", key, err)
	case ServerError:
		t.Log("got:", e)
		if got := e.Prefix(); got != "WRONGTYPE" {
			t.Errorf(`LINDEX %q error %q got prefix %q, want "WRONGTYPE"`, key, err, got)
		}
	}

	_, err = testClient.LRANGE(key, 42, 99)
	switch e := err.(type) {
	default:
		t.Errorf("LRANGE %q got error %v, want a RedisError", key, err)
	case ServerError:
		t.Log("got:", e)
		if got := e.Prefix(); got != "WRONGTYPE" {
			t.Errorf(`LRANGE %q error %q got prefix %q, want "WRONGTYPE"`, key, err, got)
		}
	}
}

func TestFirstByteError(t *testing.T) {
	got := firstByteError('?', []byte("abc\r\n")).Error()
	want := `redis: protocol violation; unexpected first byte 0x3f in line "?abc\r\n"`
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func BenchmarkSimpleString(b *testing.B) {
	const key = "bench-key"
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
	const key = "bench-key"
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

func BenchmarkBulkString(b *testing.B) {
	const key = "bench-key"
	for _, size := range []int{8, 200, 1000} {
		b.Run(fmt.Sprintf("%dbyte", size), func(b *testing.B) {
			if err := benchClient.SET(key, make([]byte, size)); err != nil {
				b.Fatal("population error:", err)
			}

			b.Run("sequential", func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					bytes, err := benchClient.GET(key)
					if err != nil {
						b.Fatal("error:", err)
					}
					if len(bytes) != size {
						b.Fatalf("got %d bytes, want %d", len(bytes), size)
					}
				}
			})
			b.Run("parallel", func(b *testing.B) {
				b.RunParallel(func(pb *testing.PB) {
					for pb.Next() {
						bytes, err := benchClient.GET(key)
						if err != nil {
							b.Fatal("error:", err)
						}
						if len(bytes) != size {
							b.Fatalf("got %d bytes, want %d", len(bytes), size)
						}
					}
				})
			})
		})
	}
}

func BenchmarkArray(b *testing.B) {
	const key = "bench-array"
	defer func() {
		if _, err := benchClient.DEL(key); err != nil {
			b.Fatal("cleanup error:", err)
		}
	}()

	for _, size := range []int64{2, 12, 144} {
		b.Run(fmt.Sprintf("%dvalues", size), func(b *testing.B) {
			for n, err := benchClient.LLEN(key); n < size; n, err = benchClient.RPUSHString(key, "some-value") {
				if err != nil {
					b.Fatal("population error:", err)
				}
			}

			b.Run("sequential", func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					values, err := benchClient.LRANGE(key, 0, size-1)
					if err != nil {
						b.Fatal("error:", err)
					} else if int64(len(values)) != size {
						b.Fatalf("got %d values", len(values))
					}
				}
			})
			b.Run("parallel", func(b *testing.B) {
				b.RunParallel(func(pb *testing.PB) {
					for pb.Next() {
						values, err := benchClient.LRANGE(key, 0, size-1)
						if err != nil {
							b.Fatal("error:", err)
						} else if int64(len(values)) != size {
							b.Fatalf("got %d values", len(values))
						}
					}
				})
			})
		})
	}
}
