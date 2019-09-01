package redis

import (
	"fmt"
	"log"
	"math"
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
}

func TestParseInt(t *testing.T) {
	for _, v := range []int64{0, -1, 1, math.MinInt64, math.MaxInt64} {
		got := ParseInt([]byte(strconv.FormatInt(v, 10)))
		if got != v {
			t.Errorf("got %d, want %d", got, v)
		}
	}
}

func TestNormalizeAddr(t *testing.T) {
	golden := []struct{ Addr, Normal string }{
		{"", "localhost:6379"},
		{":", "localhost:6379"},
		{"test.host", "test.host:6379"},
		{"test.host:", "test.host:6379"},
		{":99", "localhost:99"},
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
