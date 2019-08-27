package redis

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"testing"
	"time"
)

var testClient *Client

func init() {
	addr := os.Getenv("TEST_REDIS_ADDR")
	if addr == "" {
		log.Fatal("Need TEST_REDIS_ADDR evironment variable with an address of a test server.\nCAUTION! Tests insert, modify and delete data.")
	}
	testClient = NewClient(addr)

	rand.Seed(time.Now().UnixNano())
}

func TestKeyCRUD(t *testing.T) {
	const key, value, update = "key1", "first-value", "second-value"

	if err := testClient.SET(key, []byte(value)); err != nil {
		t.Fatalf("SET %q %q got error %q", key, value, err)
	}

	if bytes, err := testClient.GET(key); err != nil {
		t.Errorf("GET %q got error %q", key, err)
	} else if string(bytes) != value {
		t.Errorf(`GET %q got %q, want %q`, key, bytes, value)
	}

	if err := testClient.SETString(key, update); err != nil {
		t.Errorf("SET %q %q update got error %q", key, value, err)
	} else {
		if bytes, err := testClient.GET(key); err != nil {
			t.Errorf("GET %q got error %q", key, err)
		} else if string(bytes) != update {
			t.Errorf(`GET %q got %q, want %q`, key, bytes, update)
		}
	}

	ok, err := testClient.DEL(key)
	if err != nil {
		t.Errorf("DEL %q error %q", key, err)
	}
	if !ok {
		t.Errorf("DEL %q got false, want true", key)
	}
}

func TestBytesKeyCRUD(t *testing.T) {
	key, value, update := []byte("key1"), []byte("first-value"), []byte("second-value")

	if err := testClient.BytesSET(key, value); err != nil {
		t.Fatalf("SET %q %q got error %q", key, value, err)
	}

	if bytes, err := testClient.BytesGET(key); err != nil {
		t.Errorf("GET %q got error %q", key, err)
	} else if string(bytes) != string(value) {
		t.Errorf(`GET %q got %q, want %q`, key, bytes, value)
	}

	if err := testClient.BytesSET(key, update); err != nil {
		t.Errorf("SET %q %q update got error %q", key, value, err)
	} else {
		if bytes, err := testClient.BytesGET(key); err != nil {
			t.Errorf("GET %q got error %q", key, err)
		} else if string(bytes) != string(update) {
			t.Errorf(`GET %q got %q, want %q`, key, bytes, update)
		}
	}

	ok, err := testClient.BytesDEL(key)
	if err != nil {
		t.Errorf("DEL %q error %q", key, err)
	}
	if !ok {
		t.Errorf("DEL %q got false, want true", key)
	}
}

func TestKeyAbsent(t *testing.T) {
	const key = "doesn't exist"

	bytes, err := testClient.GET(key)
	if err != nil {
		t.Errorf("GET %q got error %q", key, err)
	}
	if bytes != nil {
		t.Errorf("GET %q got %q, want nil", key, bytes)
	}

	ok, err := testClient.DEL(key)
	if err != nil {
		t.Errorf("DEL %q got error %q", key, err)
	}
	if ok {
		t.Errorf("DEL %q got true, want false", key)
	}
}

func TestListLeft(t *testing.T) {
	key := fmt.Sprintf("test-list-%d", rand.Uint64())
	const minus, zero, one = "-", "zero", "one"

	if newLen, err := testClient.LPUSH(key, []byte(one)); err != nil {
		t.Fatalf("LPUSH %q %q got error %q", key, one, err)
	} else if newLen != 1 {
		t.Errorf("LPUSH %q %q got len %d, want 1", key, one, newLen)
	}
	if newLen, err := testClient.BytesLPUSH([]byte(key), []byte(zero)); err != nil {
		t.Fatalf("LPUSH %q %q got error %q", key, zero, err)
	} else if newLen != 2 {
		t.Errorf("LPUSH %q %q got len %d, want 2", key, zero, newLen)
	}
	if newLen, err := testClient.LPUSHString(key, minus); err != nil {
		t.Fatalf("LPUSH %q %q got error %q", key, minus, err)
	} else if newLen != 3 {
		t.Errorf("LPUSH %q %q got len %d, want 3", key, minus, newLen)
	}

	if value, err := testClient.LPOP(key); err != nil {
		t.Errorf("LPOP %q got error %q", key, err)
	} else if string(value) != minus {
		t.Errorf("LPOP %q got %q, want %q", key, value, minus)
	}
	if value, err := testClient.BytesLPOP([]byte(key)); err != nil {
		t.Errorf("LPOP %q got error %q", key, err)
	} else if string(value) != zero {
		t.Errorf("LPOP %q got %q, want %q", key, value, zero)
	}
}

func TestListRight(t *testing.T) {
	key := fmt.Sprintf("test-list-%d", rand.Uint64())
	const minus, zero, one = "-", "zero", "one"

	if newLen, err := testClient.RPUSH(key, []byte(one)); err != nil {
		t.Fatalf("RPUSH %q %q got error %q", key, one, err)
	} else if newLen != 1 {
		t.Errorf("RPUSH %q %q got len %d, want 1", key, one, newLen)
	}
	if newLen, err := testClient.BytesRPUSH([]byte(key), []byte(zero)); err != nil {
		t.Fatalf("RPUSH %q %q got error %q", key, zero, err)
	} else if newLen != 2 {
		t.Errorf("RPUSH %q %q got len %d, want 2", key, zero, newLen)
	}
	if newLen, err := testClient.RPUSHString(key, minus); err != nil {
		t.Fatalf("RPUSH %q %q got error %q", key, minus, err)
	} else if newLen != 3 {
		t.Errorf("RPUSH %q %q got len %d, want 3", key, minus, newLen)
	}

	if value, err := testClient.RPOP(key); err != nil {
		t.Errorf("RPOP %q got error %q", key, err)
	} else if string(value) != minus {
		t.Errorf("RPOP %q got %q, want %q", key, value, minus)
	}
	if value, err := testClient.BytesRPOP([]byte(key)); err != nil {
		t.Errorf("RPOP %q got error %q", key, err)
	} else if string(value) != zero {
		t.Errorf("RPOP %q got %q, want %q", key, value, zero)
	}
}

func TestNoSuchList(t *testing.T) {
	const key = "doesn't exist"

	if n, err := testClient.LLEN(key); err != nil {
		t.Errorf("LLEN %q got error %q", key, err)
	} else if n != 0 {
		t.Errorf("LLEN %q got %d, want 0 for non-existing", key, n)
	}

	const noSuchKey = `redis: server error "ERR no such key"`
	if err := testClient.LSET(key, 1, nil); err == nil || err.Error() != noSuchKey {
		t.Errorf(`LSET %q 1 "" got error %q, want %q`, key, err, noSuchKey)
	}

	if value, err := testClient.LPOP(key); err != nil {
		t.Errorf("LPOP %q got error %q", key, err)
	} else if value != nil {
		t.Errorf("LPOP %q got %q, want nil", key, value)
	}
	if value, err := testClient.RPOP(key); err != nil {
		t.Errorf("RPOP %q got error %q", key, err)
	} else if value != nil {
		t.Errorf("RPOP %q got %q, want nil", key, value)
	}
}

func TestHashCRUD(t *testing.T) {
	const key, field, value, update = "key2", "field1", "first-value", "second-value"

	if newField, err := testClient.HSET(key, field, []byte(value)); err != nil {
		t.Fatalf("HSET %q %q %q got error %q", key, field, value, err)
	} else if !newField {
		t.Errorf("HSET %q %q %q got newField false", key, field, value)
	}

	if bytes, err := testClient.HGET(key, field); err != nil {
		t.Errorf("HGET %q %q got error %q", key, field, err)
	} else if string(bytes) != value {
		t.Errorf(`HGET %q %q got %q, want %q`, key, field, bytes, value)
	}

	if newField, err := testClient.HSETString(key, field, update); err != nil {
		t.Errorf("HSET %q %q %q update got error %q", key, field, value, err)
	} else {
		if newField {
			t.Errorf("HSET %q %q %q update got newField true", key, field, value)
		}
		if bytes, err := testClient.HGET(key, field); err != nil {
			t.Errorf("HGET %q %q got error %q", key, field, err)
		} else if string(bytes) != update {
			t.Errorf(`HGET %q %q got %q, want %q`, key, field, bytes, update)
		}
	}

	ok, err := testClient.HDEL(key, field)
	if err != nil {
		t.Errorf("HDEL %q %q error %q", key, field, err)
	}
	if !ok {
		t.Errorf("HDEL %q %q got false, want true", key, field)
	}
}

func TestBytesHashCRUD(t *testing.T) {
	key, field, value, update := []byte("key2"), []byte("field1"), []byte("first-value"), []byte("second-value")

	if newField, err := testClient.BytesHSET(key, field, value); err != nil {
		t.Fatalf("HSET %q %q %q got error %q", key, field, value, err)
	} else if !newField {
		t.Errorf("HSET %q %q %q got newField false", key, field, value)
	}

	if bytes, err := testClient.BytesHGET(key, field); err != nil {
		t.Errorf("HGET %q %q got error %q", key, field, err)
	} else if string(bytes) != string(value) {
		t.Errorf(`HGET %q %q got %q, want %q`, key, field, bytes, value)
	}

	if newField, err := testClient.BytesHSET(key, field, update); err != nil {
		t.Errorf("HSET %q %q %q update got error %q", key, field, value, err)
	} else {
		if newField {
			t.Errorf("HSET %q %q %q update got newField true", key, field, value)
		}
		if bytes, err := testClient.BytesHGET(key, field); err != nil {
			t.Errorf("HGET %q %q got error %q", key, field, err)
		} else if string(bytes) != string(update) {
			t.Errorf(`HGET %q %q got %q, want %q`, key, field, bytes, update)
		}
	}

	ok, err := testClient.BytesHDEL(key, field)
	if err != nil {
		t.Errorf("HDEL %q %q error %q", key, field, err)
	}
	if !ok {
		t.Errorf("HDEL %q %q got false, want true", key, field)
	}
}

func TestHashAbsent(t *testing.T) {
	key, field := "doesn't exist", "also not set"

	bytes, err := testClient.HGET(key, field)
	if err != nil {
		t.Errorf("HGET %q got error %q", key, err)
	}
	if bytes != nil {
		t.Errorf("HGET %q got %q, want nil", key, bytes)
	}

	ok, err := testClient.HDEL(key, field)
	if err != nil {
		t.Errorf("HDEL %q got error %q", key, err)
	}
	if ok {
		t.Errorf("HDEL %q got true, want false", key)
	}

	key = "does exist"
	_, err = testClient.HSETString(key, "another field", "arbitrary")
	if err != nil {
		t.Fatal("hash creation error:", err)
	}

	bytes, err = testClient.HGET(key, field)
	if err != nil {
		t.Errorf("HGET %q got error %q", key, err)
	}
	if bytes != nil {
		t.Errorf("HGET %q got %q, want nil", key, bytes)
	}

	ok, err = testClient.HDEL(key, field)
	if err != nil {
		t.Errorf("HDEL %q got error %q", key, err)
	}
	if ok {
		t.Errorf("HDEL %q got true, want false", key)
	}
}

func BenchmarkSimpleString(b *testing.B) {
	const key = "bench-key"
	value := make([]byte, 8)
	b.Run("sequential", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			if err := testClient.SET(key, value); err != nil {
				b.Fatal("error:", err)
			}
		}
	})
	b.Run("parallel", func(b *testing.B) {
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				if err := testClient.SET(key, value); err != nil {
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
			if _, err := testClient.DEL(key); err != nil {
				b.Fatal("error:", err)
			}
		}
	})
	b.Run("parallel", func(b *testing.B) {
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				if _, err := testClient.DEL(key); err != nil {
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
			if err := testClient.SET(key, make([]byte, size)); err != nil {
				b.Fatal("population error:", err)
			}

			b.Run("sequential", func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					bytes, err := testClient.GET(key)
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
						bytes, err := testClient.GET(key)
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
