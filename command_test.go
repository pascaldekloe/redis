package redis

import (
	"fmt"
	"log"
	"os"
	"testing"
)

var testClient *Client

func TestMain(m *testing.M) {
	addr := os.Getenv("TEST_REDIS_ADDR")
	if addr == "" {
		log.Fatal("Need TEST_REDIS_ADDR evironment variable with address of test server.\nCAUTION! Tests insert, modify and delete data.")
	}

	testClient = NewClient(addr)

	os.Exit(m.Run())
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

func TestKeysAbsent(t *testing.T) {
	const key = "doesn't exist"

	bytes, err := testClient.GET(key)
	if err != ErrNull {
		t.Errorf("GET %q got error %q, want %q", key, err, ErrNull)
	}
	if len(bytes) != 0 {
		t.Errorf("GET %q got %q", key, bytes)
	}

	ok, err := testClient.DEL(key)
	if err != nil {
		t.Errorf("DEL %q got error %q", key, err)
	}
	if ok {
		t.Errorf("DEL %q got true, want false", key)
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
