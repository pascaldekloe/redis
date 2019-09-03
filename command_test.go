package redis

import "testing"

func TestKeyCRUD(t *testing.T) {
	t.Parallel()
	key := randomKey("test-key")
	const value, update = "first-value", "second-value"

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
	t.Parallel()
	key := []byte(randomKey("test-key"))
	value, update := []byte("first-value"), []byte("second-value")

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
	t.Parallel()
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
	t.Parallel()
	key := randomKey("test-list")
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

	if values, err := testClient.LRANGE(key, 0, 2); err != nil {
		t.Errorf("LRANGE %q 0 2 got error %q", key, err)
	} else if len(values) != 3 || string(values[0]) != minus || string(values[1]) != zero || string(values[2]) != one {
		t.Fatalf("LRANGE %q 0 2 got %q, want [%q, %q, %q]", key, values, minus, zero, one)
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
	t.Parallel()
	key := randomKey("test-list")
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

	if values, err := testClient.LRANGE(key, 0, 100); err != nil {
		t.Errorf("LRANGE %q 0 100 got error %q", key, err)
	} else if len(values) != 3 || string(values[0]) != one || string(values[1]) != zero || string(values[2]) != minus {
		t.Fatalf("LRANGE %q 0 100 got %q, want [%q, %q, %q]", key, values, one, zero, minus)
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

	if value, err := testClient.BytesLRANGE([]byte(key), 0, 10); err != nil {
		t.Errorf(`LRANGE %q 0 10 got error %q`, key, err)
	} else if value == nil {
		t.Errorf(`LRANGE %q 0 10 got nil, want empty`, key)
	} else if len(value) != 0 {
		t.Errorf(`LRANGE %q 0 10 got %q, want empty`, key, value)
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
	t.Parallel()
	key := randomKey("test-hash")
	const field, value, update = "field1", "first-value", "second-value"

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
	t.Parallel()
	key := []byte(randomKey("test-hash"))
	field, value, update := []byte("field1"), []byte("first-value"), []byte("second-value")

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
	t.Parallel()
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

	key = randomKey("does exist")
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
