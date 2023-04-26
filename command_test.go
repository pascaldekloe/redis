package redis

import (
	"fmt"
	"reflect"
	"testing"
	"time"
)

func TestKeyCRUD(t *testing.T) {
	t.Parallel()
	key := randomKey("test-key")
	const value, update = "first-value", "second-value"

	if err := testClient.SET(key, value); err != nil {
		t.Fatalf("SET %q %q error: %s", key, value, err)
	}

	if v, err := testClient.GET(key); err != nil {
		t.Errorf("GET %q error: %s", key, err)
	} else if v != value {
		t.Errorf(`GET %q got %q, want %q`, key, v, value)
	}

	if err := testClient.SET(key, update); err != nil {
		t.Errorf("SET %q %q update error: %s", key, value, err)
	} else {
		if v, err := testClient.GET(key); err != nil {
			t.Errorf("GET %q error: %s", key, err)
		} else if v != update {
			t.Errorf(`GET %q got %q, want %q`, key, v, update)
		}
	}

	ok, err := testClient.DEL(key)
	if err != nil {
		t.Errorf("DEL %q error: %s", key, err)
	}
	if !ok {
		t.Errorf("DEL %q got false, want true", key)
	}
}

func TestBatchKeyCRUD(t *testing.T) {
	t.Parallel()
	key1, key2 := randomKey("test-key"), randomKey("test-key")
	value1, value2, value3 := "one", "", "x"

	if err := testClient.MSET([]string{key1, key2}, []string{value1, value2}); err != nil {
		t.Fatalf("MSET %q %q %q %q error: %s", key1, value1, key2, value2, err)
	}
	if err := testClient.MSET([]string{key1}, []string{value3}); err != nil {
		t.Fatalf("MSET %q %q error: %s", key1, value3, err)
	}

	absentKey := "doesn't exist"

	if values, err := testClient.MGET(key1, key2, absentKey); err != nil {
		t.Errorf("MGET %q %q %q error: %s", key1, key2, absentKey, err)
	} else if want := []string{value3, value2, ""}; !reflect.DeepEqual(values, want) {
		t.Errorf(`MGET %q %q %q got %q, want %q`, key1, key2, absentKey, values, want)
	}

	if n, err := testClient.DELArgs(key1, key2, absentKey); err != nil {
		t.Errorf("DEL %q %q %q error: %s", key1, key2, absentKey, err)
	} else if n != 2 {
		t.Errorf("DEL %q %q %q got %d, want 2", key1, key2, absentKey, n)
	}
}

func TestKeyAbsent(t *testing.T) {
	t.Parallel()
	const key = "doesn't exist"
	const key2 = "doesn't either"

	// byte slices nil on key absence
	byteClient := byteValueClient(t)

	s, err := testClient.GET(key)
	if err != nil {
		t.Errorf("GET %q error: %s", key, err)
	} else if s != "" {
		t.Errorf("GET %q got %q, want empty string", key, s)
	}

	bytes, err := byteClient.GET(key)
	if err != nil {
		t.Errorf("GET %q error: %s", key, err)
	} else if bytes != nil {
		t.Errorf("GET %q got %q, want nil", key, bytes)
	}

	ok, err := testClient.DEL(key)
	if err != nil {
		t.Errorf("DEL %q error: %s", key, err)
	} else if ok {
		t.Errorf("DEL %q got true, want false", key)
	}

	n, err := testClient.DELArgs(key, key2)
	if err != nil {
		t.Errorf("DEL %q %q error: %s", key, key2, err)
	} else if n != 0 {
		t.Errorf("DEL %q %q got %d, want 0", key, key2, n)
	}
}

func TestKeyModification(t *testing.T) {
	t.Parallel()
	key := randomKey("test")

	if n, err := testClient.INCR(key); err != nil {
		t.Errorf("INCR %q error: %s", key, err)
	} else if n != 1 {
		t.Errorf("INCR %q got %d, want 1", key, n)
	}
	if n, err := testClient.INCR(key); err != nil {
		t.Errorf("INCR %q error: %s", key, err)
	} else if n != 2 {
		t.Errorf("INCR %q got %d, want 2", key, n)
	}

	if n, err := testClient.INCRBY(key, 1e9); err != nil {
		t.Errorf("INCRBY %q 1000000000 error: %s", key, err)
	} else if n != 1000000002 {
		t.Errorf("INCRBY %q 1000000000 got %d, want 1000000002", key, n)
	}
	if n, err := testClient.INCRBY(key, -1e9); err != nil {
		t.Errorf("INCRBY %q -1000000000 error: %s", key, err)
	} else if n != 2 {
		t.Errorf("INCRBY %q -1000000000 got %d, want 2", key, n)
	}

	if newLen, err := testClient.APPEND(key, "a"); err != nil {
		t.Errorf(`APPEND %q "a" error: %s`, key, err)
	} else if newLen != 2 {
		t.Errorf(`APPEND %q "a" got %d, want 2`, key, newLen)
	}
	if newLen, err := testClient.APPEND(key, "b"); err != nil {
		t.Errorf(`APPEND %q "b" error: %s`, key, err)
	} else if newLen != 3 {
		t.Errorf(`APPEND %q "b" got %d, want 3`, key, newLen)
	}
	if newLen, err := testClient.APPEND(key, "c"); err != nil {
		t.Errorf(`APPEND %q "c" error: %s`, key, err)
	} else if newLen != 4 {
		t.Errorf(`APPEND %q "c" got %d, want 4`, key, newLen)
	}
}

func TestKeyOptions(t *testing.T) {
	t.Parallel()
	key := randomKey("test")

	if ok, err := testClient.SETWithOptions(key, "", SETOptions{Flags: XX}); err != nil {
		t.Fatalf(`SET %q "" XX error: %s`, key, err)
	} else if ok {
		t.Fatalf(`SET %q "" XX got true`, key)
	}

	if ok, err := testClient.SETWithOptions(key, "", SETOptions{Flags: PX, Expire: time.Millisecond}); err != nil {
		t.Fatalf(`SET %q "" PX 1 error: %s`, key, err)
	} else if !ok {
		t.Fatalf(`SET %q "" PX 1 got false`, key)
	}

	time.Sleep(20 * time.Millisecond)

	if ok, err := testClient.SETWithOptions(key, "value", SETOptions{Flags: NX}); err != nil {
		t.Errorf(`SET %q "value" "NX" error: %s`, key, err)
	} else if !ok {
		t.Errorf(`SET %q "value" "NX" got false`, key)
	}
}

func TestStrings(t *testing.T) {
	t.Parallel()
	key := randomKey("test")

	if err := testClient.SET(key, "abc"); err != nil {
		t.Errorf("INCR %q error: %s", key, err)
	}

	if l, err := testClient.STRLEN(key); err != nil {
		t.Errorf("STRLEN %q error: %s", key, err)
	} else if l != 3 {
		t.Errorf("STRLEN %q got %d, want 3", key, l)
	}
	if v, err := testClient.GETRANGE(key, 2, 3); err != nil {
		t.Errorf("GETRANGE %q 2 3 error: %s", key, err)
	} else if v != "c" {
		t.Errorf(`GETRANGE %q 2 3 got %q, want "c"`, key, v)
	}
	if v, err := testClient.GETRANGE(key, -3, -2); err != nil {
		t.Errorf("GETRANGE %q -3 -2 error: %s", key, err)
	} else if v != "ab" {
		t.Errorf(`GETRANGE %q -3 -2 got %q, want "ab"`, key, v)
	}
}

func TestStringsAbsent(t *testing.T) {
	t.Parallel()
	const key = "does not exist"

	// byte slices nil on key absence
	byteClient := byteValueClient(t)

	if l, err := testClient.STRLEN(key); err != nil {
		t.Errorf("STRLEN %q error: %s", key, err)
	} else if l != 0 {
		t.Errorf("STRLEN %q got %d, want 0", key, l)
	}
	if s, err := testClient.GETRANGE(key, 2, 3); err != nil {
		t.Errorf("GETRANGE %q 2 3 error: %s", key, err)
	} else if s != "" {
		t.Errorf("GETRANGE %q 2 3 got %q, want empty string", key, s)
	}
	if bytes, err := byteClient.GETRANGE(key, 2, 3); err != nil {
		t.Errorf("GETRANGE %q 2 3 error: %s", key, err)
	} else if bytes == nil {
		t.Errorf("GETRANGE %q 2 3 got nil, want empty", key)
	}
}

func TestListLeft(t *testing.T) {
	t.Parallel()
	key := randomKey("test-list")
	const minus, zero, one = "-", "zero", "one"

	if newLen, err := testClient.LPUSH(key, one); err != nil {
		t.Fatalf("LPUSH %q %q error: %s", key, one, err)
	} else if newLen != 1 {
		t.Errorf("LPUSH %q %q got len %d, want 1", key, one, newLen)
	}
	if newLen, err := testClient.LPUSH(key, zero); err != nil {
		t.Fatalf("LPUSH %q %q error: %s", key, zero, err)
	} else if newLen != 2 {
		t.Errorf("LPUSH %q %q got len %d, want 2", key, zero, newLen)
	}
	if newLen, err := testClient.LPUSH(key, minus); err != nil {
		t.Fatalf("LPUSH %q %q error: %s", key, minus, err)
	} else if newLen != 3 {
		t.Errorf("LPUSH %q %q got len %d, want 3", key, minus, newLen)
	}

	if values, err := testClient.LRANGE(key, 0, 2); err != nil {
		t.Errorf("LRANGE %q 0 2 error: %s", key, err)
	} else if len(values) != 3 || string(values[0]) != minus || string(values[1]) != zero || string(values[2]) != one {
		t.Fatalf("LRANGE %q 0 2 got %q, want [%q, %q, %q]", key, values, minus, zero, one)
	}

	if value, err := testClient.LPOP(key); err != nil {
		t.Errorf("LPOP %q error: %s", key, err)
	} else if value != minus {
		t.Errorf("LPOP %q got %q, want %q", key, value, minus)
	}
	if value, err := testClient.LPOP(key); err != nil {
		t.Errorf("LPOP %q error: %s", key, err)
	} else if value != zero {
		t.Errorf("LPOP %q got %q, want %q", key, value, zero)
	}

	if value, err := testClient.LLEN(key); err != nil {
		t.Errorf("LLEN %q error: %s", key, err)
	} else if value != 1 {
		t.Errorf("LLEN %q got %d, want 1", key, value)
	}
}

func TestListRight(t *testing.T) {
	t.Parallel()
	key := randomKey("test-list")
	const minus, zero, one = "-", "zero", "one"

	if newLen, err := testClient.RPUSH(key, one); err != nil {
		t.Fatalf("RPUSH %q %q error: %s", key, one, err)
	} else if newLen != 1 {
		t.Errorf("RPUSH %q %q got len %d, want 1", key, one, newLen)
	}
	if newLen, err := testClient.RPUSH(key, zero); err != nil {
		t.Fatalf("RPUSH %q %q error: %s", key, zero, err)
	} else if newLen != 2 {
		t.Errorf("RPUSH %q %q got len %d, want 2", key, zero, newLen)
	}
	if newLen, err := testClient.RPUSH(key, minus); err != nil {
		t.Fatalf("RPUSH %q %q error: %s", key, minus, err)
	} else if newLen != 3 {
		t.Errorf("RPUSH %q %q got len %d, want 3", key, minus, newLen)
	}

	if values, err := testClient.LRANGE(key, 0, 100); err != nil {
		t.Errorf("LRANGE %q 0 100 error: %s", key, err)
	} else if len(values) != 3 || string(values[0]) != one || string(values[1]) != zero || string(values[2]) != minus {
		t.Fatalf("LRANGE %q 0 100 got %q, want [%q, %q, %q]", key, values, one, zero, minus)
	}

	if value, err := testClient.RPOP(key); err != nil {
		t.Errorf("RPOP %q error: %s", key, err)
	} else if value != minus {
		t.Errorf("RPOP %q got %q, want %q", key, value, minus)
	}
	if value, err := testClient.RPOP(key); err != nil {
		t.Errorf("RPOP %q error: %s", key, err)
	} else if value != zero {
		t.Errorf("RPOP %q got %q, want %q", key, value, zero)
	}

	if value, err := testClient.LLEN(key); err != nil {
		t.Errorf("LLEN %q error: %s", key, err)
	} else if value != 1 {
		t.Errorf("LLEN %q got %d, want 1", key, value)
	}
}

func TestListAbsent(t *testing.T) {
	const key = "doesn't exist"

	// byte slices nil on key absence
	byteClient := byteValueClient(t)

	if n, err := testClient.LLEN(key); err != nil {
		t.Errorf("LLEN %q error: %s", key, err)
	} else if n != 0 {
		t.Errorf("LLEN %q got %d, want 0 for non-existing", key, n)
	}

	const noSuchKey = `redis: error message "ERR no such key"`
	if err := testClient.LSET(key, 1, ""); err == nil || err.Error() != noSuchKey {
		t.Errorf(`LSET %q 1 "" got error %q, want %q`, key, err, noSuchKey)
	}

	if values, err := testClient.LRANGE(key, 0, 10); err != nil {
		t.Errorf("LRANGE %q 0 10 error: %s", key, err)
	} else if len(values) != 0 {
		t.Errorf("LRANGE %q 0 10 got %q, want [ ]", key, values)
	}

	if s, err := testClient.LPOP(key); err != nil {
		t.Errorf("LPOP %q error: %s", key, err)
	} else if s != "" {
		t.Errorf("LPOP %q got %q, want empty string", key, s)
	}
	if bytes, err := byteClient.LPOP(key); err != nil {
		t.Errorf("LPOP %q error: %s", key, err)
	} else if bytes != nil {
		t.Errorf("LPOP %q got %q, want nil", key, bytes)
	}

	if s, err := testClient.RPOP(key); err != nil {
		t.Errorf("RPOP %q error: %s", key, err)
	} else if s != "" {
		t.Errorf("RPOP %q got %q, want empty string", key, s)
	}
	if bytes, err := byteClient.RPOP(key); err != nil {
		t.Errorf("RPOP %q error: %s", key, err)
	} else if bytes != nil {
		t.Errorf("RPOP %q got %q, want nil", key, bytes)
	}

	if err := testClient.LTRIM(key, 1, 1); err != nil {
		t.Errorf("LTRIM %q 1 1 error: %s", key, err)
	}
}

func TestListIndex(t *testing.T) {
	t.Parallel()
	key := randomKey("array")

	for _, value := range []string{"one", "two", "tree"} {
		_, err := testClient.RPUSH(key, value)
		if err != nil {
			t.Fatal("population error:", err)
		}
	}

	if err := testClient.LSET(key, 0, "1"); err != nil {
		t.Errorf(`LSET %q 0 "1" error: %s`, key, err)
	}
	if err := testClient.LSET(key, -2, "2"); err != nil {
		t.Errorf(`LSET %q -2 "2" error: %s`, key, err)
	}
	if err := testClient.LSET(key, 2, "3"); err != nil {
		t.Errorf(`LSET %q 2 "3" error: %s`, key, err)
	}

	switch err := testClient.LSET(key, 3, "x").(type) {
	case ServerError:
		if want := "ERR index out of range"; string(err) != want {
			t.Errorf("LSET got error %q, want %q", err, want)
		}
	default:
		t.Errorf("LSET out of range error %q, want a ServerError", err)
	}

	if value, err := testClient.LINDEX(key, -3); err != nil {
		t.Errorf(`LINDEX %q -3 error: %s`, key, err)
	} else if value != "1" {
		t.Errorf(`LINDEX %q -3 got %q, want "1"`, key, value)
	}
	if value, err := testClient.LINDEX(key, 1); err != nil {
		t.Errorf(`LINDEX %q 1 error: %s`, key, err)
	} else if value != "2" {
		t.Errorf(`LINDEX %q 1 got %q, want "2"`, key, value)
	}
	if value, err := testClient.LINDEX(key, -1); err != nil {
		t.Errorf(`LINDEX %q -1 error: %s`, key, err)
	} else if value != "3" {
		t.Errorf(`LINDEX %q -1 got %q, want "3"`, key, value)
	}

	if value, err := testClient.LINDEX(key, 3); err != nil {
		t.Errorf("LINDEX %q 3 error: %s", key, err)
	} else if value != "" {
		t.Errorf("LINDEX %q 3 got %q, want empty string", key, value)
	}
}

func TestListRemove(t *testing.T) {
	t.Parallel()
	key := randomKey("array")

	for _, value := range []string{"0", "1", "2", "3", "4", "5"} {
		_, err := testClient.RPUSH(key, value)
		if err != nil {
			t.Fatal("population error:", err)
		}
	}

	if err := testClient.LTRIM(key, 0, 4); err != nil {
		t.Errorf("LTRIM %q 0 4 error: %s", key, err)
	}
	if err := testClient.LTRIM(key, 1, -2); err != nil {
		t.Errorf("LTRIM %q 0 4 error: %s", key, err)
	}

	const want = `["1" "2" "3"]`
	if values, err := testClient.LRANGE(key, 0, -1); err != nil {
		t.Fatal("lookup error:", err)
	} else if got := fmt.Sprintf("%q", values); got != want {
		t.Fatalf("got %s, want %s", got, want)
	}
}

func TestHashCRUD(t *testing.T) {
	t.Parallel()
	key := randomKey("test-hash")
	const field, value, update = "field1", "first-value", "second-value"

	if newField, err := testClient.HSET(key, field, value); err != nil {
		t.Fatalf("HSET %q %q %q error: %s", key, field, value, err)
	} else if !newField {
		t.Errorf("HSET %q %q %q got newField false", key, field, value)
	}

	if v, err := testClient.HGET(key, field); err != nil {
		t.Errorf("HGET %q %q error: %s", key, field, err)
	} else if v != value {
		t.Errorf(`HGET %q %q got %q, want %q`, key, field, v, value)
	}

	if newField, err := testClient.HSET(key, field, update); err != nil {
		t.Errorf("HSET %q %q %q update error: %s", key, field, update, err)
	} else {
		if newField {
			t.Errorf("HSET %q %q %q update got newField true", key, field, value)
		}
		if v, err := testClient.HGET(key, field); err != nil {
			t.Errorf("HGET %q %q error: %s", key, field, err)
		} else if v != update {
			t.Errorf(`HGET %q %q got %q, want %q`, key, field, v, update)
		}
	}

	ok, err := testClient.HDEL(key, field)
	if err != nil {
		t.Errorf("HDEL %q %q error: %s", key, field, err)
	} else if !ok {
		t.Errorf("HDEL %q %q got false, want true", key, field)
	}
}

func TestBatchHashCRUD(t *testing.T) {
	t.Parallel()
	key := randomKey("test-hash")
	const field, value, update = "field1", "first-value", "second-value"

	if err := testClient.HMSET(key, []string{field}, []string{value}); err != nil {
		t.Fatalf("HMSET %q %q %q error: %s", key, field, value, err)
	}

	if values, err := testClient.HMGET(key, field); err != nil {
		t.Errorf("HMGET %q %q error: %s", key, field, err)
	} else if len(values) != 1 || values[0] != value {
		t.Errorf(`HMGET %q %q got %q, want %q`, key, field, values, value)
	}

	if err := testClient.HMSET(key, []string{field}, []string{update}); err != nil {
		t.Errorf("HMSET %q %q %q update error: %s", key, field, update, err)
	} else {
		if values, err := testClient.HMGET(key, field); err != nil {
			t.Errorf("HMGET %q %q error: %s", key, field, err)
		} else if len(values) != 1 || values[0] != update {
			t.Errorf(`HMGET %q %q got %q, want %q`, key, field, values, update)
		}
	}

	field2 := "doesn't exist"
	n, err := testClient.HDELArgs(key, field, field2)
	if err != nil {
		t.Errorf("HDEL %q %q %q error: %s", key, field, field2, err)
	} else if n != 1 {
		t.Errorf("HDEL %q %q %q got %d, want 1", key, field, field2, n)
	}
}

func TestHashAbsent(t *testing.T) {
	t.Parallel()
	var key, field = "doesn't exist", "also not set"

	// byte slices nil on key absence
	byteClient := byteValueClient(t)

	s, err := testClient.HGET(key, field)
	if err != nil {
		t.Errorf("HGET %q error: %s", key, err)
	} else if s != "" {
		t.Errorf("HGET %q got %q, want empty string", key, s)
	}
	bytes, err := byteClient.HGET(key, field)
	if err != nil {
		t.Errorf("HGET %q error: %s", key, err)
	} else if bytes != nil {
		t.Errorf("HGET %q got %q, want nil", key, bytes)
	}

	ok, err := testClient.HDEL(key, field)
	if err != nil {
		t.Errorf("HDEL %q error: %s", key, err)
	} else if ok {
		t.Errorf("HDEL %q got true, want false", key)
	}

	key = randomKey("does exist")
	_, err = testClient.HSET(key, "another field", "arbitrary")
	if err != nil {
		t.Fatal("hash creation error:", err)
	}

	s, err = testClient.HGET(key, field)
	if err != nil {
		t.Errorf("HGET %q error: %s", key, err)
	} else if s != "" {
		t.Errorf("HGET %q got %q, want empty string", key, s)
	}
	bytes, err = byteClient.HGET(key, field)
	if err != nil {
		t.Errorf("HGET %q error: %s", key, err)
	} else if bytes != nil {
		t.Errorf("HGET %q got %q, want nil", key, bytes)
	}

	ok, err = testClient.HDEL(key, field)
	if err != nil {
		t.Errorf("HDEL %q error: %s", key, err)
	} else if ok {
		t.Errorf("HDEL %q got true, want false", key)
	}
}
