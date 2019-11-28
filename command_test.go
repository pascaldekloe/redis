package redis

import (
	"fmt"
	"reflect"
	"testing"
	"time"
)

func TestDBSwitch(t *testing.T) {
	key, value := randomKey("test-key"), "✓"

	if err := testClient.SELECT(13); err != nil {
		t.Fatal("SELECT 13 error:", err)
	}
	if err := testClient.SETString(key, value); err != nil {
		t.Fatalf("SET %q %q in DB 13 error: %s", key, value, err)
	}
	if ok, err := testClient.MOVE(key, 14); err != nil {
		t.Errorf("MOVE %q 14 error: %s", key, err)
	} else if !ok {
		t.Errorf("MOVE %q 14 got false", key)
	}
	if bytes, err := testClient.GET(key); err != nil {
		t.Errorf("GET %q in DB 13 error: %s", key, err)
	} else if bytes != nil {
		t.Errorf("GET %q in DB 13 got %q, want nil", key, bytes)
	}
	if err := testClient.FLUSHDB(true); err != nil {
		t.Error("FLUSHDB ASYNC error:", err)
	}

	if err := testClient.SELECT(14); err != nil {
		t.Fatal("SELECT 14 error:", err)
	}
	if bytes, err := testClient.GET(key); err != nil {
		t.Errorf("GET %q in DB 14 error: %s", key, err)
	} else if string(bytes) != value {
		t.Errorf("GET %q in DB 14 got %q, want %q", key, bytes, value)
	}
	if err := testClient.FLUSHDB(false); err != nil {
		t.Fatal("FLUSHDB error:", err)
	}
	if ok, err := testClient.BytesMOVE([]byte(key), 13); err != nil {
		t.Errorf("MOVE %q 13 after FLUSHDB error: %s", key, err)
	} else if ok {
		t.Errorf("MOVE %q 13 after FLUSHDB got true", key)
	}
}

func TestKeyCRUD(t *testing.T) {
	t.Parallel()
	key := randomKey("test-key")
	const value, update = "first-value", "second-value"

	if err := testClient.SET(key, []byte(value)); err != nil {
		t.Fatalf("SET %q %q error: %s", key, value, err)
	}

	if bytes, err := testClient.GET(key); err != nil {
		t.Errorf("GET %q error: %s", key, err)
	} else if string(bytes) != value {
		t.Errorf(`GET %q got %q, want %q`, key, bytes, value)
	}

	if err := testClient.SETString(key, update); err != nil {
		t.Errorf("SET %q %q update error: %s", key, value, err)
	} else {
		if bytes, err := testClient.GET(key); err != nil {
			t.Errorf("GET %q error: %s", key, err)
		} else if string(bytes) != update {
			t.Errorf(`GET %q got %q, want %q`, key, bytes, update)
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

func TestBytesKeyCRUD(t *testing.T) {
	t.Parallel()
	key := []byte(randomKey("test-key"))
	value, update := []byte(""), []byte("second-value")

	if err := testClient.BytesSET(key, value); err != nil {
		t.Fatalf("SET %q %q error: %s", key, value, err)
	}

	if bytes, err := testClient.BytesGET(key); err != nil {
		t.Errorf("GET %q error: %s", key, err)
	} else if !reflect.DeepEqual(bytes, value) {
		t.Errorf(`GET %q got %q, want %q`, key, bytes, value)
	}

	if err := testClient.BytesSET(key, update); err != nil {
		t.Errorf("SET %q %q update error: %s", key, value, err)
	} else {
		if bytes, err := testClient.BytesGET(key); err != nil {
			t.Errorf("GET %q error: %s", key, err)
		} else if !reflect.DeepEqual(bytes, update) {
			t.Errorf(`GET %q got %q, want %q`, key, bytes, update)
		}
	}

	ok, err := testClient.BytesDEL(key)
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
	value1, value2, value3 := []byte("one"), []byte(""), []byte("x")

	if err := testClient.MSET([]string{key1, key2}, [][]byte{value1, value2}); err != nil {
		t.Fatalf("MSET %q %q %q %q error: %s", key1, value1, key2, value2, err)
	}
	if err := testClient.MSETString([]string{key1}, []string{string(value3)}); err != nil {
		t.Fatalf("MSET %q %q error: %s", key1, value3, err)
	}

	absentKey := "doesn't exist"

	if values, err := testClient.MGET(key1, key2, absentKey); err != nil {
		t.Errorf("MGET %q %q %q error: %s", key1, key2, absentKey, err)
	} else if want := [][]byte{value3, value2, nil}; !reflect.DeepEqual(values, want) {
		t.Errorf(`MGET %q %q %q got %q, want %q`, key1, key2, absentKey, values, want)
	}

	if n, err := testClient.DELArgs(key1, key2, absentKey); err != nil {
		t.Errorf("DEL %q %q %q error: %s", key1, key2, absentKey, err)
	} else if n != 2 {
		t.Errorf("DEL %q %q %q got %d, want 2", key1, key2, absentKey, n)
	}
}

func TestBatchBytesKeyCRD(t *testing.T) {
	t.Parallel()
	key1, key2 := []byte(randomKey("test-key")), []byte(randomKey("test-key"))
	value1, value2 := []byte("one"), []byte("")

	if err := testClient.BytesMSET([][]byte{key1, key2}, [][]byte{value1, value2}); err != nil {
		t.Fatalf("MSET %q %q %q %q error: %s", key1, value1, key2, value2, err)
	}

	absentKey := []byte("doesn't exist")

	if values, err := testClient.BytesMGET(key1, key2, absentKey); err != nil {
		t.Errorf("MGET %q %q %q error: %s", key1, key2, absentKey, err)
	} else if want := [][]byte{value1, value2, nil}; !reflect.DeepEqual(values, want) {
		t.Errorf(`MGET %q %q %q got %q, want %q`, key1, key2, absentKey, values, want)
	}

	if n, err := testClient.BytesDELArgs(key1, key2, absentKey); err != nil {
		t.Errorf("DEL %q %q %q error: %s", key1, key2, absentKey, err)
	} else if n != 2 {
		t.Errorf("DEL %q %q %q got %d, want 2", key1, key2, absentKey, n)
	}
}

func TestKeyAbsent(t *testing.T) {
	t.Parallel()
	const key = "doesn't exist"
	const key2 = "doesn't either"

	bytes, err := testClient.GET(key)
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
	if n, err := testClient.BytesINCR([]byte(key)); err != nil {
		t.Errorf("INCR %q error: %s", key, err)
	} else if n != 2 {
		t.Errorf("INCR %q got %d, want 2", key, n)
	}

	if n, err := testClient.INCRBY(key, 1e9); err != nil {
		t.Errorf("INCRBY %q 1000000000 error: %s", key, err)
	} else if n != 1000000002 {
		t.Errorf("INCRBY %q 1000000000 got %d, want 1000000002", key, n)
	}
	if n, err := testClient.BytesINCRBY([]byte(key), -1e9); err != nil {
		t.Errorf("INCRBY %q -1000000000 error: %s", key, err)
	} else if n != 2 {
		t.Errorf("INCRBY %q -1000000000 got %d, want 2", key, n)
	}

	if newLen, err := testClient.APPEND(key, []byte("a")); err != nil {
		t.Errorf(`APPEND %q "a" error: %s`, key, err)
	} else if newLen != 2 {
		t.Errorf(`APPEND %q "a" got %d, want 2`, key, newLen)
	}
	if newLen, err := testClient.APPENDString(key, "b"); err != nil {
		t.Errorf(`APPEND %q "b" error: %s`, key, err)
	} else if newLen != 3 {
		t.Errorf(`APPEND %q "b" got %d, want 3`, key, newLen)
	}
	if newLen, err := testClient.BytesAPPEND([]byte(key), []byte("c")); err != nil {
		t.Errorf(`APPEND %q "c" error: %s`, key, err)
	} else if newLen != 4 {
		t.Errorf(`APPEND %q "c" got %d, want 4`, key, newLen)
	}
}

func TestKeyOptions(t *testing.T) {
	t.Parallel()
	key := randomKey("test")

	if ok, err := testClient.BytesSETWithOptions([]byte(key), nil, SETOptions{Flags: XX}); err != nil {
		t.Fatalf(`SET %q "" XX error: %s`, key, err)
	} else if ok {
		t.Fatalf(`SET %q "" XX got true`, key)
	}

	if ok, err := testClient.SETWithOptions(key, nil, SETOptions{Flags: PX, Expire: time.Millisecond}); err != nil {
		t.Fatalf(`SET %q "" PX 1 error: %s`, key, err)
	} else if !ok {
		t.Fatalf(`SET %q "" PX 1 got false`, key)
	}

	time.Sleep(20 * time.Millisecond)

	if ok, err := testClient.SETStringWithOptions(key, "value", SETOptions{Flags: NX}); err != nil {
		t.Errorf(`SET %q "value" "NX" error: %s`, key, err)
	} else if !ok {
		t.Errorf(`SET %q "value" "NX" got false`, key)
	}
}

func TestStrings(t *testing.T) {
	t.Parallel()
	key := randomKey("test")

	if err := testClient.SETString(key, "abc"); err != nil {
		t.Errorf("INCR %q error: %s", key, err)
	}

	if l, err := testClient.STRLEN(key); err != nil {
		t.Errorf("STRLEN %q error: %s", key, err)
	} else if l != 3 {
		t.Errorf("STRLEN %q got %d, want 3", key, l)
	}
	if bytes, err := testClient.GETRANGE(key, 2, 3); err != nil {
		t.Errorf("GETRANGE %q 2 3 error: %s", key, err)
	} else if string(bytes) != "c" {
		t.Errorf(`GETRANGE %q 2 3 got %q, want "c"`, key, bytes)
	}
	if bytes, err := testClient.GETRANGE(key, -3, -2); err != nil {
		t.Errorf("GETRANGE %q -3 -2 error: %s", key, err)
	} else if string(bytes) != "ab" {
		t.Errorf(`GETRANGE %q -3 -2 got %q, want "ab"`, key, bytes)
	}
}

func TestStringsAbsent(t *testing.T) {
	t.Parallel()
	key := []byte("does not exist")

	if l, err := testClient.BytesSTRLEN(key); err != nil {
		t.Errorf("STRLEN %q error: %s", key, err)
	} else if l != 0 {
		t.Errorf("STRLEN %q got %d, want 0", key, l)
	}
	if bytes, err := testClient.BytesGETRANGE(key, 2, 3); err != nil {
		t.Errorf("GETRANGE %q 2 3 error: %s", key, err)
	} else if bytes == nil || len(bytes) != 0 {
		t.Errorf(`GETRANGE %q 2 3 got %q, want ""`, key, bytes)
	}
}

func TestListLeft(t *testing.T) {
	t.Parallel()
	key := randomKey("test-list")
	const minus, zero, one = "-", "zero", "one"

	if newLen, err := testClient.LPUSH(key, []byte(one)); err != nil {
		t.Fatalf("LPUSH %q %q error: %s", key, one, err)
	} else if newLen != 1 {
		t.Errorf("LPUSH %q %q got len %d, want 1", key, one, newLen)
	}
	if newLen, err := testClient.BytesLPUSH([]byte(key), []byte(zero)); err != nil {
		t.Fatalf("LPUSH %q %q error: %s", key, zero, err)
	} else if newLen != 2 {
		t.Errorf("LPUSH %q %q got len %d, want 2", key, zero, newLen)
	}
	if newLen, err := testClient.LPUSHString(key, minus); err != nil {
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
	} else if string(value) != minus {
		t.Errorf("LPOP %q got %q, want %q", key, value, minus)
	}
	if value, err := testClient.BytesLPOP([]byte(key)); err != nil {
		t.Errorf("LPOP %q error: %s", key, err)
	} else if string(value) != zero {
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

	if newLen, err := testClient.RPUSH(key, []byte(one)); err != nil {
		t.Fatalf("RPUSH %q %q error: %s", key, one, err)
	} else if newLen != 1 {
		t.Errorf("RPUSH %q %q got len %d, want 1", key, one, newLen)
	}
	if newLen, err := testClient.BytesRPUSH([]byte(key), []byte(zero)); err != nil {
		t.Fatalf("RPUSH %q %q error: %s", key, zero, err)
	} else if newLen != 2 {
		t.Errorf("RPUSH %q %q got len %d, want 2", key, zero, newLen)
	}
	if newLen, err := testClient.RPUSHString(key, minus); err != nil {
		t.Fatalf("RPUSH %q %q error: %s", key, minus, err)
	} else if newLen != 3 {
		t.Errorf("RPUSH %q %q got len %d, want 3", key, minus, newLen)
	}

	if values, err := testClient.BytesLRANGE([]byte(key), 0, 100); err != nil {
		t.Errorf("LRANGE %q 0 100 error: %s", key, err)
	} else if len(values) != 3 || string(values[0]) != one || string(values[1]) != zero || string(values[2]) != minus {
		t.Fatalf("LRANGE %q 0 100 got %q, want [%q, %q, %q]", key, values, one, zero, minus)
	}

	if value, err := testClient.RPOP(key); err != nil {
		t.Errorf("RPOP %q error: %s", key, err)
	} else if string(value) != minus {
		t.Errorf("RPOP %q got %q, want %q", key, value, minus)
	}
	if value, err := testClient.BytesRPOP([]byte(key)); err != nil {
		t.Errorf("RPOP %q error: %s", key, err)
	} else if string(value) != zero {
		t.Errorf("RPOP %q got %q, want %q", key, value, zero)
	}

	if value, err := testClient.BytesLLEN([]byte(key)); err != nil {
		t.Errorf("LLEN %q error: %s", key, err)
	} else if value != 1 {
		t.Errorf("LLEN %q got %d, want 1", key, value)
	}
}

func TestListAbsent(t *testing.T) {
	const key = "doesn't exist"

	if n, err := testClient.LLEN(key); err != nil {
		t.Errorf("LLEN %q error: %s", key, err)
	} else if n != 0 {
		t.Errorf("LLEN %q got %d, want 0 for non-existing", key, n)
	}

	const noSuchKey = `redis: server error "ERR no such key"`
	if err := testClient.LSET(key, 1, nil); err == nil || err.Error() != noSuchKey {
		t.Errorf(`LSET %q 1 "" got error %q, want %q`, key, err, noSuchKey)
	}

	if value, err := testClient.BytesLRANGE([]byte(key), 0, 10); err != nil {
		t.Errorf(`LRANGE %q 0 10 error: %s`, key, err)
	} else if value == nil {
		t.Errorf(`LRANGE %q 0 10 got nil, want empty`, key)
	} else if len(value) != 0 {
		t.Errorf(`LRANGE %q 0 10 got %q, want empty`, key, value)
	}

	if value, err := testClient.LPOP(key); err != nil {
		t.Errorf("LPOP %q error: %s", key, err)
	} else if value != nil {
		t.Errorf("LPOP %q got %q, want nil", key, value)
	}
	if value, err := testClient.RPOP(key); err != nil {
		t.Errorf("RPOP %q error: %s", key, err)
	} else if value != nil {
		t.Errorf("RPOP %q got %q, want nil", key, value)
	}

	if err := testClient.LTRIM(key, 1, 1); err != nil {
		t.Errorf("LTRIM %q 1 1 error: %s", key, err)
	}
}

func TestListIndex(t *testing.T) {
	t.Parallel()
	key := randomKey("array")

	for _, value := range []string{"one", "two", "tree"} {
		_, err := testClient.RPUSHString(key, value)
		if err != nil {
			t.Fatal("population error:", err)
		}
	}

	if err := testClient.LSET(key, 0, []byte{'1'}); err != nil {
		t.Errorf(`LSET %q 0 "1" error: %s`, key, err)
	}
	if err := testClient.LSETString(key, -2, "2"); err != nil {
		t.Errorf(`LSET %q -2 "2" error: %s`, key, err)
	}
	if err := testClient.BytesLSET([]byte(key), 2, []byte{'3'}); err != nil {
		t.Errorf(`LSET %q 2 "3" error: %s`, key, err)
	}

	switch err := testClient.LSET(key, 3, []byte{'x'}).(type) {
	case ServerError:
		if want := "ERR index out of range"; string(err) != want {
			t.Errorf("LSET got error %q, want %q", err, want)
		}
	default:
		t.Errorf("LSET out of range error %q, want ServerError", err)
	}

	if value, err := testClient.LINDEX(key, -3); err != nil {
		t.Errorf(`LINDEX %q -3 error: %s`, key, err)
	} else if string(value) != "1" {
		t.Errorf(`LINDEX %q -3 got %q, want "1"`, key, value)
	}
	if value, err := testClient.LINDEX(key, 1); err != nil {
		t.Errorf(`LINDEX %q 1 error: %s`, key, err)
	} else if string(value) != "2" {
		t.Errorf(`LINDEX %q 1 got %q, want "2"`, key, value)
	}
	if value, err := testClient.BytesLINDEX([]byte(key), -1); err != nil {
		t.Errorf(`LINDEX %q -1 error: %s`, key, err)
	} else if string(value) != "3" {
		t.Errorf(`LINDEX %q -1 got %q, want "3"`, key, value)
	}

	if value, err := testClient.LINDEX(key, 3); err != nil {
		t.Errorf("LINDEX %q 3 error: %s", key, err)
	} else if value != nil {
		t.Errorf("LINDEX %q 3 got %q, want nil", key, value)
	}
}

func TestListRemove(t *testing.T) {
	t.Parallel()
	key := randomKey("array")

	for _, value := range []string{"0", "1", "2", "3", "4", "5"} {
		_, err := testClient.RPUSHString(key, value)
		if err != nil {
			t.Fatal("population error:", err)
		}
	}

	if err := testClient.LTRIM(key, 0, 4); err != nil {
		t.Errorf("LTRIM %q 0 4 error: %s", key, err)
	}
	if err := testClient.BytesLTRIM([]byte(key), 1, -2); err != nil {
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

	if newField, err := testClient.HSET(key, field, []byte(value)); err != nil {
		t.Fatalf("HSET %q %q %q error: %s", key, field, value, err)
	} else if !newField {
		t.Errorf("HSET %q %q %q got newField false", key, field, value)
	}

	if bytes, err := testClient.HGET(key, field); err != nil {
		t.Errorf("HGET %q %q error: %s", key, field, err)
	} else if string(bytes) != value {
		t.Errorf(`HGET %q %q got %q, want %q`, key, field, bytes, value)
	}

	if newField, err := testClient.HSETString(key, field, update); err != nil {
		t.Errorf("HSET %q %q %q update error: %s", key, field, update, err)
	} else {
		if newField {
			t.Errorf("HSET %q %q %q update got newField true", key, field, value)
		}
		if bytes, err := testClient.HGET(key, field); err != nil {
			t.Errorf("HGET %q %q error: %s", key, field, err)
		} else if string(bytes) != update {
			t.Errorf(`HGET %q %q got %q, want %q`, key, field, bytes, update)
		}
	}

	ok, err := testClient.HDEL(key, field)
	if err != nil {
		t.Errorf("HDEL %q %q error: %s", key, field, err)
	} else if !ok {
		t.Errorf("HDEL %q %q got false, want true", key, field)
	}
}

func TestBytesHashCRUD(t *testing.T) {
	t.Parallel()
	key := []byte(randomKey("test-hash"))
	field, value, update := []byte("field1"), []byte(""), []byte("second-value")

	if newField, err := testClient.BytesHSET(key, field, value); err != nil {
		t.Fatalf("HSET %q %q %q error: %s", key, field, value, err)
	} else if !newField {
		t.Errorf("HSET %q %q %q got newField false", key, field, value)
	}

	if bytes, err := testClient.BytesHGET(key, field); err != nil {
		t.Errorf("HGET %q %q error: %s", key, field, err)
	} else if !reflect.DeepEqual(bytes, value) {
		t.Errorf(`HGET %q %q got %q, want %q`, key, field, bytes, value)
	}

	if newField, err := testClient.BytesHSET(key, field, update); err != nil {
		t.Errorf("HSET %q %q %q update error: %s", key, field, update, err)
	} else {
		if newField {
			t.Errorf("HSET %q %q %q update got newField true", key, field, value)
		}
		if bytes, err := testClient.BytesHGET(key, field); err != nil {
			t.Errorf("HGET %q %q error: %s", key, field, err)
		} else if !reflect.DeepEqual(bytes, update) {
			t.Errorf(`HGET %q %q got %q, want %q`, key, field, bytes, update)
		}
	}

	ok, err := testClient.BytesHDEL(key, field)
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

	if err := testClient.HMSET(key, []string{field}, [][]byte{[]byte(value)}); err != nil {
		t.Fatalf("HMSET %q %q %q error: %s", key, field, value, err)
	}

	if bytesValues, err := testClient.HMGET(key, field); err != nil {
		t.Errorf("HMGET %q %q error: %s", key, field, err)
	} else if len(bytesValues) != 1 || string(bytesValues[0]) != value {
		t.Errorf(`HMGET %q %q got %q, want %q`, key, field, bytesValues, value)
	}

	if err := testClient.HMSETString(key, []string{field}, []string{update}); err != nil {
		t.Errorf("HMSET %q %q %q update error: %s", key, field, update, err)
	} else {
		if bytesValues, err := testClient.HMGET(key, field); err != nil {
			t.Errorf("HMGET %q %q error: %s", key, field, err)
		} else if len(bytesValues) != 1 || string(bytesValues[0]) != update {
			t.Errorf(`HMGET %q %q got %q, want %q`, key, field, bytesValues, update)
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

func TestBytesBatchHashCRUD(t *testing.T) {
	t.Parallel()
	key := []byte(randomKey("test-hash"))
	field, value, update := []byte("field1"), []byte(""), []byte("second-value")

	if err := testClient.BytesHMSET(key, [][]byte{field}, [][]byte{[]byte(value)}); err != nil {
		t.Fatalf("HMSET %q %q %q error: %s", key, field, value, err)
	}

	if bytesValues, err := testClient.BytesHMGET(key, field); err != nil {
		t.Errorf("HMGET %q %q error: %s", key, field, err)
	} else if len(bytesValues) != 1 || string(bytesValues[0]) != string(value) {
		t.Errorf(`HMGET %q %q got %q, want %q`, key, field, bytesValues, value)
	}

	if err := testClient.BytesHMSET(key, [][]byte{field}, [][]byte{update}); err != nil {
		t.Errorf("HMSET %q %q %q update error: %s", key, field, update, err)
	} else {
		if bytesValues, err := testClient.BytesHMGET(key, field); err != nil {
			t.Errorf("HMGET %q %q error: %s", key, field, err)
		} else if len(bytesValues) != 1 || string(bytesValues[0]) != string(update) {
			t.Errorf(`HMGET %q %q got %q, want %q`, key, field, bytesValues, update)
		}
	}

	field2 := []byte("doesn't exist")
	n, err := testClient.BytesHDELArgs(key, field, field2)
	if err != nil {
		t.Errorf("HDEL %q %q %q error: %s", key, field, field2, err)
	} else if n != 1 {
		t.Errorf("HDEL %q %q %q got %d, want 1", key, field, field2, n)
	}
}

func TestHashAbsent(t *testing.T) {
	t.Parallel()
	key, field := "doesn't exist", "also not set"

	bytes, err := testClient.HGET(key, field)
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
	_, err = testClient.HSETString(key, "another field", "arbitrary")
	if err != nil {
		t.Fatal("hash creation error:", err)
	}

	bytes, err = testClient.HGET(key, field)
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
