package redis

import (
	"errors"
	"time"
)

// Flags For SETOptions.
const (
	// NX only sets the key if it does not already exist.
	NX = 1 << iota
	// XX only sets the key if it does already exist.
	XX

	// EX sets an expire time, in seconds.
	EX
	// PX sets an expire time, in milliseconds.
	PX
)

// EXPIRE flags include NX And XX.
const (
	// GT sets expiry only when the new expiry is greater than current one.
	GT = 32 << iota
	// LT sets expiry only when the new expiry is less than current one.
	LT
)

// SETOptions are extra arguments for the SET command.
type SETOptions struct {
	// Composotion of NX, XX, EX or PX. The combinations
	// (NX | XX) and (EX | PX) are rejected to prevent
	// mistakes.
	Flags uint

	// The value is truncated to seconds with the EX flag,
	// or milliseconds with PX. Non-zero values without any
	// expiry Flags are rejected to prevent mistakes.
	Expire time.Duration
}

// MOVE executes <https://redis.io/commands/move>.
func (c *Client[Key, Value]) MOVE(k Key, db int64) (bool, error) {
	n, err := c.commandInteger(requestWithStringAndDecimal("*3\r\n$4\r\nMOVE\r\n$", k, db))
	return n != 0, err
}

// FLUSHDB executes <https://redis.io/commands/flushdb>.
func (c *Client[Key, Value]) FLUSHDB(async bool) error {
	var r *request
	if async {
		r = requestFix("*2\r\n$7\r\nFLUSHDB\r\n$5\r\nASYNC\r\n")
	} else {
		r = requestFix("*1\r\n$7\r\nFLUSHDB\r\n")
	}
	return c.commandOK(r)
}

// EXPIRE executes <https://redis.io/commands/expire>.
// Flags can be any of NX, XX, GT or LT.
func (c *Client[Key, Value]) EXPIRE(k Key, seconds int64, flags uint) (bool, error) {
	if unknown := flags &^ (NX | XX | GT | LT); unknown != 0 {
		return false, errors.New("redis: unknown EXPIRE flags")
	}

	var n int64
	var err error
	switch flags {
	case 0:
		n, err = c.commandInteger(requestWithStringAndDecimal("*3\r\n$6\r\nEXPIRE\r\n$", k, seconds))
	case NX:
		n, err = c.commandInteger(requestWithStringAndDecimalAndString("*4\r\n$6\r\nEXPIRE\r\n$", k, seconds, "NX"))
	case XX:
		n, err = c.commandInteger(requestWithStringAndDecimalAndString("*4\r\n$6\r\nEXPIRE\r\n$", k, seconds, "XX"))
	case GT:
		n, err = c.commandInteger(requestWithStringAndDecimalAndString("*4\r\n$6\r\nEXPIRE\r\n$", k, seconds, "GT"))
	case LT:
		n, err = c.commandInteger(requestWithStringAndDecimalAndString("*4\r\n$6\r\nEXPIRE\r\n$", k, seconds, "LT"))
	default:
		return false, errors.New("redis: multiple EXPIRE flags denied")
	}
	return n != 0, err
}

// FLUSHALL executes <https://redis.io/commands/flushall>.
func (c *Client[Key, Value]) FLUSHALL(async bool) error {
	var r *request
	if async {
		r = requestFix("*2\r\n$8\r\nFLUSHALL\r\n$5\r\nASYNC\r\n")
	} else {
		r = requestFix("*1\r\n$8\r\nFLUSHALL\r\n")
	}
	return c.commandOK(r)
}

// GET executes <https://redis.io/commands/get>.
// The return is zero if the Key does not exist.
func (c *Client[Key, Value]) GET(k Key) (Value, error) {
	return c.commandBulk(requestWithString("*2\r\n$3\r\nGET\r\n$", k))
}

// MGET executes <https://redis.io/commands/mget>.
// The Values for non-existing Keys stay zero.
func (c *Client[Key, Value]) MGET(m ...Key) ([]Value, error) {
	return c.commandArray(requestWithList("\r\n$4\r\nMGET", m))
}

// SET executes <https://redis.io/commands/set>.
func (c *Client[Key, Value]) SET(k Key, v Value) error {
	return c.commandOK(requestWith2Strings("*3\r\n$3\r\nSET\r\n$", k, v))
}

// SETWithOptions executes <https://redis.io/commands/set> with options.
// The return is false if the SET operation was not performed due to an NX or XX
// condition.
func (c *Client[Key, Value]) SETWithOptions(k Key, v Value, o SETOptions) (bool, error) {
	if unknown := o.Flags &^ (NX | XX | EX | PX); unknown != 0 {
		return false, errors.New("redis: unknown SET flags")
	}

	var existArg string
	switch o.Flags & (NX | XX) {
	case 0:
		break
	case NX:
		existArg = "NX"
	case XX:
		existArg = "XX"
	default:
		return false, errors.New("redis: combination of NX and XX not allowed")
	}

	var expireArg string
	var expire int64
	switch o.Flags & (EX | PX) {
	case 0:
		if o.Expire != 0 {
			return false, errors.New("redis: expire time without EX or PX not allowed")
		}
	case EX:
		expireArg = "EX"
		expire = int64(o.Expire / time.Second)
	case PX:
		expireArg = "PX"
		expire = int64(o.Expire / time.Millisecond)
	default:
		return false, errors.New("redis: combination of EX and PX not allowed")
	}

	var r *request
	switch {
	case existArg != "" && expireArg == "":
		r = requestWith3Strings("*4\r\n$3\r\nSET\r\n$", k, v, existArg)
	case existArg == "" && expireArg != "":
		r = requestWith3StringsAndDecimal("*5\r\n$3\r\nSET\r\n$", k, v, expireArg, expire)
	case existArg != "" && expireArg != "":
		r = requestWith4StringsAndDecimal("*6\r\n$3\r\nSET\r\n$", k, v, existArg, expireArg, expire)
	default:
		err := c.SET(k, v)
		return err == nil, err
	}

	err := c.commandOK(r)
	if err == errNull {
		return false, nil
	}
	return err == nil, err
}

// MSET executes <https://redis.io/commands/mset>.
func (c *Client[Key, Value]) MSET(mk []Key, mv []Value) error {
	r, err := requestWithMap("\r\n$4\r\nMSET", mk, mv)
	if err != nil {
		return err
	}
	return c.commandOK(r)
}

// DEL executes <https://redis.io/commands/del>.
func (c *Client[Key, Value]) DEL(k Key) (bool, error) {
	removed, err := c.commandInteger(requestWithString("*2\r\n$3\r\nDEL\r\n$", k))
	return removed != 0, err
}

// DELArgs executes <https://redis.io/commands/del>.
func (c *Client[Key, Value]) DELArgs(m ...Key) (int64, error) {
	return c.commandInteger(requestWithList("\r\n$3\r\nDEL", m))
}

// INCR executes <https://redis.io/commands/incr>.
func (c *Client[Key, Value]) INCR(k Key) (newValue int64, err error) {
	return c.commandInteger(requestWithString("*2\r\n$4\r\nINCR\r\n$", k))
}

// INCRBY executes <https://redis.io/commands/incrby>.
func (c *Client[Key, Value]) INCRBY(k Key, increment int64) (newValue int64, err error) {
	return c.commandInteger(requestWithStringAndDecimal("*3\r\n$6\r\nINCRBY\r\n$", k, increment))
}

// STRLEN executes <https://redis.io/commands/strlen>.
func (c *Client[Key, Value]) STRLEN(k Key) (int64, error) {
	return c.commandInteger(requestWithString("*2\r\n$6\r\nSTRLEN\r\n$", k))
}

// GETRANGE executes <https://redis.io/commands/getrange>.
// The return is empty if the Key does not exist.
func (c *Client[Key, Value]) GETRANGE(k Key, start, end int64) (Value, error) {
	return c.commandBulk(requestWithStringAnd2Decimals("*4\r\n$8\r\nGETRANGE\r\n$", k, start, end))
}

// APPEND executes <https://redis.io/commands/append>.
func (c *Client[Key, Value]) APPEND(k Key, v Value) (newLen int64, err error) {
	return c.commandInteger(requestWith2Strings("*3\r\n$6\r\nAPPEND\r\n$", k, v))
}

// LLEN executes <https://redis.io/commands/llen>.
// The return is 0 if the Key does not exist.
func (c *Client[Key, Value]) LLEN(k Key) (int64, error) {
	return c.commandInteger(requestWithString("*2\r\n$4\r\nLLEN\r\n$", k))
}

// LINDEX executes <https://redis.io/commands/lindex>.
// The return is zero if the Key does not exist.
// The return is zero if index is out of range.
func (c *Client[Key, Value]) LINDEX(k Key, index int64) (Value, error) {
	return c.commandBulk(requestWithStringAndDecimal("*3\r\n$6\r\nLINDEX\r\n$", k, index))
}

// LRANGE executes <https://redis.io/commands/lrange>.
// The return is empty if the Key does not exist.
func (c *Client[Key, Value]) LRANGE(k Key, start, stop int64) ([]Value, error) {
	return c.commandArray(requestWithStringAnd2Decimals("*4\r\n$6\r\nLRANGE\r\n$", k, start, stop))
}

// LPOP executes <https://redis.io/commands/lpop>.
// The return is zero if the Key does not exist.
func (c *Client[Key, Value]) LPOP(k Key) (Value, error) {
	return c.commandBulk(requestWithString("*2\r\n$4\r\nLPOP\r\n$", k))
}

// RPOP executes <https://redis.io/commands/rpop>.
// The return is zero if the Key does not exist.
func (c *Client[Key, Value]) RPOP(k Key) (Value, error) {
	return c.commandBulk(requestWithString("*2\r\n$4\r\nRPOP\r\n$", k))
}

// LTRIM executes <https://redis.io/commands/ltrim>.
func (c *Client[Key, Value]) LTRIM(k Key, start, stop int64) error {
	return c.commandOK(requestWithStringAnd2Decimals("*4\r\n$5\r\nLTRIM\r\n$", k, start, stop))
}

// LSET executes <https://redis.io/commands/lset>.
func (c *Client[Key, Value]) LSET(k Key, index int64, value Value) error {
	return c.commandOK(requestWithStringAndDecimalAndString("*4\r\n$4\r\nLSET\r\n$", k, index, value))
}

// LPUSH executes <https://redis.io/commands/lpush>.
func (c *Client[Key, Value]) LPUSH(k Key, v Value) (newLen int64, err error) {
	return c.commandInteger(requestWith2Strings("*3\r\n$5\r\nLPUSH\r\n$", k, v))
}

// RPUSH executes <https://redis.io/commands/rpush>.
func (c *Client[Key, Value]) RPUSH(k Key, v Value) (newLen int64, err error) {
	return c.commandInteger(requestWith2Strings("*3\r\n$5\r\nRPUSH\r\n$", k, v))
}

// SCARD executes <https://redis.io/commands/scard>.
func (c *Client[Key, Value]) SCARD(k Key) (int64, error) {
	return c.commandInteger(requestWithString("*2\r\n$5\r\nSCARD\r\n$", k))
}

// SADD executes <https://redis.io/commands/sadd>.
func (c *Client[Key, Value]) SADD(k, m Key) (bool, error) {
	n, err := c.commandInteger(requestWith2Strings("*3\r\n$4\r\nSADD\r\n$", k, m))
	return n != 0, err
}

// SADD executes <https://redis.io/commands/sadd>.
func (c *Client[Key, Value]) SADDArgs(k Key, m ...Key) (int64, error) {
	return c.commandInteger(requestWithStringAndList("\r\n$4\r\nSADD\r\n$", k, m))
}

// SREM executes <https://redis.io/commands/srem>.
func (c *Client[Key, Value]) SREM(k, m Key) (bool, error) {
	n, err := c.commandInteger(requestWith2Strings("*3\r\n$4\r\nSREM\r\n$", k, m))
	return n != 0, err
}

// SREM executes <https://redis.io/commands/srem>.
func (c *Client[Key, Value]) SREMArgs(k Key, m ...Key) (int64, error) {
	return c.commandInteger(requestWithStringAndList("\r\n$4\r\nSREM\r\n$", k, m))
}

// SMEMBERS executes <https://redis.io/commands/smembers>.
func (c *Client[Key, Value]) SMEMBERS(k Key) ([]Value, error) {
	return c.commandArray(requestWithString("*2\r\n$8\r\nSMEMBERS\r\n$", k))
}

// SINTER executes <https://redis.io/commands/sinter>.
func (c *Client[Key, Value]) SINTER(k ...Key) ([]Value, error) {
	return c.commandArray(requestWithList("\r\n$6\r\nSINTER", k))
}

// SUNION executes <https://redis.io/commands/sunion>.
func (c *Client[Key, Value]) SUNION(k ...Key) ([]Value, error) {
	return c.commandArray(requestWithList("\r\n$6\r\nSUNION", k))
}

// HGET executes <https://redis.io/commands/hget>.
// The return is zero if the Key does not exist.
func (c *Client[Key, Value]) HGET(k, f Key) (Value, error) {
	return c.commandBulk(requestWith2Strings("*3\r\n$4\r\nHGET\r\n$", k, f))
}

// HSET executes <https://redis.io/commands/hset>.
func (c *Client[Key, Value]) HSET(k, f Key, v Value) (newField bool, err error) {
	created, err := c.commandInteger(requestWith3Strings("*4\r\n$4\r\nHSET\r\n$", k, f, v))
	return created != 0, err
}

// HDEL executes <https://redis.io/commands/hdel>.
func (c *Client[Key, Value]) HDEL(k, f Key) (bool, error) {
	removed, err := c.commandInteger(requestWith2Strings("*3\r\n$4\r\nHDEL\r\n$", k, f))
	return removed != 0, err
}

// HDELArgs executes <https://redis.io/commands/hdel>.
func (c *Client[Key, Value]) HDELArgs(k Key, mf ...Key) (int64, error) {
	return c.commandInteger(requestWithStringAndList("\r\n$4\r\nHDEL\r\n$", k, mf))
}

// HMGET executes <https://redis.io/commands/hmget>.
// The Values for non-existing Keys stay zero.
func (c *Client[Key, Value]) HMGET(k Key, mf ...Key) ([]Value, error) {
	return c.commandArray(requestWithStringAndList("\r\n$5\r\nHMGET\r\n$", k, mf))
}

// HMSET executes <https://redis.io/commands/hmset>.
func (c *Client[Key, Value]) HMSET(k Key, mf []Key, mv []Value) error {
	r, err := requestWithStringAndMap("\r\n$5\r\nHMSET\r\n$", k, mf, mv)
	if err != nil {
		return err
	}
	return c.commandOK(r)
}
