package redis

import (
	"errors"
	"fmt"
	"sync/atomic"
	"time"
)

// Flags For SETOptions
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

func (o *SETOptions) args() (existArg, expireArg string, expire int64, err error) {
	if unknown := o.Flags &^ (NX | XX | EX | PX); unknown != 0 {
		return "", "", 0, fmt.Errorf("redis: unknown flags %#x", unknown)
	}

	switch o.Flags & (NX | XX) {
	case 0:
		break
	case NX:
		existArg = "NX"
	case XX:
		existArg = "XX"
	default:
		return "", "", 0, errors.New("redis: combination of NX and XX not allowed")
	}

	switch o.Flags & (EX | PX) {
	case 0:
		if o.Expire != 0 {
			return "", "", 0, errors.New("redis: expire time without EX nor PX not allowed")
		}
	case EX:
		expireArg = "EX"
		expire = int64(o.Expire / time.Second)
	case PX:
		expireArg = "PX"
		expire = int64(o.Expire / time.Millisecond)
	default:
		return "", "", 0, errors.New("redis: combination of EX and PX not allowed")
	}
	return
}

// SELECT executes <https://redis.io/commands/select> in a persistent way, even
// when the return is in error. Any following command executions apply to this
// database selection, reconnects included.
//
// Deprecated: Use ClientConfig.DB instead. The SELECT method has unintuitive
// behaviour on error scenario.
func (c *Client) SELECT(db int64) error {
	atomic.StoreInt64(&c.config.DB, db)
	return c.commandOKOrReconnect(requestWithDecimal("*2\r\n$6\r\nSELECT\r\n$", db))
}

// MOVE executes <https://redis.io/commands/move>.
func (c *Client) MOVE(key string, db int64) (bool, error) {
	n, err := c.commandInteger(requestWithStringAndDecimal("*3\r\n$4\r\nMOVE\r\n$", key, db))
	return n != 0, err
}

// BytesMOVE executes <https://redis.io/commands/move>.
func (c *Client) BytesMOVE(key []byte, db int64) (bool, error) {
	n, err := c.commandInteger(requestWithStringAndDecimal("*3\r\n$4\r\nMOVE\r\n$", key, db))
	return n != 0, err
}

// FLUSHDB executes <https://redis.io/commands/flushdb>.
func (c *Client) FLUSHDB(async bool) error {
	var r *request
	if async {
		r = requestFix("*2\r\n$7\r\nFLUSHDB\r\n$5\r\nASYNC\r\n")
	} else {
		r = requestFix("*1\r\n$7\r\nFLUSHDB\r\n")
	}
	return c.commandOK(r)
}

// FLUSHALL executes <https://redis.io/commands/flushall>.
func (c *Client) FLUSHALL(async bool) error {
	var r *request
	if async {
		r = requestFix("*2\r\n$8\r\nFLUSHALL\r\n$5\r\nASYNC\r\n")
	} else {
		r = requestFix("*1\r\n$8\r\nFLUSHALL\r\n")
	}
	return c.commandOK(r)
}

// GET executes <https://redis.io/commands/get>.
// The return is nil if key does not exist.
func (c *Client) GET(key string) (value []byte, err error) {
	return c.commandBulkBytes(requestWithString("*2\r\n$3\r\nGET\r\n$", key))
}

// GETString executes <https://redis.io/commands/get>.
// Boolean ok is false if key does not exist.
func (c *Client) GETString(key string) (value string, ok bool, err error) {
	return c.commandBulkString(requestWithString("*2\r\n$3\r\nGET\r\n$", key))
}

// BytesGET executes <https://redis.io/commands/get>.
// The return is nil if key does not exist.
func (c *Client) BytesGET(key []byte) (value []byte, err error) {
	return c.commandBulkBytes(requestWithString("*2\r\n$3\r\nGET\r\n$", key))
}

// MGET executes <https://redis.io/commands/mget>.
// For every key that does not exist, a nil value is returned.
func (c *Client) MGET(keys ...string) (values [][]byte, err error) {
	return c.commandBytesArray(requestWithList("\r\n$4\r\nMGET", keys))
}

// MGETString executes <https://redis.io/commands/mget>.
// For every key that does not exist, an empty string is returned.
func (c *Client) MGETString(keys ...string) (values []string, err error) {
	return c.commandStringArray(requestWithList("\r\n$4\r\nMGET", keys))
}

// BytesMGET executes <https://redis.io/commands/mget>.
// For every key that does not exist, a nil value is returned.
func (c *Client) BytesMGET(keys ...[]byte) (values [][]byte, err error) {
	return c.commandBytesArray(requestWithList("\r\n$4\r\nMGET", keys))
}

// SET executes <https://redis.io/commands/set>.
func (c *Client) SET(key string, value []byte) error {
	return c.commandOK(requestWith2Strings("*3\r\n$3\r\nSET\r\n$", key, value))
}

// BytesSET executes <https://redis.io/commands/set>.
func (c *Client) BytesSET(key, value []byte) error {
	return c.commandOK(requestWith2Strings("*3\r\n$3\r\nSET\r\n$", key, value))
}

// SETString executes <https://redis.io/commands/set>.
func (c *Client) SETString(key, value string) error {
	return c.commandOK(requestWith2Strings("*3\r\n$3\r\nSET\r\n$", key, value))
}

// SETWithOptions executes <https://redis.io/commands/set> with options.
// The return is false if the SET operation was not performed due to an NX or XX
// condition.
func (c *Client) SETWithOptions(key string, value []byte, o SETOptions) (bool, error) {
	existArg, expireArg, expire, err := o.args()
	if err != nil {
		return false, err
	}

	var r *request
	switch {
	case existArg != "" && expireArg == "":
		r = requestWith3Strings("*4\r\n$3\r\nSET\r\n$", key, value, existArg)
	case existArg == "" && expireArg != "":
		r = requestWith3StringsAndDecimal("*5\r\n$3\r\nSET\r\n$", key, value, expireArg, expire)
	case existArg != "" && expireArg != "":
		r = requestWith4StringsAndDecimal("*6\r\n$3\r\nSET\r\n$", key, value, existArg, expireArg, expire)
	default:
		err := c.SET(key, value)
		return err == nil, err
	}

	err = c.commandOK(r)
	if err == errNull {
		return false, nil
	}
	return err == nil, err
}

// BytesSETWithOptions executes <https://redis.io/commands/set> with options.
// The return is false if the SET operation was not performed due to an NX or XX
// condition.
func (c *Client) BytesSETWithOptions(key, value []byte, o SETOptions) (bool, error) {
	existArg, expireArg, expire, err := o.args()
	if err != nil {
		return false, err
	}

	var r *request
	switch {
	case existArg != "" && expireArg == "":
		r = requestWith3Strings("*4\r\n$3\r\nSET\r\n$", key, value, existArg)
	case existArg == "" && expireArg != "":
		r = requestWith3StringsAndDecimal("*5\r\n$3\r\nSET\r\n$", key, value, expireArg, expire)
	case existArg != "" && expireArg != "":
		r = requestWith4StringsAndDecimal("*6\r\n$3\r\nSET\r\n$", key, value, existArg, expireArg, expire)
	default:
		err := c.BytesSET(key, value)
		return err == nil, err
	}

	err = c.commandOK(r)
	if err == errNull {
		return false, nil
	}
	return err == nil, err
}

// SETStringWithOptions executes <https://redis.io/commands/set> with options.
// The return is false if the SET operation was not performed due to an NX or XX
// condition.
func (c *Client) SETStringWithOptions(key, value string, o SETOptions) (bool, error) {
	existArg, expireArg, expire, err := o.args()
	if err != nil {
		return false, err
	}

	var r *request
	switch {
	case existArg != "" && expireArg == "":
		r = requestWith3Strings("*4\r\n$3\r\nSET\r\n$", key, value, existArg)
	case existArg == "" && expireArg != "":
		r = requestWith3StringsAndDecimal("*5\r\n$3\r\nSET\r\n$", key, value, expireArg, expire)
	case existArg != "" && expireArg != "":
		r = requestWith4StringsAndDecimal("*6\r\n$3\r\nSET\r\n$", key, value, existArg, expireArg, expire)
	default:
		err := c.SETString(key, value)
		return err == nil, err
	}

	err = c.commandOK(r)
	if err == errNull {
		return false, nil
	}
	return err == nil, err
}

// MSET executes <https://redis.io/commands/mset>.
func (c *Client) MSET(keys []string, values [][]byte) error {
	r, err := requestWithMap("\r\n$4\r\nMSET", keys, values)
	if err != nil {
		return err
	}
	return c.commandOK(r)
}

// BytesMSET executes <https://redis.io/commands/mset>.
func (c *Client) BytesMSET(keys, values [][]byte) error {
	r, err := requestWithMap("\r\n$4\r\nMSET", keys, values)
	if err != nil {
		return err
	}
	return c.commandOK(r)
}

// MSETString executes <https://redis.io/commands/mset>.
func (c *Client) MSETString(keys, values []string) error {
	r, err := requestWithMap("\r\n$4\r\nMSET", keys, values)
	if err != nil {
		return err
	}
	return c.commandOK(r)
}

// DEL executes <https://redis.io/commands/del>.
func (c *Client) DEL(key string) (bool, error) {
	removed, err := c.commandInteger(requestWithString("*2\r\n$3\r\nDEL\r\n$", key))
	return removed != 0, err
}

// DELArgs executes <https://redis.io/commands/del>.
func (c *Client) DELArgs(keys ...string) (int64, error) {
	return c.commandInteger(requestWithList("\r\n$3\r\nDEL", keys))
}

// BytesDEL executes <https://redis.io/commands/del>.
func (c *Client) BytesDEL(key []byte) (bool, error) {
	removed, err := c.commandInteger(requestWithString("*2\r\n$3\r\nDEL\r\n$", key))
	return removed != 0, err
}

// BytesDELArgs executes <https://redis.io/commands/del>.
func (c *Client) BytesDELArgs(keys ...[]byte) (int64, error) {
	return c.commandInteger(requestWithList("\r\n$3\r\nDEL", keys))
}

// INCR executes <https://redis.io/commands/incr>.
func (c *Client) INCR(key string) (newValue int64, err error) {
	return c.commandInteger(requestWithString("*2\r\n$4\r\nINCR\r\n$", key))
}

// BytesINCR executes <https://redis.io/commands/incr>.
func (c *Client) BytesINCR(key []byte) (newValue int64, err error) {
	return c.commandInteger(requestWithString("*2\r\n$4\r\nINCR\r\n$", key))
}

// INCRBY executes <https://redis.io/commands/incrby>.
func (c *Client) INCRBY(key string, increment int64) (newValue int64, err error) {
	return c.commandInteger(requestWithStringAndDecimal("*3\r\n$6\r\nINCRBY\r\n$", key, increment))
}

// BytesINCRBY executes <https://redis.io/commands/incrby>.
func (c *Client) BytesINCRBY(key []byte, increment int64) (newValue int64, err error) {
	return c.commandInteger(requestWithStringAndDecimal("*3\r\n$6\r\nINCRBY\r\n$", key, increment))
}

// STRLEN executes <https://redis.io/commands/strlen>.
func (c *Client) STRLEN(key string) (int64, error) {
	return c.commandInteger(requestWithString("*2\r\n$6\r\nSTRLEN\r\n$", key))
}

// BytesSTRLEN executes <https://redis.io/commands/strlen>.
func (c *Client) BytesSTRLEN(key []byte) (int64, error) {
	return c.commandInteger(requestWithString("*2\r\n$6\r\nSTRLEN\r\n$", key))
}

// GETRANGE executes <https://redis.io/commands/getrange>.
func (c *Client) GETRANGE(key string, start, end int64) ([]byte, error) {
	return c.commandBulkBytes(requestWithStringAnd2Decimals("*4\r\n$8\r\nGETRANGE\r\n$", key, start, end))
}

// GETRANGEString executes <https://redis.io/commands/getrange>.
func (c *Client) GETRANGEString(key string, start, end int64) (string, error) {
	s, _, err := c.commandBulkString(requestWithStringAnd2Decimals("*4\r\n$8\r\nGETRANGE\r\n$", key, start, end))
	return s, err
}

// BytesGETRANGE executes <https://redis.io/commands/getrange>.
func (c *Client) BytesGETRANGE(key []byte, start, end int64) ([]byte, error) {
	return c.commandBulkBytes(requestWithStringAnd2Decimals("*4\r\n$8\r\nGETRANGE\r\n$", key, start, end))
}

// APPEND executes <https://redis.io/commands/append>.
func (c *Client) APPEND(key string, value []byte) (newLen int64, err error) {
	return c.commandInteger(requestWith2Strings("*3\r\n$6\r\nAPPEND\r\n$", key, value))
}

// BytesAPPEND executes <https://redis.io/commands/append>.
func (c *Client) BytesAPPEND(key, value []byte) (newLen int64, err error) {
	return c.commandInteger(requestWith2Strings("*3\r\n$6\r\nAPPEND\r\n$", key, value))
}

// APPENDString executes <https://redis.io/commands/append>.
func (c *Client) APPENDString(key, value string) (newLen int64, err error) {
	return c.commandInteger(requestWith2Strings("*3\r\n$6\r\nAPPEND\r\n$", key, value))
}

// LLEN executes <https://redis.io/commands/llen>.
// The return is 0 if key does not exist.
func (c *Client) LLEN(key string) (int64, error) {
	return c.commandInteger(requestWithString("*2\r\n$4\r\nLLEN\r\n$", key))
}

// BytesLLEN executes <https://redis.io/commands/llen>.
// The return is 0 if key does not exist.
func (c *Client) BytesLLEN(key []byte) (int64, error) {
	return c.commandInteger(requestWithString("*2\r\n$4\r\nLLEN\r\n$", key))
}

// LINDEX executes <https://redis.io/commands/lindex>.
// The return is nil if key does not exist.
// The return is nil if index is out of range.
func (c *Client) LINDEX(key string, index int64) (value []byte, err error) {
	return c.commandBulkBytes(requestWithStringAndDecimal("*3\r\n$6\r\nLINDEX\r\n$", key, index))
}

// LINDEXString executes <https://redis.io/commands/lindex>.
// Boolean ok is false if key does not exist.
// Boolean ok is false if index is out of range.
func (c *Client) LINDEXString(key string, index int64) (value string, ok bool, err error) {
	return c.commandBulkString(requestWithStringAndDecimal("*3\r\n$6\r\nLINDEX\r\n$", key, index))
}

// BytesLINDEX executes <https://redis.io/commands/lindex>.
// The return is nil if key does not exist.
// The return is nil if index is out of range.
func (c *Client) BytesLINDEX(key []byte, index int64) (value []byte, err error) {
	return c.commandBulkBytes(requestWithStringAndDecimal("*3\r\n$6\r\nLINDEX\r\n$", key, index))
}

// LRANGE executes <https://redis.io/commands/lrange>.
// The return is empty if key does not exist.
func (c *Client) LRANGE(key string, start, stop int64) (values [][]byte, err error) {
	return c.commandBytesArray(requestWithStringAnd2Decimals("*4\r\n$6\r\nLRANGE\r\n$", key, start, stop))
}

// LRANGEString executes <https://redis.io/commands/lrange>.
// The return is empty if key does not exist.
func (c *Client) LRANGEString(key string, start, stop int64) (values []string, err error) {
	return c.commandStringArray(requestWithStringAnd2Decimals("*4\r\n$6\r\nLRANGE\r\n$", key, start, stop))
}

// BytesLRANGE executes <https://redis.io/commands/lrange>.
// The return is empty if key does not exist.
func (c *Client) BytesLRANGE(key []byte, start, stop int64) (values [][]byte, err error) {
	return c.commandBytesArray(requestWithStringAnd2Decimals("*4\r\n$6\r\nLRANGE\r\n$", key, start, stop))
}

// LPOP executes <https://redis.io/commands/lpop>.
// The return is nil if key does not exist.
func (c *Client) LPOP(key string) (value []byte, err error) {
	return c.commandBulkBytes(requestWithString("*2\r\n$4\r\nLPOP\r\n$", key))
}

// LPOPString executes <https://redis.io/commands/lpop>.
// Boolean ok is false if key does not exist.
func (c *Client) LPOPString(key string) (value string, ok bool, err error) {
	return c.commandBulkString(requestWithString("*2\r\n$4\r\nLPOP\r\n$", key))
}

// BytesLPOP executes <https://redis.io/commands/lpop>.
// The return is nil if key does not exist.
func (c *Client) BytesLPOP(key []byte) (value []byte, err error) {
	return c.commandBulkBytes(requestWithString("*2\r\n$4\r\nLPOP\r\n$", key))
}

// RPOP executes <https://redis.io/commands/rpop>.
// The return is nil if key does not exist.
func (c *Client) RPOP(key string) (value []byte, err error) {
	return c.commandBulkBytes(requestWithString("*2\r\n$4\r\nRPOP\r\n$", key))
}

// RPOPString executes <https://redis.io/commands/rpop>.
// Boolean ok is false if key does not exist.
func (c *Client) RPOPString(key string) (value string, ok bool, err error) {
	return c.commandBulkString(requestWithString("*2\r\n$4\r\nRPOP\r\n$", key))
}

// BytesRPOP executes <https://redis.io/commands/rpop>.
// The return is nil if key does not exist.
func (c *Client) BytesRPOP(key []byte) (value []byte, err error) {
	return c.commandBulkBytes(requestWithString("*2\r\n$4\r\nRPOP\r\n$", key))
}

// LTRIM executes <https://redis.io/commands/ltrim>.
func (c *Client) LTRIM(key string, start, stop int64) error {
	return c.commandOK(requestWithStringAnd2Decimals("*4\r\n$5\r\nLTRIM\r\n$", key, start, stop))
}

// BytesLTRIM executes <https://redis.io/commands/ltrim>.
func (c *Client) BytesLTRIM(key []byte, start, stop int64) error {
	return c.commandOK(requestWithStringAnd2Decimals("*4\r\n$5\r\nLTRIM\r\n$", key, start, stop))
}

// LSET executes <https://redis.io/commands/lset>.
func (c *Client) LSET(key string, index int64, value []byte) error {
	return c.commandOK(requestWithStringAndDecimalAndString("*4\r\n$4\r\nLSET\r\n$", key, index, value))
}

// LSETString executes <https://redis.io/commands/lset>.
func (c *Client) LSETString(key string, index int64, value string) error {
	return c.commandOK(requestWithStringAndDecimalAndString("*4\r\n$4\r\nLSET\r\n$", key, index, value))
}

// BytesLSET executes <https://redis.io/commands/lset>.
func (c *Client) BytesLSET(key []byte, index int64, value []byte) error {
	return c.commandOK(requestWithStringAndDecimalAndString("*4\r\n$4\r\nLSET\r\n$", key, index, value))
}

// LPUSH executes <https://redis.io/commands/lpush>.
func (c *Client) LPUSH(key string, value []byte) (newLen int64, err error) {
	return c.commandInteger(requestWith2Strings("*3\r\n$5\r\nLPUSH\r\n$", key, value))
}

// BytesLPUSH executes <https://redis.io/commands/lpush>.
func (c *Client) BytesLPUSH(key, value []byte) (newLen int64, err error) {
	return c.commandInteger(requestWith2Strings("*3\r\n$5\r\nLPUSH\r\n$", key, value))
}

// LPUSHString executes <https://redis.io/commands/lpush>.
func (c *Client) LPUSHString(key, value string) (newLen int64, err error) {
	return c.commandInteger(requestWith2Strings("*3\r\n$5\r\nLPUSH\r\n$", key, value))
}

// RPUSH executes <https://redis.io/commands/rpush>.
func (c *Client) RPUSH(key string, value []byte) (newLen int64, err error) {
	return c.commandInteger(requestWith2Strings("*3\r\n$5\r\nRPUSH\r\n$", key, value))
}

// BytesRPUSH executes <https://redis.io/commands/rpush>.
func (c *Client) BytesRPUSH(key, value []byte) (newLen int64, err error) {
	return c.commandInteger(requestWith2Strings("*3\r\n$5\r\nRPUSH\r\n$", key, value))
}

// RPUSHString executes <https://redis.io/commands/rpush>.
func (c *Client) RPUSHString(key, value string) (newLen int64, err error) {
	return c.commandInteger(requestWith2Strings("*3\r\n$5\r\nRPUSH\r\n$", key, value))
}

// HGET executes <https://redis.io/commands/hget>.
// The return is nil if key does not exist.
func (c *Client) HGET(key, field string) (value []byte, err error) {
	return c.commandBulkBytes(requestWith2Strings("*3\r\n$4\r\nHGET\r\n$", key, field))
}

// HGETString executes <https://redis.io/commands/hget>.
// Boolean ok is false if key does not exist.
func (c *Client) HGETString(key, field string) (value string, ok bool, err error) {
	return c.commandBulkString(requestWith2Strings("*3\r\n$4\r\nHGET\r\n$", key, field))
}

// BytesHGET executes <https://redis.io/commands/hget>.
// The return is nil if key does not exist.
func (c *Client) BytesHGET(key, field []byte) (value []byte, err error) {
	return c.commandBulkBytes(requestWith2Strings("*3\r\n$4\r\nHGET\r\n$", key, field))
}

// HSET executes <https://redis.io/commands/hset>.
func (c *Client) HSET(key, field string, value []byte) (newField bool, err error) {
	created, err := c.commandInteger(requestWith3Strings("*4\r\n$4\r\nHSET\r\n$", key, field, value))
	return created != 0, err
}

// BytesHSET executes <https://redis.io/commands/hset>.
func (c *Client) BytesHSET(key, field, value []byte) (newField bool, err error) {
	created, err := c.commandInteger(requestWith3Strings("*4\r\n$4\r\nHSET\r\n$", key, field, value))
	return created != 0, err
}

// HSETString executes <https://redis.io/commands/hset>.
func (c *Client) HSETString(key, field, value string) (updated bool, err error) {
	replaced, err := c.commandInteger(requestWith3Strings("*4\r\n$4\r\nHSET\r\n$", key, field, value))
	return replaced != 0, err
}

// HDEL executes <https://redis.io/commands/hdel>.
func (c *Client) HDEL(key, field string) (bool, error) {
	removed, err := c.commandInteger(requestWith2Strings("*3\r\n$4\r\nHDEL\r\n$", key, field))
	return removed != 0, err
}

// HDELArgs executes <https://redis.io/commands/hdel>.
func (c *Client) HDELArgs(key string, fields ...string) (int64, error) {
	return c.commandInteger(requestWithStringAndList("\r\n$4\r\nHDEL\r\n$", key, fields))
}

// BytesHDEL executes <https://redis.io/commands/hdel>.
func (c *Client) BytesHDEL(key, field []byte) (bool, error) {
	removed, err := c.commandInteger(requestWith2Strings("*3\r\n$4\r\nHDEL\r\n$", key, field))
	return removed != 0, err
}

// BytesHDELArgs executes <https://redis.io/commands/hdel>.
func (c *Client) BytesHDELArgs(key []byte, fields ...[]byte) (int64, error) {
	return c.commandInteger(requestWithStringAndList("\r\n$4\r\nHDEL\r\n$", key, fields))
}

// HMGET executes <https://redis.io/commands/hmget>.
// For every field that does not exist, a nil value is returned.
func (c *Client) HMGET(key string, fields ...string) (values [][]byte, err error) {
	return c.commandBytesArray(requestWithStringAndList("\r\n$5\r\nHMGET\r\n$", key, fields))
}

// HMGETString executes <https://redis.io/commands/hmget>.
// For every field that does not exist, an empty string is returned.
func (c *Client) HMGETString(key string, fields ...string) (values []string, err error) {
	return c.commandStringArray(requestWithStringAndList("\r\n$5\r\nHMGET\r\n$", key, fields))
}

// BytesHMGET executes <https://redis.io/commands/hmget>.
// For every field that does not exist, a nil value is returned.
func (c *Client) BytesHMGET(key []byte, fields ...[]byte) (values [][]byte, err error) {
	return c.commandBytesArray(requestWithStringAndList("\r\n$5\r\nHMGET\r\n$", key, fields))
}

// BytesHMSET executes <https://redis.io/commands/hmset>.
func (c *Client) BytesHMSET(key []byte, fields, values [][]byte) error {
	r, err := requestWithStringAndMap("\r\n$5\r\nHMSET\r\n$", key, fields, values)
	if err != nil {
		return err
	}
	return c.commandOK(r)
}

// HMSET executes <https://redis.io/commands/hmset>.
func (c *Client) HMSET(key string, fields []string, values [][]byte) error {
	r, err := requestWithStringAndMap("\r\n$5\r\nHMSET\r\n$", key, fields, values)
	if err != nil {
		return err
	}
	return c.commandOK(r)
}

// HMSETString executes <https://redis.io/commands/hmset>.
func (c *Client) HMSETString(key string, fields, values []string) error {
	r, err := requestWithStringAndMap("\r\n$5\r\nHMSET\r\n$", key, fields, values)
	if err != nil {
		return err
	}
	return c.commandOK(r)
}
