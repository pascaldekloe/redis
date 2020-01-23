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
	// Composotion of NX, XX, EX or PX.
	Flags uint

	// The value is rounded to seconds with the EX flag,
	// and milliseconds with PX. Non-zero values without
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

// AUTH executes <https://redis.io/commands/auth>.
// The authentication is restored automatically on connection loss.
func (c *Client) AUTH(password string) error {
	c.password.Store(password)
	r := newRequest("*2\r\n$4\r\nAUTH\r\n$")
	r.addString(password)
	return c.commandRequireOK(r)
}

// SELECT executes <https://redis.io/commands/select>.
// The selection is restored automatically on connection loss. Client executes
// all commands in db after a SELECT, even when the return is in error.
func (c *Client) SELECT(db int64) error {
	atomic.StoreInt64(&c.db, db)
	r := newRequest("*2\r\n$6\r\nSELECT\r\n$")
	r.addDecimal(db)
	return c.commandRequireOK(r)
}

// MOVE executes <https://redis.io/commands/move>.
func (c *Client) MOVE(key string, db int64) (bool, error) {
	r := newRequest("*3\r\n$4\r\nMOVE\r\n$")
	r.addStringInt(key, db)
	n, err := c.commandInteger(r)
	return n != 0, err
}

// BytesMOVE executes <https://redis.io/commands/move>.
func (c *Client) BytesMOVE(key []byte, db int64) (bool, error) {
	r := newRequest("*3\r\n$4\r\nMOVE\r\n$")
	r.addBytesInt(key, db)
	n, err := c.commandInteger(r)
	return n != 0, err
}

// FLUSHDB executes <https://redis.io/commands/flushdb>.
func (c *Client) FLUSHDB(async bool) error {
	var r *request
	if async {
		r = newRequest("*2\r\n$7\r\nFLUSHDB\r\n$5\r\nASYNC\r\n")
	} else {
		r = newRequest("*1\r\n$7\r\nFLUSHDB\r\n")
	}
	return c.commandOK(r)
}

// FLUSHALL executes <https://redis.io/commands/flushall>.
func (c *Client) FLUSHALL(async bool) error {
	var r *request
	if async {
		r = newRequest("*2\r\n$8\r\nFLUSHALL\r\n$5\r\nASYNC\r\n")
	} else {
		r = newRequest("*1\r\n$8\r\nFLUSHALL\r\n")
	}
	return c.commandOK(r)
}

// GET executes <https://redis.io/commands/get>.
// The return is nil if key does not exist.
func (c *Client) GET(key string) (value []byte, err error) {
	r := newRequest("*2\r\n$3\r\nGET\r\n$")
	r.addString(key)
	return c.commandBlobBytes(r)
}

// GETString executes <https://redis.io/commands/get>.
// Boolean ok is false if key does not exist.
func (c *Client) GETString(key string) (value string, ok bool, err error) {
	r := newRequest("*2\r\n$3\r\nGET\r\n$")
	r.addString(key)
	return c.commandBlobString(r)
}

// BytesGET executes <https://redis.io/commands/get>.
// The return is nil if key does not exist.
func (c *Client) BytesGET(key []byte) (value []byte, err error) {
	r := newRequest("*2\r\n$3\r\nGET\r\n$")
	r.addBytes(key)
	return c.commandBlobBytes(r)
}

// MGET executes <https://redis.io/commands/mget>.
// For every key that does not exist, a nil value is returned.
func (c *Client) MGET(keys ...string) (values [][]byte, err error) {
	r := newRequestSize(len(keys)+1, "\r\n$4\r\nMGET")
	r.addStringList(keys)
	return c.commandBytesArray(r)
}

// MGETString executes <https://redis.io/commands/mget>.
// For every key that does not exist, an empty string is returned.
func (c *Client) MGETString(keys ...string) (values []string, err error) {
	r := newRequestSize(len(keys)+1, "\r\n$4\r\nMGET")
	r.addStringList(keys)
	return c.commandStringArray(r)
}

// BytesMGET executes <https://redis.io/commands/mget>.
// For every key that does not exist, a nil value is returned.
func (c *Client) BytesMGET(keys ...[]byte) (values [][]byte, err error) {
	r := newRequestSize(len(keys)+1, "\r\n$4\r\nMGET")
	r.addBytesList(keys)
	return c.commandBytesArray(r)
}

// SET executes <https://redis.io/commands/set>.
func (c *Client) SET(key string, value []byte) error {
	r := newRequest("*3\r\n$3\r\nSET\r\n$")
	r.addStringBytes(key, value)
	return c.commandOK(r)
}

// BytesSET executes <https://redis.io/commands/set>.
func (c *Client) BytesSET(key, value []byte) error {
	r := newRequest("*3\r\n$3\r\nSET\r\n$")
	r.addBytesBytes(key, value)
	return c.commandOK(r)
}

// SETString executes <https://redis.io/commands/set>.
func (c *Client) SETString(key, value string) error {
	r := newRequest("*3\r\n$3\r\nSET\r\n$")
	r.addStringString(key, value)
	return c.commandOK(r)
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
		r = newRequest("*4\r\n$3\r\nSET\r\n$")
		r.addStringBytesString(key, value, existArg)
	case existArg == "" && expireArg != "":
		r = newRequest("*5\r\n$3\r\nSET\r\n$")
		r.addStringBytesStringInt(key, value, expireArg, expire)
	case existArg != "" && expireArg != "":
		r = newRequest("*6\r\n$3\r\nSET\r\n$")
		r.addStringBytesStringStringInt(key, value, existArg, expireArg, expire)
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
		r = newRequest("*4\r\n$3\r\nSET\r\n$")
		r.addBytesBytesString(key, value, existArg)
	case existArg == "" && expireArg != "":
		r = newRequest("*5\r\n$3\r\nSET\r\n$")
		r.addBytesBytesStringInt(key, value, expireArg, expire)
	case existArg != "" && expireArg != "":
		r = newRequest("*6\r\n$3\r\nSET\r\n$")
		r.addBytesBytesStringStringInt(key, value, existArg, expireArg, expire)
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
		r = newRequest("*4\r\n$3\r\nSET\r\n$")
		r.addStringStringString(key, value, existArg)
	case existArg == "" && expireArg != "":
		r = newRequest("*5\r\n$3\r\nSET\r\n$")
		r.addStringStringStringInt(key, value, expireArg, expire)
	case existArg != "" && expireArg != "":
		r = newRequest("*6\r\n$3\r\nSET\r\n$")
		r.addStringStringStringStringInt(key, value, existArg, expireArg, expire)
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
	r := newRequestSize(len(keys)*2+1, "\r\n$4\r\nMSET")
	r.addStringBytesMapLists(keys, values)
	return c.commandOK(r)
}

// BytesMSET executes <https://redis.io/commands/mset>.
func (c *Client) BytesMSET(keys, values [][]byte) error {
	r := newRequestSize(len(keys)*2+1, "\r\n$4\r\nMSET")
	r.addBytesBytesMapLists(keys, values)
	return c.commandOK(r)
}

// MSETString executes <https://redis.io/commands/mset>.
func (c *Client) MSETString(keys, values []string) error {
	r := newRequestSize(len(keys)*2+1, "\r\n$4\r\nMSET")
	r.addStringStringMapLists(keys, values)
	return c.commandOK(r)
}

// DEL executes <https://redis.io/commands/del>.
func (c *Client) DEL(key string) (bool, error) {
	r := newRequest("*2\r\n$3\r\nDEL\r\n$")
	r.addString(key)
	removed, err := c.commandInteger(r)
	return removed != 0, err
}

// DELArgs executes <https://redis.io/commands/del>.
func (c *Client) DELArgs(keys ...string) (int64, error) {
	r := newRequestSize(1+len(keys), "\r\n$3\r\nDEL")
	r.addStringList(keys)
	return c.commandInteger(r)
}

// BytesDEL executes <https://redis.io/commands/del>.
func (c *Client) BytesDEL(key []byte) (bool, error) {
	r := newRequest("*2\r\n$3\r\nDEL\r\n$")
	r.addBytes(key)
	removed, err := c.commandInteger(r)
	return removed != 0, err
}

// BytesDELArgs executes <https://redis.io/commands/del>.
func (c *Client) BytesDELArgs(keys ...[]byte) (int64, error) {
	r := newRequestSize(1+len(keys), "\r\n$3\r\nDEL")
	r.addBytesList(keys)
	return c.commandInteger(r)
}

// INCR executes <https://redis.io/commands/incr>.
func (c *Client) INCR(key string) (newValue int64, err error) {
	r := newRequest("*2\r\n$4\r\nINCR\r\n$")
	r.addString(key)
	return c.commandInteger(r)
}

// BytesINCR executes <https://redis.io/commands/incr>.
func (c *Client) BytesINCR(key []byte) (newValue int64, err error) {
	r := newRequest("*2\r\n$4\r\nINCR\r\n$")
	r.addBytes(key)
	return c.commandInteger(r)
}

// INCRBY executes <https://redis.io/commands/incrby>.
func (c *Client) INCRBY(key string, increment int64) (newValue int64, err error) {
	r := newRequest("*3\r\n$6\r\nINCRBY\r\n$")
	r.addStringInt(key, increment)
	return c.commandInteger(r)
}

// BytesINCRBY executes <https://redis.io/commands/incrby>.
func (c *Client) BytesINCRBY(key []byte, increment int64) (newValue int64, err error) {
	r := newRequest("*3\r\n$6\r\nINCRBY\r\n$")
	r.addBytesInt(key, increment)
	return c.commandInteger(r)
}

// STRLEN executes <https://redis.io/commands/strlen>.
func (c *Client) STRLEN(key string) (int64, error) {
	r := newRequest("*2\r\n$6\r\nSTRLEN\r\n$")
	r.addString(key)
	return c.commandInteger(r)
}

// BytesSTRLEN executes <https://redis.io/commands/strlen>.
func (c *Client) BytesSTRLEN(key []byte) (int64, error) {
	r := newRequest("*2\r\n$6\r\nSTRLEN\r\n$")
	r.addBytes(key)
	return c.commandInteger(r)
}

// GETRANGE executes <https://redis.io/commands/getrange>.
func (c *Client) GETRANGE(key string, start, end int64) ([]byte, error) {
	r := newRequest("*4\r\n$8\r\nGETRANGE\r\n$")
	r.addStringIntInt(key, start, end)
	return c.commandBlobBytes(r)
}

// GETRANGEString executes <https://redis.io/commands/getrange>.
func (c *Client) GETRANGEString(key string, start, end int64) (string, error) {
	r := newRequest("*4\r\n$8\r\nGETRANGE\r\n$")
	r.addStringIntInt(key, start, end)
	s, _, err := c.commandBlobString(r)
	return s, err
}

// BytesGETRANGE executes <https://redis.io/commands/getrange>.
func (c *Client) BytesGETRANGE(key []byte, start, end int64) ([]byte, error) {
	r := newRequest("*4\r\n$8\r\nGETRANGE\r\n$")
	r.addBytesIntInt(key, start, end)
	return c.commandBlobBytes(r)
}

// APPEND executes <https://redis.io/commands/append>.
func (c *Client) APPEND(key string, value []byte) (newLen int64, err error) {
	r := newRequest("*3\r\n$6\r\nAPPEND\r\n$")
	r.addStringBytes(key, value)
	return c.commandInteger(r)
}

// BytesAPPEND executes <https://redis.io/commands/append>.
func (c *Client) BytesAPPEND(key, value []byte) (newLen int64, err error) {
	r := newRequest("*3\r\n$6\r\nAPPEND\r\n$")
	r.addBytesBytes(key, value)
	return c.commandInteger(r)
}

// APPENDString executes <https://redis.io/commands/append>.
func (c *Client) APPENDString(key, value string) (newLen int64, err error) {
	r := newRequest("*3\r\n$6\r\nAPPEND\r\n$")
	r.addStringString(key, value)
	return c.commandInteger(r)
}

// LLEN executes <https://redis.io/commands/llen>.
// The return is 0 if key does not exist.
func (c *Client) LLEN(key string) (int64, error) {
	r := newRequest("*2\r\n$4\r\nLLEN\r\n$")
	r.addString(key)
	return c.commandInteger(r)
}

// BytesLLEN executes <https://redis.io/commands/llen>.
// The return is 0 if key does not exist.
func (c *Client) BytesLLEN(key []byte) (int64, error) {
	r := newRequest("*2\r\n$4\r\nLLEN\r\n$")
	r.addBytes(key)
	return c.commandInteger(r)
}

// LINDEX executes <https://redis.io/commands/lindex>.
// The return is nil if key does not exist.
// The return is nil if index is out of range.
func (c *Client) LINDEX(key string, index int64) (value []byte, err error) {
	r := newRequest("*3\r\n$6\r\nLINDEX\r\n$")
	r.addStringInt(key, index)
	return c.commandBlobBytes(r)
}

// LINDEXString executes <https://redis.io/commands/lindex>.
// Boolean ok is false if key does not exist.
// Boolean ok is false if index is out of range.
func (c *Client) LINDEXString(key string, index int64) (value string, ok bool, err error) {
	r := newRequest("*3\r\n$6\r\nLINDEX\r\n$")
	r.addStringInt(key, index)
	return c.commandBlobString(r)
}

// BytesLINDEX executes <https://redis.io/commands/lindex>.
// The return is nil if key does not exist.
// The return is nil if index is out of range.
func (c *Client) BytesLINDEX(key []byte, index int64) (value []byte, err error) {
	r := newRequest("*3\r\n$6\r\nLINDEX\r\n$")
	r.addBytesInt(key, index)
	return c.commandBlobBytes(r)
}

// LRANGE executes <https://redis.io/commands/lrange>.
// The return is empty if key does not exist.
func (c *Client) LRANGE(key string, start, stop int64) (values [][]byte, err error) {
	r := newRequest("*4\r\n$6\r\nLRANGE\r\n$")
	r.addStringIntInt(key, start, stop)
	return c.commandBytesArray(r)
}

// LRANGEString executes <https://redis.io/commands/lrange>.
// The return is empty if key does not exist.
func (c *Client) LRANGEString(key string, start, stop int64) (values []string, err error) {
	r := newRequest("*4\r\n$6\r\nLRANGE\r\n$")
	r.addStringIntInt(key, start, stop)
	return c.commandStringArray(r)
}

// BytesLRANGE executes <https://redis.io/commands/lrange>.
// The return is empty if key does not exist.
func (c *Client) BytesLRANGE(key []byte, start, stop int64) (values [][]byte, err error) {
	r := newRequest("*4\r\n$6\r\nLRANGE\r\n$")
	r.addBytesIntInt(key, start, stop)
	return c.commandBytesArray(r)
}

// LPOP executes <https://redis.io/commands/lpop>.
// The return is nil if key does not exist.
func (c *Client) LPOP(key string) (value []byte, err error) {
	r := newRequest("*2\r\n$4\r\nLPOP\r\n$")
	r.addString(key)
	return c.commandBlobBytes(r)
}

// LPOPString executes <https://redis.io/commands/lpop>.
// Boolean ok is false if key does not exist.
func (c *Client) LPOPString(key string) (value string, ok bool, err error) {
	r := newRequest("*2\r\n$4\r\nLPOP\r\n$")
	r.addString(key)
	return c.commandBlobString(r)
}

// BytesLPOP executes <https://redis.io/commands/lpop>.
// The return is nil if key does not exist.
func (c *Client) BytesLPOP(key []byte) (value []byte, err error) {
	r := newRequest("*2\r\n$4\r\nLPOP\r\n$")
	r.addBytes(key)
	return c.commandBlobBytes(r)
}

// RPOP executes <https://redis.io/commands/rpop>.
// The return is nil if key does not exist.
func (c *Client) RPOP(key string) (value []byte, err error) {
	r := newRequest("*2\r\n$4\r\nRPOP\r\n$")
	r.addString(key)
	return c.commandBlobBytes(r)
}

// RPOPString executes <https://redis.io/commands/rpop>.
// Boolean ok is false if key does not exist.
func (c *Client) RPOPString(key string) (value string, ok bool, err error) {
	r := newRequest("*2\r\n$4\r\nRPOP\r\n$")
	r.addString(key)
	return c.commandBlobString(r)
}

// BytesRPOP executes <https://redis.io/commands/rpop>.
// The return is nil if key does not exist.
func (c *Client) BytesRPOP(key []byte) (value []byte, err error) {
	r := newRequest("*2\r\n$4\r\nRPOP\r\n$")
	r.addBytes(key)
	return c.commandBlobBytes(r)
}

// LTRIM executes <https://redis.io/commands/ltrim>.
func (c *Client) LTRIM(key string, start, stop int64) error {
	r := newRequest("*4\r\n$5\r\nLTRIM\r\n$")
	r.addStringIntInt(key, start, stop)
	return c.commandOK(r)
}

// BytesLTRIM executes <https://redis.io/commands/ltrim>.
func (c *Client) BytesLTRIM(key []byte, start, stop int64) error {
	r := newRequest("*4\r\n$5\r\nLTRIM\r\n$")
	r.addBytesIntInt(key, start, stop)
	return c.commandOK(r)
}

// LSET executes <https://redis.io/commands/lset>.
func (c *Client) LSET(key string, index int64, value []byte) error {
	r := newRequest("*4\r\n$4\r\nLSET\r\n$")
	r.addStringIntBytes(key, index, value)
	return c.commandOK(r)
}

// LSETString executes <https://redis.io/commands/lset>.
func (c *Client) LSETString(key string, index int64, value string) error {
	r := newRequest("*4\r\n$4\r\nLSET\r\n$")
	r.addStringIntString(key, index, value)
	return c.commandOK(r)
}

// BytesLSET executes <https://redis.io/commands/lset>.
func (c *Client) BytesLSET(key []byte, index int64, value []byte) error {
	r := newRequest("*4\r\n$4\r\nLSET\r\n$")
	r.addBytesIntBytes(key, index, value)
	return c.commandOK(r)
}

// LPUSH executes <https://redis.io/commands/lpush>.
func (c *Client) LPUSH(key string, value []byte) (newLen int64, err error) {
	r := newRequest("*3\r\n$5\r\nLPUSH\r\n$")
	r.addStringBytes(key, value)
	return c.commandInteger(r)
}

// BytesLPUSH executes <https://redis.io/commands/lpush>.
func (c *Client) BytesLPUSH(key, value []byte) (newLen int64, err error) {
	r := newRequest("*3\r\n$5\r\nLPUSH\r\n$")
	r.addBytesBytes(key, value)
	return c.commandInteger(r)
}

// LPUSHString executes <https://redis.io/commands/lpush>.
func (c *Client) LPUSHString(key, value string) (newLen int64, err error) {
	r := newRequest("*3\r\n$5\r\nLPUSH\r\n$")
	r.addStringString(key, value)
	return c.commandInteger(r)
}

// RPUSH executes <https://redis.io/commands/rpush>.
func (c *Client) RPUSH(key string, value []byte) (newLen int64, err error) {
	r := newRequest("*3\r\n$5\r\nRPUSH\r\n$")
	r.addStringBytes(key, value)
	return c.commandInteger(r)
}

// BytesRPUSH executes <https://redis.io/commands/rpush>.
func (c *Client) BytesRPUSH(key, value []byte) (newLen int64, err error) {
	r := newRequest("*3\r\n$5\r\nRPUSH\r\n$")
	r.addBytesBytes(key, value)
	return c.commandInteger(r)
}

// RPUSHString executes <https://redis.io/commands/rpush>.
func (c *Client) RPUSHString(key, value string) (newLen int64, err error) {
	r := newRequest("*3\r\n$5\r\nRPUSH\r\n$")
	r.addStringString(key, value)
	return c.commandInteger(r)
}

// HGET executes <https://redis.io/commands/hget>.
// The return is nil if key does not exist.
func (c *Client) HGET(key, field string) (value []byte, err error) {
	r := newRequest("*3\r\n$4\r\nHGET\r\n$")
	r.addStringString(key, field)
	return c.commandBlobBytes(r)
}

// HGETString executes <https://redis.io/commands/hget>.
// Boolean ok is false if key does not exist.
func (c *Client) HGETString(key, field string) (value string, ok bool, err error) {
	r := newRequest("*3\r\n$4\r\nHGET\r\n$")
	r.addStringString(key, field)
	return c.commandBlobString(r)
}

// BytesHGET executes <https://redis.io/commands/hget>.
// The return is nil if key does not exist.
func (c *Client) BytesHGET(key, field []byte) (value []byte, err error) {
	r := newRequest("*3\r\n$4\r\nHGET\r\n$")
	r.addBytesBytes(key, field)
	return c.commandBlobBytes(r)
}

// HSET executes <https://redis.io/commands/hset>.
func (c *Client) HSET(key, field string, value []byte) (newField bool, err error) {
	r := newRequest("*4\r\n$4\r\nHSET\r\n$")
	r.addStringStringBytes(key, field, value)
	created, err := c.commandInteger(r)
	return created != 0, err
}

// BytesHSET executes <https://redis.io/commands/hset>.
func (c *Client) BytesHSET(key, field, value []byte) (newField bool, err error) {
	r := newRequest("*4\r\n$4\r\nHSET\r\n$")
	r.addBytesBytesBytes(key, field, value)
	created, err := c.commandInteger(r)
	return created != 0, err
}

// HSETString executes <https://redis.io/commands/hset>.
func (c *Client) HSETString(key, field, value string) (updated bool, err error) {
	r := newRequest("*4\r\n$4\r\nHSET\r\n$")
	r.addStringStringString(key, field, value)
	replaced, err := c.commandInteger(r)
	return replaced != 0, err
}

// HDEL executes <https://redis.io/commands/hdel>.
func (c *Client) HDEL(key, field string) (bool, error) {
	r := newRequest("*3\r\n$4\r\nHDEL\r\n$")
	r.addStringString(key, field)
	removed, err := c.commandInteger(r)
	return removed != 0, err
}

// HDELArgs executes <https://redis.io/commands/hdel>.
func (c *Client) HDELArgs(key string, fields ...string) (int64, error) {
	r := newRequestSize(2+len(fields), "\r\n$4\r\nHDEL\r\n$")
	r.addStringStringList(key, fields)
	return c.commandInteger(r)
}

// BytesHDEL executes <https://redis.io/commands/hdel>.
func (c *Client) BytesHDEL(key, field []byte) (bool, error) {
	r := newRequest("*3\r\n$4\r\nHDEL\r\n$")
	r.addBytesBytes(key, field)
	removed, err := c.commandInteger(r)
	return removed != 0, err
}

// BytesHDELArgs executes <https://redis.io/commands/hdel>.
func (c *Client) BytesHDELArgs(key []byte, fields ...[]byte) (int64, error) {
	r := newRequestSize(2+len(fields), "\r\n$4\r\nHDEL\r\n$")
	r.addBytesBytesList(key, fields)
	return c.commandInteger(r)
}

// HMGET executes <https://redis.io/commands/hmget>.
// For every field that does not exist, a nil value is returned.
func (c *Client) HMGET(key string, fields ...string) (values [][]byte, err error) {
	r := newRequestSize(2+len(fields), "\r\n$5\r\nHMGET\r\n$")
	r.addStringStringList(key, fields)
	return c.commandBytesArray(r)
}

// HMGETString executes <https://redis.io/commands/hmget>.
// For every field that does not exist, an empty string is returned.
func (c *Client) HMGETString(key string, fields ...string) (values []string, err error) {
	r := newRequestSize(2+len(fields), "\r\n$5\r\nHMGET\r\n$")
	r.addStringStringList(key, fields)
	return c.commandStringArray(r)
}

// BytesHMGET executes <https://redis.io/commands/hmget>.
// For every field that does not exist, a nil value is returned.
func (c *Client) BytesHMGET(key []byte, fields ...[]byte) (values [][]byte, err error) {
	r := newRequestSize(2+len(fields), "\r\n$5\r\nHMGET\r\n$")
	r.addBytesBytesList(key, fields)
	return c.commandBytesArray(r)
}

// BytesHMSET executes <https://redis.io/commands/hmset>.
func (c *Client) BytesHMSET(key []byte, fields, values [][]byte) error {
	r := newRequestSize(2+len(fields)*2, "\r\n$5\r\nHMSET\r\n$")
	err := r.addBytesBytesBytesMapLists(key, fields, values)
	if err != nil {
		return err
	}
	return c.commandOK(r)
}

// HMSET executes <https://redis.io/commands/hmset>.
func (c *Client) HMSET(key string, fields []string, values [][]byte) error {
	r := newRequestSize(2+len(fields)*2, "\r\n$5\r\nHMSET\r\n$")
	err := r.addStringStringBytesMapLists(key, fields, values)
	if err != nil {
		return err
	}
	return c.commandOK(r)
}

// HMSETString executes <https://redis.io/commands/hmset>.
func (c *Client) HMSETString(key string, fields, values []string) error {
	r := newRequestSize(2+len(fields)*2, "\r\n$5\r\nHMSET\r\n$")
	err := r.addStringStringStringMapLists(key, fields, values)
	if err != nil {
		return err
	}
	return c.commandOK(r)
}
