package redis

import (
	"net"
	"strconv"
	"sync"
	"time"
)

var writeBuffers = sync.Pool{New: func() interface{} { return make([]byte, 256) }}

// GET executes <https://redis.io/commands/get>.
// The return is nil if key does not exist.
func (c *Client) GET(key string) (value []byte, err error) {
	const prefix = "*2\r\n$3\r\nGET\r\n$"
	buf := append(writeBuffers.Get().([]byte)[:0], prefix...)
	buf = appendString(buf, key)
	value, err = c.bulkCmd(buf)
	writeBuffers.Put(buf)
	return
}

// BytesGET executes <https://redis.io/commands/get>.
// The return is nil if key does not exist.
func (c *Client) BytesGET(key []byte) (value []byte, err error) {
	const prefix = "*2\r\n$3\r\nGET\r\n$"
	buf := append(writeBuffers.Get().([]byte)[:0], prefix...)
	buf = appendBytes(buf, key)
	value, err = c.bulkCmd(buf)
	writeBuffers.Put(buf)
	return
}

// SET executes <https://redis.io/commands/set>.
func (c *Client) SET(key string, value []byte) error {
	const prefix = "*3\r\n$3\r\nSET\r\n$"
	buf := append(writeBuffers.Get().([]byte)[:0], prefix...)
	buf = appendStringBytes(buf, key, value)
	err := c.okCmd(buf)
	writeBuffers.Put(buf)
	return err
}

// BytesSET executes <https://redis.io/commands/set>.
func (c *Client) BytesSET(key, value []byte) error {
	const prefix = "*3\r\n$3\r\nSET\r\n$"
	buf := append(writeBuffers.Get().([]byte)[:0], prefix...)
	buf = appendBytesBytes(buf, key, value)
	err := c.okCmd(buf)
	writeBuffers.Put(buf)
	return err
}

// SETString executes <https://redis.io/commands/set>.
func (c *Client) SETString(key, value string) error {
	const prefix = "*3\r\n$3\r\nSET\r\n$"
	buf := append(writeBuffers.Get().([]byte)[:0], prefix...)
	buf = appendStringString(buf, key, value)
	err := c.okCmd(buf)
	writeBuffers.Put(buf)
	return err
}

// DEL executes <https://redis.io/commands/del>.
func (c *Client) DEL(key string) (bool, error) {
	const prefix = "*2\r\n$3\r\nDEL\r\n$"
	buf := append(writeBuffers.Get().([]byte)[:0], prefix...)
	buf = appendString(buf, key)
	removed, err := c.intCmd(buf)
	writeBuffers.Put(buf)
	return removed != 0, err
}

// BytesDEL executes <https://redis.io/commands/del>.
func (c *Client) BytesDEL(key []byte) (bool, error) {
	const prefix = "*2\r\n$3\r\nDEL\r\n$"
	buf := append(writeBuffers.Get().([]byte)[:0], prefix...)
	buf = appendBytes(buf, key)
	removed, err := c.intCmd(buf)
	writeBuffers.Put(buf)
	return removed != 0, err
}

// APPEND executes <https://redis.io/commands/append>.
func (c *Client) APPEND(key string, value []byte) (newLen int64, err error) {
	const prefix = "*3\r\n$6\r\nAPPEND\r\n$"
	buf := append(writeBuffers.Get().([]byte)[:0], prefix...)
	buf = appendStringBytes(buf, key, value)
	newLen, err = c.intCmd(buf)
	writeBuffers.Put(buf)
	return
}

// BytesAPPEND executes <https://redis.io/commands/append>.
func (c *Client) BytesAPPEND(key, value []byte) (newLen int64, err error) {
	const prefix = "*3\r\n$6\r\nAPPEND\r\n$"
	buf := append(writeBuffers.Get().([]byte)[:0], prefix...)
	buf = appendBytesBytes(buf, key, value)
	newLen, err = c.intCmd(buf)
	writeBuffers.Put(buf)
	return
}

// APPENDString executes <https://redis.io/commands/append>.
func (c *Client) APPENDString(key, value string) (newLen int64, err error) {
	const prefix = "*3\r\n$6\r\nAPPEND\r\n$"
	buf := append(writeBuffers.Get().([]byte)[:0], prefix...)
	buf = appendStringString(buf, key, value)
	newLen, err = c.intCmd(buf)
	writeBuffers.Put(buf)
	return
}

// LLEN executes <https://redis.io/commands/llen>.
// The return is 0 if key does not exist.
func (c *Client) LLEN(key string) (int64, error) {
	const prefix = "*2\r\n$4\r\nLLEN\r\n$"
	buf := append(writeBuffers.Get().([]byte)[:0], prefix...)
	buf = appendString(buf, key)
	n, err := c.intCmd(buf)
	writeBuffers.Put(buf)
	return n, err
}

// BytesLLEN executes <https://redis.io/commands/llen>.
// The return is 0 if key does not exist.
func (c *Client) BytesLLEN(key []byte) (int64, error) {
	const prefix = "*2\r\n$4\r\nLLEN\r\n$"
	buf := append(writeBuffers.Get().([]byte)[:0], prefix...)
	buf = appendBytes(buf, key)
	n, err := c.intCmd(buf)
	writeBuffers.Put(buf)
	return n, err
}

// LINDEX executes <https://redis.io/commands/lindex>.
// The return is nil if key does not exist.
// The return is nil if index is out of range.
func (c *Client) LINDEX(key string, index int64) (value []byte, err error) {
	const prefix = "*3\r\n$6\r\nLINDEX\r\n$"
	buf := append(writeBuffers.Get().([]byte)[:0], prefix...)
	buf = appendStringInt(buf, key, index)
	value, err = c.bulkCmd(buf)
	writeBuffers.Put(buf)
	return value, err
}

// BytesLINDEX executes <https://redis.io/commands/lindex>.
// The return is nil if key does not exist.
// The return is nil if index is out of range.
func (c *Client) BytesLINDEX(key []byte, index int64) (value []byte, err error) {
	const prefix = "*3\r\n$6\r\nLINDEX\r\n$"
	buf := append(writeBuffers.Get().([]byte)[:0], prefix...)
	buf = appendBytesInt(buf, key, index)
	value, err = c.bulkCmd(buf)
	writeBuffers.Put(buf)
	return value, err
}

// LRANGE executes <https://redis.io/commands/lrange>.
// The return is empty if key does not exist.
func (c *Client) LRANGE(key string, start, end int64) (values [][]byte, err error) {
	const prefix = "*4\r\n$6\r\nLRANGE\r\n$"
	buf := append(writeBuffers.Get().([]byte)[:0], prefix...)
	buf = appendStringIntInt(buf, key, start, end)
	values, err = c.arrayCmd(buf)
	writeBuffers.Put(buf)
	return values, err
}

// BytesLRANGE executes <https://redis.io/commands/lrange>.
// The return is empty if key does not exist.
func (c *Client) BytesLRANGE(key []byte, start, end int64) (values [][]byte, err error) {
	const prefix = "*4\r\n$6\r\nLRANGE\r\n$"
	buf := append(writeBuffers.Get().([]byte)[:0], prefix...)
	buf = appendBytesIntInt(buf, key, start, end)
	values, err = c.arrayCmd(buf)
	writeBuffers.Put(buf)
	return values, err
}

// LPOP executes <https://redis.io/commands/lpop>.
// The return is nil if key does not exist.
func (c *Client) LPOP(key string) (value []byte, err error) {
	const prefix = "*2\r\n$4\r\nLPOP\r\n$"
	buf := append(writeBuffers.Get().([]byte)[:0], prefix...)
	buf = appendString(buf, key)
	value, err = c.bulkCmd(buf)
	writeBuffers.Put(buf)
	return
}

// BytesLPOP executes <https://redis.io/commands/lpop>.
// The return is nil if key does not exist.
func (c *Client) BytesLPOP(key []byte) (value []byte, err error) {
	const prefix = "*2\r\n$4\r\nLPOP\r\n$"
	buf := append(writeBuffers.Get().([]byte)[:0], prefix...)
	buf = appendBytes(buf, key)
	value, err = c.bulkCmd(buf)
	writeBuffers.Put(buf)
	return
}

// RPOP executes <https://redis.io/commands/rpop>.
// The return is nil if key does not exist.
func (c *Client) RPOP(key string) (value []byte, err error) {
	const prefix = "*2\r\n$4\r\nRPOP\r\n$"
	buf := append(writeBuffers.Get().([]byte)[:0], prefix...)
	buf = appendString(buf, key)
	value, err = c.bulkCmd(buf)
	writeBuffers.Put(buf)
	return
}

// BytesRPOP executes <https://redis.io/commands/rpop>.
// The return is nil if key does not exist.
func (c *Client) BytesRPOP(key []byte) (value []byte, err error) {
	const prefix = "*2\r\n$4\r\nRPOP\r\n$"
	buf := append(writeBuffers.Get().([]byte)[:0], prefix...)
	buf = appendBytes(buf, key)
	value, err = c.bulkCmd(buf)
	writeBuffers.Put(buf)
	return
}

// LSET executes <https://redis.io/commands/lset>.
func (c *Client) LSET(key string, index int64, value []byte) error {
	const prefix = "*4\r\n$4\r\nLSET\r\n$"
	buf := append(writeBuffers.Get().([]byte)[:0], prefix...)
	buf = appendStringIntBytes(buf, key, index, value)
	err := c.okCmd(buf)
	writeBuffers.Put(buf)
	return err
}

// LSETString executes <https://redis.io/commands/lset>.
func (c *Client) LSETString(key string, index int64, value string) error {
	const prefix = "*4\r\n$4\r\nSET\r\n$"
	buf := append(writeBuffers.Get().([]byte)[:0], prefix...)
	buf = appendStringIntString(buf, key, index, value)
	err := c.okCmd(buf)
	writeBuffers.Put(buf)
	return err
}

// BytesLSET executes <https://redis.io/commands/lset>.
func (c *Client) BytesLSET(key []byte, index int64, value []byte) error {
	const prefix = "*4\r\n$4\r\nLSET\r\n$"
	buf := append(writeBuffers.Get().([]byte)[:0], prefix...)
	buf = appendBytesIntBytes(buf, key, index, value)
	err := c.okCmd(buf)
	writeBuffers.Put(buf)
	return err
}

// LPUSH executes <https://redis.io/commands/lpush>.
func (c *Client) LPUSH(key string, value []byte) (newLen int64, err error) {
	const prefix = "*3\r\n$5\r\nLPUSH\r\n$"
	buf := append(writeBuffers.Get().([]byte)[:0], prefix...)
	buf = appendStringBytes(buf, key, value)
	newLen, err = c.intCmd(buf)
	writeBuffers.Put(buf)
	return
}

// BytesLPUSH executes <https://redis.io/commands/lpush>.
func (c *Client) BytesLPUSH(key, value []byte) (newLen int64, err error) {
	const prefix = "*3\r\n$5\r\nLPUSH\r\n$"
	buf := append(writeBuffers.Get().([]byte)[:0], prefix...)
	buf = appendBytesBytes(buf, key, value)
	newLen, err = c.intCmd(buf)
	writeBuffers.Put(buf)
	return
}

// LPUSHString executes <https://redis.io/commands/lpush>.
func (c *Client) LPUSHString(key, value string) (newLen int64, err error) {
	const prefix = "*3\r\n$5\r\nLPUSH\r\n$"
	buf := append(writeBuffers.Get().([]byte)[:0], prefix...)
	buf = appendStringString(buf, key, value)
	newLen, err = c.intCmd(buf)
	writeBuffers.Put(buf)
	return
}

// RPUSH executes <https://redis.io/commands/rpush>.
func (c *Client) RPUSH(key string, value []byte) (newLen int64, err error) {
	const prefix = "*3\r\n$5\r\nRPUSH\r\n$"
	buf := append(writeBuffers.Get().([]byte)[:0], prefix...)
	buf = appendStringBytes(buf, key, value)
	newLen, err = c.intCmd(buf)
	writeBuffers.Put(buf)
	return
}

// BytesRPUSH executes <https://redis.io/commands/rpush>.
func (c *Client) BytesRPUSH(key, value []byte) (newLen int64, err error) {
	const prefix = "*3\r\n$5\r\nRPUSH\r\n$"
	buf := append(writeBuffers.Get().([]byte)[:0], prefix...)
	buf = appendBytesBytes(buf, key, value)
	newLen, err = c.intCmd(buf)
	writeBuffers.Put(buf)
	return
}

// RPUSHString executes <https://redis.io/commands/rpush>.
func (c *Client) RPUSHString(key, value string) (newLen int64, err error) {
	const prefix = "*3\r\n$5\r\nRPUSH\r\n$"
	buf := append(writeBuffers.Get().([]byte)[:0], prefix...)
	buf = appendStringString(buf, key, value)
	newLen, err = c.intCmd(buf)
	writeBuffers.Put(buf)
	return
}

// HGET executes <https://redis.io/commands/hget>.
// The return is nil if key does not exist.
func (c *Client) HGET(key, field string) (value []byte, err error) {
	const prefix = "*3\r\n$4\r\nHGET\r\n$"
	buf := append(writeBuffers.Get().([]byte)[:0], prefix...)
	buf = appendStringString(buf, key, field)
	value, err = c.bulkCmd(buf)
	writeBuffers.Put(buf)
	return
}

// BytesHGET executes <https://redis.io/commands/hget>.
// The return is nil if key does not exist.
func (c *Client) BytesHGET(key, field []byte) (value []byte, err error) {
	const prefix = "*3\r\n$4\r\nHGET\r\n$"
	buf := append(writeBuffers.Get().([]byte)[:0], prefix...)
	buf = appendBytesBytes(buf, key, field)
	value, err = c.bulkCmd(buf)
	writeBuffers.Put(buf)
	return
}

// HSET executes <https://redis.io/commands/hset>.
func (c *Client) HSET(key, field string, value []byte) (newField bool, err error) {
	const prefix = "*4\r\n$4\r\nHSET\r\n$"
	buf := append(writeBuffers.Get().([]byte)[:0], prefix...)
	buf = appendStringStringBytes(buf, key, field, value)
	created, err := c.intCmd(buf)
	writeBuffers.Put(buf)
	return created != 0, err
}

// BytesHSET executes <https://redis.io/commands/hset>.
func (c *Client) BytesHSET(key, field, value []byte) (newField bool, err error) {
	const prefix = "*4\r\n$4\r\nHSET\r\n$"
	buf := append(writeBuffers.Get().([]byte)[:0], prefix...)
	buf = appendBytesBytesBytes(buf, key, field, value)
	created, err := c.intCmd(buf)
	writeBuffers.Put(buf)
	return created != 0, err
}

// HSETString executes <https://redis.io/commands/hset>.
func (c *Client) HSETString(key, field, value string) (updated bool, err error) {
	const prefix = "*4\r\n$4\r\nHSET\r\n$"
	buf := append(writeBuffers.Get().([]byte)[:0], prefix...)
	buf = appendStringStringString(buf, key, field, value)
	replaced, err := c.intCmd(buf)
	writeBuffers.Put(buf)
	return replaced != 0, err
}

// HDEL executes <https://redis.io/commands/hdel>.
func (c *Client) HDEL(key, field string) (bool, error) {
	const prefix = "*3\r\n$4\r\nHDEL\r\n$"
	buf := append(writeBuffers.Get().([]byte)[:0], prefix...)
	buf = appendStringString(buf, key, field)
	removed, err := c.intCmd(buf)
	writeBuffers.Put(buf)
	return removed != 0, err
}

// BytesHDEL executes <https://redis.io/commands/hdel>.
func (c *Client) BytesHDEL(key, field []byte) (bool, error) {
	const prefix = "*3\r\n$4\r\nHDEL\r\n$"
	buf := append(writeBuffers.Get().([]byte)[:0], prefix...)
	buf = appendBytesBytes(buf, key, field)
	removed, err := c.intCmd(buf)
	writeBuffers.Put(buf)
	return removed != 0, err
}

func appendBytes(buf, a []byte) []byte {
	buf = strconv.AppendUint(buf, uint64(len(a)), 10)
	buf = append(buf, '\r', '\n')
	buf = append(buf, a...)
	buf = append(buf, '\r', '\n')
	return buf
}

func appendString(buf []byte, a string) []byte {
	buf = strconv.AppendUint(buf, uint64(len(a)), 10)
	buf = append(buf, '\r', '\n')
	buf = append(buf, a...)
	buf = append(buf, '\r', '\n')
	return buf
}

func appendBytesBytes(buf, a1, a2 []byte) []byte {
	buf = strconv.AppendUint(buf, uint64(len(a1)), 10)
	buf = append(buf, '\r', '\n')
	buf = append(buf, a1...)
	buf = append(buf, '\r', '\n', '$')
	buf = strconv.AppendUint(buf, uint64(len(a2)), 10)
	buf = append(buf, '\r', '\n')
	buf = append(buf, a2...)
	buf = append(buf, '\r', '\n')
	return buf
}

func appendBytesInt(buf, a1 []byte, a2 int64) []byte {
	buf = strconv.AppendUint(buf, uint64(len(a1)), 10)
	buf = append(buf, '\r', '\n')
	buf = append(buf, a1...)
	buf = append(buf, '\r', '\n', '$')

	buf = appendDecimal(buf, a2)

	buf = append(buf, '\r', '\n')
	return buf
}

func appendStringBytes(buf []byte, a1 string, a2 []byte) []byte {
	buf = strconv.AppendUint(buf, uint64(len(a1)), 10)
	buf = append(buf, '\r', '\n')
	buf = append(buf, a1...)
	buf = append(buf, '\r', '\n', '$')
	buf = strconv.AppendUint(buf, uint64(len(a2)), 10)
	buf = append(buf, '\r', '\n')
	buf = append(buf, a2...)
	buf = append(buf, '\r', '\n')
	return buf
}

func appendStringInt(buf []byte, a1 string, a2 int64) []byte {
	buf = strconv.AppendUint(buf, uint64(len(a1)), 10)
	buf = append(buf, '\r', '\n')
	buf = append(buf, a1...)
	buf = append(buf, '\r', '\n', '$')

	buf = appendDecimal(buf, a2)

	buf = append(buf, '\r', '\n')
	return buf
}

func appendStringString(buf []byte, a1, a2 string) []byte {
	buf = strconv.AppendUint(buf, uint64(len(a1)), 10)
	buf = append(buf, '\r', '\n')
	buf = append(buf, a1...)
	buf = append(buf, '\r', '\n', '$')
	buf = strconv.AppendUint(buf, uint64(len(a2)), 10)
	buf = append(buf, '\r', '\n')
	buf = append(buf, a2...)
	buf = append(buf, '\r', '\n')
	return buf
}

func appendBytesBytesBytes(buf, a1, a2, a3 []byte) []byte {
	buf = strconv.AppendUint(buf, uint64(len(a1)), 10)
	buf = append(buf, '\r', '\n')
	buf = append(buf, a1...)
	buf = append(buf, '\r', '\n', '$')
	buf = strconv.AppendUint(buf, uint64(len(a2)), 10)
	buf = append(buf, '\r', '\n')
	buf = append(buf, a2...)
	buf = append(buf, '\r', '\n', '$')
	buf = strconv.AppendUint(buf, uint64(len(a3)), 10)
	buf = append(buf, '\r', '\n')
	buf = append(buf, a3...)
	buf = append(buf, '\r', '\n')
	return buf
}

func appendBytesIntBytes(buf, a1 []byte, a2 int64, a3 []byte) []byte {
	buf = strconv.AppendUint(buf, uint64(len(a1)), 10)
	buf = append(buf, '\r', '\n')
	buf = append(buf, a1...)
	buf = append(buf, '\r', '\n', '$')

	buf = appendDecimal(buf, a2)

	buf = append(buf, '\r', '\n', '$')
	buf = strconv.AppendUint(buf, uint64(len(a3)), 10)
	buf = append(buf, '\r', '\n')
	buf = append(buf, a3...)
	buf = append(buf, '\r', '\n')
	return buf
}

func appendBytesIntInt(buf, a1 []byte, a2, a3 int64) []byte {
	buf = strconv.AppendUint(buf, uint64(len(a1)), 10)
	buf = append(buf, '\r', '\n')
	buf = append(buf, a1...)
	buf = append(buf, '\r', '\n', '$')

	buf = appendDecimal(buf, a2)

	buf = append(buf, '\r', '\n', '$')

	buf = appendDecimal(buf, a3)

	buf = append(buf, '\r', '\n')
	return buf
}

func appendStringIntBytes(buf []byte, a1 string, a2 int64, a3 []byte) []byte {
	buf = strconv.AppendUint(buf, uint64(len(a1)), 10)
	buf = append(buf, '\r', '\n')
	buf = append(buf, a1...)
	buf = append(buf, '\r', '\n', '$')

	buf = appendDecimal(buf, a2)

	buf = append(buf, '\r', '\n', '$')
	buf = strconv.AppendUint(buf, uint64(len(a3)), 10)
	buf = append(buf, '\r', '\n')
	buf = append(buf, a3...)
	buf = append(buf, '\r', '\n')
	return buf
}

func appendStringIntInt(buf []byte, a1 string, a2, a3 int64) []byte {
	buf = strconv.AppendUint(buf, uint64(len(a1)), 10)
	buf = append(buf, '\r', '\n')
	buf = append(buf, a1...)
	buf = append(buf, '\r', '\n', '$')

	buf = appendDecimal(buf, a2)

	buf = append(buf, '\r', '\n', '$')

	buf = appendDecimal(buf, a3)

	buf = append(buf, '\r', '\n')
	return buf
}

func appendStringIntString(buf []byte, a1 string, a2 int64, a3 string) []byte {
	buf = strconv.AppendUint(buf, uint64(len(a1)), 10)
	buf = append(buf, '\r', '\n')
	buf = append(buf, a1...)
	buf = append(buf, '\r', '\n', '$')

	buf = appendDecimal(buf, a2)

	buf = append(buf, '\r', '\n', '$')
	buf = strconv.AppendUint(buf, uint64(len(a3)), 10)
	buf = append(buf, '\r', '\n')
	buf = append(buf, a3...)
	buf = append(buf, '\r', '\n')
	return buf
}

func appendStringStringBytes(buf []byte, a1, a2 string, a3 []byte) []byte {
	buf = strconv.AppendUint(buf, uint64(len(a1)), 10)
	buf = append(buf, '\r', '\n')
	buf = append(buf, a1...)
	buf = append(buf, '\r', '\n', '$')
	buf = strconv.AppendUint(buf, uint64(len(a2)), 10)
	buf = append(buf, '\r', '\n')
	buf = append(buf, a2...)
	buf = append(buf, '\r', '\n', '$')
	buf = strconv.AppendUint(buf, uint64(len(a3)), 10)
	buf = append(buf, '\r', '\n')
	buf = append(buf, a3...)
	buf = append(buf, '\r', '\n')
	return buf
}

func appendStringStringString(buf []byte, a1, a2, a3 string) []byte {
	buf = strconv.AppendUint(buf, uint64(len(a1)), 10)
	buf = append(buf, '\r', '\n')
	buf = append(buf, a1...)
	buf = append(buf, '\r', '\n', '$')
	buf = strconv.AppendUint(buf, uint64(len(a2)), 10)
	buf = append(buf, '\r', '\n')
	buf = append(buf, a2...)
	buf = append(buf, '\r', '\n', '$')
	buf = strconv.AppendUint(buf, uint64(len(a3)), 10)
	buf = append(buf, '\r', '\n')
	buf = append(buf, a3...)
	buf = append(buf, '\r', '\n')
	return buf
}

func appendDecimal(buf []byte, v int64) []byte {
	sizeOffset := len(buf)
	sizeOneDecimal := v > -1e8 && v < 1e9
	if sizeOneDecimal {
		buf = append(buf, 0, '\r', '\n')
	} else {
		buf = append(buf, 0, 0, '\r', '\n')
	}

	intOffset := len(buf)
	buf = strconv.AppendInt(buf, v, 10)
	if size := len(buf) - intOffset; sizeOneDecimal {
		buf[sizeOffset] = byte(size + '0')
	} else {
		buf[sizeOffset] = byte(size/10 + '0')
		buf[sizeOffset+1] = byte(size%10 + '0')
	}
	return buf
}

func (c *Client) okCmd(buf []byte) error {
	parser := okParsers.Get().(okParser)
	if err := c.send(buf, parser); err != nil {
		return err
	}

	// await response
	err := <-parser
	okParsers.Put(parser)
	return err
}

func (c *Client) intCmd(buf []byte) (int64, error) {
	parser := intParsers.Get().(intParser)
	if err := c.send(buf, parser); err != nil {
		return 0, err
	}

	// await response
	resp := <-parser
	intParsers.Put(parser)
	return resp.Int, resp.Err
}

func (c *Client) bulkCmd(buf []byte) ([]byte, error) {
	parser := bulkParsers.Get().(bulkParser)
	if err := c.send(buf, parser); err != nil {
		return nil, err
	}

	// await response
	resp := <-parser
	bulkParsers.Put(parser)
	return resp.Bytes, resp.Err
}

func (c *Client) arrayCmd(buf []byte) ([][]byte, error) {
	parser := arrayParsers.Get().(arrayParser)
	if err := c.send(buf, parser); err != nil {
		return nil, err
	}

	// await response
	resp := <-parser
	arrayParsers.Put(parser)
	return resp.Array, resp.Err
}

func (c *Client) send(buf []byte, callback parser) error {
	var conn net.Conn
	select {
	case conn = <-c.writeSem:
		break // lock aquired
	case err := <-c.offline:
		return err
	}

	// send command
	conn.SetWriteDeadline(time.Now().Add(Timeout))
	if _, err := conn.Write(buf); err != nil {
		// The write semaphore is not released.
		select {
		case c.writeErr <- struct{}{}:
		case <-c.offline:
		}
		return err
	}

	// expect response
	c.queue <- callback

	// release lock
	c.writeSem <- conn
	return nil
}
