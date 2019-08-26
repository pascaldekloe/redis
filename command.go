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

func appendBytesBytesBytes(buf []byte, a1, a2, a3 []byte) []byte {
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
