package redis

// SELECT executes <https://redis.io/commands/select>.
func (c *Client) SELECT(db int64) error {
	codec := newCodec("*2\r\n$6\r\nSELECT\r\n$")
	codec.addDecimal(db)
	return c.commandOK(codec)
}

// MOVE executes <https://redis.io/commands/move>.
func (c *Client) MOVE(key string, db int64) (bool, error) {
	codec := newCodec("*3\r\n$4\r\nMOVE\r\n$")
	codec.addStringInt(key, db)
	n, err := c.commandInteger(codec)
	return n != 0, err
}

// BytesMOVE executes <https://redis.io/commands/move>.
func (c *Client) BytesMOVE(key []byte, db int64) (bool, error) {
	codec := newCodec("*3\r\n$4\r\nMOVE\r\n$")
	codec.addBytesInt(key, db)
	n, err := c.commandInteger(codec)
	return n != 0, err
}

// FLUSHDB executes <https://redis.io/commands/flushdb>.
func (c *Client) FLUSHDB(async bool) error {
	var codec *codec
	if async {
		codec = newCodec("*2\r\n$7\r\nFLUSHDB\r\n$5\r\nASYNC\r\n")
	} else {
		codec = newCodec("*1\r\n$7\r\nFLUSHDB\r\n")
	}
	return c.commandOK(codec)
}

// FLUSHALL executes <https://redis.io/commands/flushall>.
func (c *Client) FLUSHALL(async bool) error {
	var codec *codec
	if async {
		codec = newCodec("*2\r\n$8\r\nFLUSHALL\r\n$5\r\nASYNC\r\n")
	} else {
		codec = newCodec("*1\r\n$8\r\nFLUSHALL\r\n")
	}
	return c.commandOK(codec)
}

// GET executes <https://redis.io/commands/get>.
// The return is nil if key does not exist.
func (c *Client) GET(key string) (value []byte, err error) {
	codec := newCodec("*2\r\n$3\r\nGET\r\n$")
	codec.addString(key)
	return c.commandBulk(codec)
}

// BytesGET executes <https://redis.io/commands/get>.
// The return is nil if key does not exist.
func (c *Client) BytesGET(key []byte) (value []byte, err error) {
	codec := newCodec("*2\r\n$3\r\nGET\r\n$")
	codec.addBytes(key)
	return c.commandBulk(codec)
}

// SET executes <https://redis.io/commands/set>.
func (c *Client) SET(key string, value []byte) error {
	codec := newCodec("*3\r\n$3\r\nSET\r\n$")
	codec.addStringBytes(key, value)
	return c.commandOK(codec)
}

// BytesSET executes <https://redis.io/commands/set>.
func (c *Client) BytesSET(key, value []byte) error {
	codec := newCodec("*3\r\n$3\r\nSET\r\n$")
	codec.addBytesBytes(key, value)
	return c.commandOK(codec)
}

// SETString executes <https://redis.io/commands/set>.
func (c *Client) SETString(key, value string) error {
	codec := newCodec("*3\r\n$3\r\nSET\r\n$")
	codec.addStringString(key, value)
	return c.commandOK(codec)
}

// DEL executes <https://redis.io/commands/del>.
func (c *Client) DEL(key string) (bool, error) {
	codec := newCodec("*2\r\n$3\r\nDEL\r\n$")
	codec.addString(key)
	removed, err := c.commandInteger(codec)
	return removed != 0, err
}

// BytesDEL executes <https://redis.io/commands/del>.
func (c *Client) BytesDEL(key []byte) (bool, error) {
	codec := newCodec("*2\r\n$3\r\nDEL\r\n$")
	codec.addBytes(key)
	removed, err := c.commandInteger(codec)
	return removed != 0, err
}

// INCR executes <https://redis.io/commands/incr>.
func (c *Client) INCR(key string) (newValue int64, err error) {
	codec := newCodec("*2\r\n$4\r\nINCR\r\n$")
	codec.addString(key)
	return c.commandInteger(codec)
}

// BytesINCR executes <https://redis.io/commands/incr>.
func (c *Client) BytesINCR(key []byte) (newValue int64, err error) {
	codec := newCodec("*2\r\n$4\r\nINCR\r\n$")
	codec.addBytes(key)
	return c.commandInteger(codec)
}

// INCRBY executes <https://redis.io/commands/incrby>.
func (c *Client) INCRBY(key string, increment int64) (newValue int64, err error) {
	codec := newCodec("*3\r\n$6\r\nINCRBY\r\n$")
	codec.addStringInt(key, increment)
	return c.commandInteger(codec)
}

// BytesINCRBY executes <https://redis.io/commands/incrby>.
func (c *Client) BytesINCRBY(key []byte, increment int64) (newValue int64, err error) {
	codec := newCodec("*3\r\n$6\r\nINCRBY\r\n$")
	codec.addBytesInt(key, increment)
	return c.commandInteger(codec)
}

// APPEND executes <https://redis.io/commands/append>.
func (c *Client) APPEND(key string, value []byte) (newLen int64, err error) {
	codec := newCodec("*3\r\n$6\r\nAPPEND\r\n$")
	codec.addStringBytes(key, value)
	return c.commandInteger(codec)
}

// BytesAPPEND executes <https://redis.io/commands/append>.
func (c *Client) BytesAPPEND(key, value []byte) (newLen int64, err error) {
	codec := newCodec("*3\r\n$6\r\nAPPEND\r\n$")
	codec.addBytesBytes(key, value)
	return c.commandInteger(codec)
}

// APPENDString executes <https://redis.io/commands/append>.
func (c *Client) APPENDString(key, value string) (newLen int64, err error) {
	codec := newCodec("*3\r\n$6\r\nAPPEND\r\n$")
	codec.addStringString(key, value)
	return c.commandInteger(codec)
}

// LLEN executes <https://redis.io/commands/llen>.
// The return is 0 if key does not exist.
func (c *Client) LLEN(key string) (int64, error) {
	codec := newCodec("*2\r\n$4\r\nLLEN\r\n$")
	codec.addString(key)
	return c.commandInteger(codec)
}

// BytesLLEN executes <https://redis.io/commands/llen>.
// The return is 0 if key does not exist.
func (c *Client) BytesLLEN(key []byte) (int64, error) {
	codec := newCodec("*2\r\n$4\r\nLLEN\r\n$")
	codec.addBytes(key)
	return c.commandInteger(codec)
}

// LINDEX executes <https://redis.io/commands/lindex>.
// The return is nil if key does not exist.
// The return is nil if index is out of range.
func (c *Client) LINDEX(key string, index int64) (value []byte, err error) {
	codec := newCodec("*3\r\n$6\r\nLINDEX\r\n$")
	codec.addStringInt(key, index)
	return c.commandBulk(codec)
}

// BytesLINDEX executes <https://redis.io/commands/lindex>.
// The return is nil if key does not exist.
// The return is nil if index is out of range.
func (c *Client) BytesLINDEX(key []byte, index int64) (value []byte, err error) {
	codec := newCodec("*3\r\n$6\r\nLINDEX\r\n$")
	codec.addBytesInt(key, index)
	return c.commandBulk(codec)
}

// LRANGE executes <https://redis.io/commands/lrange>.
// The return is empty if key does not exist.
func (c *Client) LRANGE(key string, start, stop int64) (values [][]byte, err error) {
	codec := newCodec("*4\r\n$6\r\nLRANGE\r\n$")
	codec.addStringIntInt(key, start, stop)
	return c.commandArray(codec)
}

// BytesLRANGE executes <https://redis.io/commands/lrange>.
// The return is empty if key does not exist.
func (c *Client) BytesLRANGE(key []byte, start, stop int64) (values [][]byte, err error) {
	codec := newCodec("*4\r\n$6\r\nLRANGE\r\n$")
	codec.addBytesIntInt(key, start, stop)
	return c.commandArray(codec)
}

// LPOP executes <https://redis.io/commands/lpop>.
// The return is nil if key does not exist.
func (c *Client) LPOP(key string) (value []byte, err error) {
	codec := newCodec("*2\r\n$4\r\nLPOP\r\n$")
	codec.addString(key)
	return c.commandBulk(codec)
}

// BytesLPOP executes <https://redis.io/commands/lpop>.
// The return is nil if key does not exist.
func (c *Client) BytesLPOP(key []byte) (value []byte, err error) {
	codec := newCodec("*2\r\n$4\r\nLPOP\r\n$")
	codec.addBytes(key)
	return c.commandBulk(codec)
}

// RPOP executes <https://redis.io/commands/rpop>.
// The return is nil if key does not exist.
func (c *Client) RPOP(key string) (value []byte, err error) {
	codec := newCodec("*2\r\n$4\r\nRPOP\r\n$")
	codec.addString(key)
	return c.commandBulk(codec)
}

// BytesRPOP executes <https://redis.io/commands/rpop>.
// The return is nil if key does not exist.
func (c *Client) BytesRPOP(key []byte) (value []byte, err error) {
	codec := newCodec("*2\r\n$4\r\nRPOP\r\n$")
	codec.addBytes(key)
	return c.commandBulk(codec)
}

// LTRIM executes <https://redis.io/commands/ltrim>.
func (c *Client) LTRIM(key string, start, stop int64) error {
	codec := newCodec("*4\r\n$5\r\nLTRIM\r\n$")
	codec.addStringIntInt(key, start, stop)
	return c.commandOK(codec)
}

// BytesLTRIM executes <https://redis.io/commands/ltrim>.
func (c *Client) BytesLTRIM(key []byte, start, stop int64) error {
	codec := newCodec("*4\r\n$5\r\nLTRIM\r\n$")
	codec.addBytesIntInt(key, start, stop)
	return c.commandOK(codec)
}

// LSET executes <https://redis.io/commands/lset>.
func (c *Client) LSET(key string, index int64, value []byte) error {
	codec := newCodec("*4\r\n$4\r\nLSET\r\n$")
	codec.addStringIntBytes(key, index, value)
	return c.commandOK(codec)
}

// LSETString executes <https://redis.io/commands/lset>.
func (c *Client) LSETString(key string, index int64, value string) error {
	codec := newCodec("*4\r\n$4\r\nLSET\r\n$")
	codec.addStringIntString(key, index, value)
	return c.commandOK(codec)
}

// BytesLSET executes <https://redis.io/commands/lset>.
func (c *Client) BytesLSET(key []byte, index int64, value []byte) error {
	codec := newCodec("*4\r\n$4\r\nLSET\r\n$")
	codec.addBytesIntBytes(key, index, value)
	return c.commandOK(codec)
}

// LPUSH executes <https://redis.io/commands/lpush>.
func (c *Client) LPUSH(key string, value []byte) (newLen int64, err error) {
	codec := newCodec("*3\r\n$5\r\nLPUSH\r\n$")
	codec.addStringBytes(key, value)
	return c.commandInteger(codec)
}

// BytesLPUSH executes <https://redis.io/commands/lpush>.
func (c *Client) BytesLPUSH(key, value []byte) (newLen int64, err error) {
	codec := newCodec("*3\r\n$5\r\nLPUSH\r\n$")
	codec.addBytesBytes(key, value)
	return c.commandInteger(codec)
}

// LPUSHString executes <https://redis.io/commands/lpush>.
func (c *Client) LPUSHString(key, value string) (newLen int64, err error) {
	codec := newCodec("*3\r\n$5\r\nLPUSH\r\n$")
	codec.addStringString(key, value)
	return c.commandInteger(codec)
}

// RPUSH executes <https://redis.io/commands/rpush>.
func (c *Client) RPUSH(key string, value []byte) (newLen int64, err error) {
	codec := newCodec("*3\r\n$5\r\nRPUSH\r\n$")
	codec.addStringBytes(key, value)
	return c.commandInteger(codec)
}

// BytesRPUSH executes <https://redis.io/commands/rpush>.
func (c *Client) BytesRPUSH(key, value []byte) (newLen int64, err error) {
	codec := newCodec("*3\r\n$5\r\nRPUSH\r\n$")
	codec.addBytesBytes(key, value)
	return c.commandInteger(codec)
}

// RPUSHString executes <https://redis.io/commands/rpush>.
func (c *Client) RPUSHString(key, value string) (newLen int64, err error) {
	codec := newCodec("*3\r\n$5\r\nRPUSH\r\n$")
	codec.addStringString(key, value)
	return c.commandInteger(codec)
}

// HGET executes <https://redis.io/commands/hget>.
// The return is nil if key does not exist.
func (c *Client) HGET(key, field string) (value []byte, err error) {
	codec := newCodec("*3\r\n$4\r\nHGET\r\n$")
	codec.addStringString(key, field)
	return c.commandBulk(codec)
}

// BytesHGET executes <https://redis.io/commands/hget>.
// The return is nil if key does not exist.
func (c *Client) BytesHGET(key, field []byte) (value []byte, err error) {
	codec := newCodec("*3\r\n$4\r\nHGET\r\n$")
	codec.addBytesBytes(key, field)
	return c.commandBulk(codec)
}

// HSET executes <https://redis.io/commands/hset>.
func (c *Client) HSET(key, field string, value []byte) (newField bool, err error) {
	codec := newCodec("*4\r\n$4\r\nHSET\r\n$")
	codec.addStringStringBytes(key, field, value)
	created, err := c.commandInteger(codec)
	return created != 0, err
}

// BytesHSET executes <https://redis.io/commands/hset>.
func (c *Client) BytesHSET(key, field, value []byte) (newField bool, err error) {
	codec := newCodec("*4\r\n$4\r\nHSET\r\n$")
	codec.addBytesBytesBytes(key, field, value)
	created, err := c.commandInteger(codec)
	return created != 0, err
}

// HSETString executes <https://redis.io/commands/hset>.
func (c *Client) HSETString(key, field, value string) (updated bool, err error) {
	codec := newCodec("*4\r\n$4\r\nHSET\r\n$")
	codec.addStringStringString(key, field, value)
	replaced, err := c.commandInteger(codec)
	return replaced != 0, err
}

// HDEL executes <https://redis.io/commands/hdel>.
func (c *Client) HDEL(key, field string) (bool, error) {
	codec := newCodec("*3\r\n$4\r\nHDEL\r\n$")
	codec.addStringString(key, field)
	removed, err := c.commandInteger(codec)
	return removed != 0, err
}

// BytesHDEL executes <https://redis.io/commands/hdel>.
func (c *Client) BytesHDEL(key, field []byte) (bool, error) {
	codec := newCodec("*3\r\n$4\r\nHDEL\r\n$")
	codec.addBytesBytes(key, field)
	removed, err := c.commandInteger(codec)
	return removed != 0, err
}
