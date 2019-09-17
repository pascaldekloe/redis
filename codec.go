package redis

import (
	"bufio"
	"errors"
	"fmt"
	"strconv"
	"sync"
)

// ErrMapSlices rejects execution due to a broken mapping.
var ErrMapSlices = errors.New("redis: number of keys doesn't match number of values")

// Result is a generic response container.
type result struct {
	err     error
	integer int64
	bulk    []byte
	array   [][]byte
}

type resultType byte

const (
	okResult resultType = iota
	integerResult
	bulkResult
	arrayResult
)

type codec struct {
	buf      []byte
	received chan struct{} // response reception
	result
	resultType
}

var codecPool = sync.Pool{
	New: func() interface{} {
		return &codec{
			buf:      make([]byte, 256),
			received: make(chan struct{}),
		}
	},
}

func newCodec(prefix string) *codec {
	c := codecPool.Get().(*codec)
	c.buf = append(c.buf[:0], prefix...)
	return c
}

func newCodecN(n int, prefix string) *codec {
	c := codecPool.Get().(*codec)
	c.buf = append(c.buf[:0], '*')
	c.buf = strconv.AppendUint(c.buf, uint64(n), 10)
	c.buf = append(c.buf, prefix...)
	return c
}

func (c *codec) decode(r *bufio.Reader) bool {
	line, err := readCRLF(r)
	if err != nil {
		c.result.err = err
		return false
	}
	if len(line) < 3 {
		c.result.err = fmt.Errorf("%w; received empty line %q", errProtocol, line)
		return false
	}
	if line[0] == '-' {
		c.result.err = ServerError(line[1 : len(line)-2])
		return true
	}

	switch c.resultType {
	case okResult:
		switch {
		case line[0] == '+' && line[1] == 'O' && line[2] == 'K':
			return true
		case line[0] == '$' && line[1] == '-' && line[2] == '1':
			c.result.err = errNull
			return true
		default:
			c.result.err = fmt.Errorf("%w; want OK simple string, received %.40q", errProtocol, line)
			return false
		}

	case integerResult:
		if line[0] != ':' {
			c.result.err = fmt.Errorf("%w; want an integer, received %.40q", errProtocol, line)
			return false
		}
		c.result.integer = ParseInt(line[1 : len(line)-2])
		return true

	case bulkResult:
		if line[0] != '$' {
			c.result.err = fmt.Errorf("%w; want a bulk string, received %.40q", errProtocol, line)
			return false
		}
		c.result.err = readBulk(r, &c.result.bulk, line)
		return c.result.err == nil

	case arrayResult:
		if line[0] != '*' {
			c.result.err = fmt.Errorf("%w; want an array, received %.40q", errProtocol, line)
			return false
		}
		var array [][]byte
		// negative means null–zero must be non-nil
		if size := ParseInt(line[1 : len(line)-2]); size >= 0 {
			array = make([][]byte, size)
		}

		// parse elements
		for i := range array {
			line, err := readCRLF(r)
			if err != nil {
				c.result.err = err
				return false
			}
			if len(line) < 3 || line[0] != '$' {
				c.result.err = fmt.Errorf("%w; array element %d received %q", errProtocol, i, line)
				return false
			}
			err = readBulk(r, &array[i], line)
			if err != nil {
				c.result.err = err
				return false
			}
		}

		c.result.array = array
		return true

	default:
		// unreachable
		c.result.err = fmt.Errorf("redis: result type %d not in use", c.resultType)
		return false
	}
}

// WARNING: line stays only valid until the next read on r.
func readCRLF(r *bufio.Reader) (line []byte, err error) {
	line, err = r.ReadSlice('\n')
	if err != nil {
		if err == bufio.ErrBufferFull {
			err = fmt.Errorf("%w; CRLF exceeds %d bytes: %.40q…", errProtocol, r.Size(), line)
		}
		return nil, err
	}
	return line, nil
}

func readBulk(r *bufio.Reader, dest *[]byte, line []byte) error {
	if len(line) < 3 {
		return fmt.Errorf("%w; received empty line: %q", errProtocol, line)
	}
	size := ParseInt(line[1 : len(line)-2])
	if size < 0 {
		return nil
	}

	*dest = make([]byte, size)
	if size != 0 {
		done, err := r.Read(*dest)
		for done < len(*dest) && err == nil {
			var more int
			more, err = r.Read((*dest)[done:])
			done += more
		}
		if err != nil {
			*dest = nil
			return err
		}
	}

	_, err := r.Discard(2) // skip CRLF
	return err
}

func (codec *codec) addBytes(a []byte) {
	codec.buf = strconv.AppendUint(codec.buf, uint64(len(a)), 10)
	codec.buf = append(codec.buf, '\r', '\n')
	codec.buf = append(codec.buf, a...)
	codec.buf = append(codec.buf, '\r', '\n')
}

func (codec *codec) addString(a string) {
	codec.buf = strconv.AppendUint(codec.buf, uint64(len(a)), 10)
	codec.buf = append(codec.buf, '\r', '\n')
	codec.buf = append(codec.buf, a...)
	codec.buf = append(codec.buf, '\r', '\n')
}

func (codec *codec) addBytesBytes(a1, a2 []byte) {
	codec.buf = strconv.AppendUint(codec.buf, uint64(len(a1)), 10)
	codec.buf = append(codec.buf, '\r', '\n')
	codec.buf = append(codec.buf, a1...)
	codec.buf = append(codec.buf, '\r', '\n', '$')
	codec.buf = strconv.AppendUint(codec.buf, uint64(len(a2)), 10)
	codec.buf = append(codec.buf, '\r', '\n')
	codec.buf = append(codec.buf, a2...)
	codec.buf = append(codec.buf, '\r', '\n')
}

func (codec *codec) addBytesBytesList(a1 []byte, a2 [][]byte) {
	codec.buf = strconv.AppendUint(codec.buf, uint64(len(a1)), 10)
	codec.buf = append(codec.buf, '\r', '\n')
	codec.buf = append(codec.buf, a1...)
	for _, b := range a2 {
		codec.buf = append(codec.buf, '\r', '\n', '$')
		codec.buf = strconv.AppendUint(codec.buf, uint64(len(b)), 10)
		codec.buf = append(codec.buf, '\r', '\n')
		codec.buf = append(codec.buf, b...)
	}
	codec.buf = append(codec.buf, '\r', '\n')
}

func (codec *codec) addBytesInt(a1 []byte, a2 int64) {
	codec.buf = strconv.AppendUint(codec.buf, uint64(len(a1)), 10)
	codec.buf = append(codec.buf, '\r', '\n')
	codec.buf = append(codec.buf, a1...)
	codec.buf = append(codec.buf, '\r', '\n', '$')

	codec.decimal(a2)

	codec.buf = append(codec.buf, '\r', '\n')
}

func (codec *codec) addStringBytes(a1 string, a2 []byte) {
	codec.buf = strconv.AppendUint(codec.buf, uint64(len(a1)), 10)
	codec.buf = append(codec.buf, '\r', '\n')
	codec.buf = append(codec.buf, a1...)
	codec.buf = append(codec.buf, '\r', '\n', '$')
	codec.buf = strconv.AppendUint(codec.buf, uint64(len(a2)), 10)
	codec.buf = append(codec.buf, '\r', '\n')
	codec.buf = append(codec.buf, a2...)
	codec.buf = append(codec.buf, '\r', '\n')
}

func (codec *codec) addStringInt(a1 string, a2 int64) {
	codec.buf = strconv.AppendUint(codec.buf, uint64(len(a1)), 10)
	codec.buf = append(codec.buf, '\r', '\n')
	codec.buf = append(codec.buf, a1...)
	codec.buf = append(codec.buf, '\r', '\n', '$')

	codec.decimal(a2)

	codec.buf = append(codec.buf, '\r', '\n')
}

func (codec *codec) addStringString(a1, a2 string) {
	codec.buf = strconv.AppendUint(codec.buf, uint64(len(a1)), 10)
	codec.buf = append(codec.buf, '\r', '\n')
	codec.buf = append(codec.buf, a1...)
	codec.buf = append(codec.buf, '\r', '\n', '$')
	codec.buf = strconv.AppendUint(codec.buf, uint64(len(a2)), 10)
	codec.buf = append(codec.buf, '\r', '\n')
	codec.buf = append(codec.buf, a2...)
	codec.buf = append(codec.buf, '\r', '\n')
}

func (codec *codec) addStringStringList(a1 string, a2 []string) {
	codec.buf = strconv.AppendUint(codec.buf, uint64(len(a1)), 10)
	codec.buf = append(codec.buf, '\r', '\n')
	codec.buf = append(codec.buf, a1...)
	for _, s := range a2 {
		codec.buf = append(codec.buf, '\r', '\n', '$')
		codec.buf = strconv.AppendUint(codec.buf, uint64(len(s)), 10)
		codec.buf = append(codec.buf, '\r', '\n')
		codec.buf = append(codec.buf, s...)
	}
	codec.buf = append(codec.buf, '\r', '\n')
}

func (codec *codec) addBytesBytesBytes(a1, a2, a3 []byte) {
	codec.buf = strconv.AppendUint(codec.buf, uint64(len(a1)), 10)
	codec.buf = append(codec.buf, '\r', '\n')
	codec.buf = append(codec.buf, a1...)
	codec.buf = append(codec.buf, '\r', '\n', '$')
	codec.buf = strconv.AppendUint(codec.buf, uint64(len(a2)), 10)
	codec.buf = append(codec.buf, '\r', '\n')
	codec.buf = append(codec.buf, a2...)
	codec.buf = append(codec.buf, '\r', '\n', '$')
	codec.buf = strconv.AppendUint(codec.buf, uint64(len(a3)), 10)
	codec.buf = append(codec.buf, '\r', '\n')
	codec.buf = append(codec.buf, a3...)
	codec.buf = append(codec.buf, '\r', '\n')
}

func (codec *codec) addBytesBytesBytesMapLists(a1 []byte, a2, a3 [][]byte) error {
	if len(a2) != len(a3) {
		return ErrMapSlices
	}
	codec.buf = strconv.AppendUint(codec.buf, uint64(len(a1)), 10)
	codec.buf = append(codec.buf, '\r', '\n')
	codec.buf = append(codec.buf, a1...)
	for i, key := range a2 {
		value := a3[i]
		codec.buf = append(codec.buf, '\r', '\n', '$')
		codec.buf = strconv.AppendUint(codec.buf, uint64(len(key)), 10)
		codec.buf = append(codec.buf, '\r', '\n')
		codec.buf = append(codec.buf, key...)
		codec.buf = append(codec.buf, '\r', '\n', '$')
		codec.buf = strconv.AppendUint(codec.buf, uint64(len(value)), 10)
		codec.buf = append(codec.buf, '\r', '\n')
		codec.buf = append(codec.buf, value...)
	}
	codec.buf = append(codec.buf, '\r', '\n')
	return nil
}

func (codec *codec) addBytesIntBytes(a1 []byte, a2 int64, a3 []byte) {
	codec.buf = strconv.AppendUint(codec.buf, uint64(len(a1)), 10)
	codec.buf = append(codec.buf, '\r', '\n')
	codec.buf = append(codec.buf, a1...)
	codec.buf = append(codec.buf, '\r', '\n', '$')

	codec.decimal(a2)

	codec.buf = append(codec.buf, '\r', '\n', '$')
	codec.buf = strconv.AppendUint(codec.buf, uint64(len(a3)), 10)
	codec.buf = append(codec.buf, '\r', '\n')
	codec.buf = append(codec.buf, a3...)
	codec.buf = append(codec.buf, '\r', '\n')
}

func (codec *codec) addBytesIntInt(a1 []byte, a2, a3 int64) {
	codec.buf = strconv.AppendUint(codec.buf, uint64(len(a1)), 10)
	codec.buf = append(codec.buf, '\r', '\n')
	codec.buf = append(codec.buf, a1...)
	codec.buf = append(codec.buf, '\r', '\n', '$')

	codec.decimal(a2)

	codec.buf = append(codec.buf, '\r', '\n', '$')

	codec.decimal(a3)

	codec.buf = append(codec.buf, '\r', '\n')
}

func (codec *codec) addStringIntBytes(a1 string, a2 int64, a3 []byte) {
	codec.buf = strconv.AppendUint(codec.buf, uint64(len(a1)), 10)
	codec.buf = append(codec.buf, '\r', '\n')
	codec.buf = append(codec.buf, a1...)
	codec.buf = append(codec.buf, '\r', '\n', '$')

	codec.decimal(a2)

	codec.buf = append(codec.buf, '\r', '\n', '$')
	codec.buf = strconv.AppendUint(codec.buf, uint64(len(a3)), 10)
	codec.buf = append(codec.buf, '\r', '\n')
	codec.buf = append(codec.buf, a3...)
	codec.buf = append(codec.buf, '\r', '\n')
}

func (codec *codec) addStringIntInt(a1 string, a2, a3 int64) {
	codec.buf = strconv.AppendUint(codec.buf, uint64(len(a1)), 10)
	codec.buf = append(codec.buf, '\r', '\n')
	codec.buf = append(codec.buf, a1...)
	codec.buf = append(codec.buf, '\r', '\n', '$')

	codec.decimal(a2)

	codec.buf = append(codec.buf, '\r', '\n', '$')

	codec.decimal(a3)

	codec.buf = append(codec.buf, '\r', '\n')
}

func (codec *codec) addStringIntString(a1 string, a2 int64, a3 string) {
	codec.buf = strconv.AppendUint(codec.buf, uint64(len(a1)), 10)
	codec.buf = append(codec.buf, '\r', '\n')
	codec.buf = append(codec.buf, a1...)
	codec.buf = append(codec.buf, '\r', '\n', '$')

	codec.decimal(a2)

	codec.buf = append(codec.buf, '\r', '\n', '$')
	codec.buf = strconv.AppendUint(codec.buf, uint64(len(a3)), 10)
	codec.buf = append(codec.buf, '\r', '\n')
	codec.buf = append(codec.buf, a3...)
	codec.buf = append(codec.buf, '\r', '\n')
}

func (codec *codec) addStringStringBytes(a1, a2 string, a3 []byte) {
	codec.buf = strconv.AppendUint(codec.buf, uint64(len(a1)), 10)
	codec.buf = append(codec.buf, '\r', '\n')
	codec.buf = append(codec.buf, a1...)
	codec.buf = append(codec.buf, '\r', '\n', '$')
	codec.buf = strconv.AppendUint(codec.buf, uint64(len(a2)), 10)
	codec.buf = append(codec.buf, '\r', '\n')
	codec.buf = append(codec.buf, a2...)
	codec.buf = append(codec.buf, '\r', '\n', '$')
	codec.buf = strconv.AppendUint(codec.buf, uint64(len(a3)), 10)
	codec.buf = append(codec.buf, '\r', '\n')
	codec.buf = append(codec.buf, a3...)
	codec.buf = append(codec.buf, '\r', '\n')
}

func (codec *codec) addStringStringString(a1, a2, a3 string) {
	codec.buf = strconv.AppendUint(codec.buf, uint64(len(a1)), 10)
	codec.buf = append(codec.buf, '\r', '\n')
	codec.buf = append(codec.buf, a1...)
	codec.buf = append(codec.buf, '\r', '\n', '$')
	codec.buf = strconv.AppendUint(codec.buf, uint64(len(a2)), 10)
	codec.buf = append(codec.buf, '\r', '\n')
	codec.buf = append(codec.buf, a2...)
	codec.buf = append(codec.buf, '\r', '\n', '$')
	codec.buf = strconv.AppendUint(codec.buf, uint64(len(a3)), 10)
	codec.buf = append(codec.buf, '\r', '\n')
	codec.buf = append(codec.buf, a3...)
	codec.buf = append(codec.buf, '\r', '\n')
}

func (codec *codec) addStringStringBytesMapLists(a1 string, a2 []string, a3 [][]byte) error {
	if len(a2) != len(a3) {
		return ErrMapSlices
	}
	codec.buf = strconv.AppendUint(codec.buf, uint64(len(a1)), 10)
	codec.buf = append(codec.buf, '\r', '\n')
	codec.buf = append(codec.buf, a1...)
	for i, key := range a2 {
		value := a3[i]
		codec.buf = append(codec.buf, '\r', '\n', '$')
		codec.buf = strconv.AppendUint(codec.buf, uint64(len(key)), 10)
		codec.buf = append(codec.buf, '\r', '\n')
		codec.buf = append(codec.buf, key...)
		codec.buf = append(codec.buf, '\r', '\n', '$')
		codec.buf = strconv.AppendUint(codec.buf, uint64(len(value)), 10)
		codec.buf = append(codec.buf, '\r', '\n')
		codec.buf = append(codec.buf, value...)
	}
	codec.buf = append(codec.buf, '\r', '\n')
	return nil
}

func (codec *codec) addStringStringStringMapLists(a1 string, a2, a3 []string) error {
	if len(a2) != len(a3) {
		return ErrMapSlices
	}
	codec.buf = strconv.AppendUint(codec.buf, uint64(len(a1)), 10)
	codec.buf = append(codec.buf, '\r', '\n')
	codec.buf = append(codec.buf, a1...)
	for i, key := range a2 {
		value := a3[i]
		codec.buf = append(codec.buf, '\r', '\n', '$')
		codec.buf = strconv.AppendUint(codec.buf, uint64(len(key)), 10)
		codec.buf = append(codec.buf, '\r', '\n')
		codec.buf = append(codec.buf, key...)
		codec.buf = append(codec.buf, '\r', '\n', '$')
		codec.buf = strconv.AppendUint(codec.buf, uint64(len(value)), 10)
		codec.buf = append(codec.buf, '\r', '\n')
		codec.buf = append(codec.buf, value...)
	}
	codec.buf = append(codec.buf, '\r', '\n')
	return nil
}

func (codec *codec) addDecimal(v int64) {
	codec.decimal(v)
	codec.buf = append(codec.buf, '\r', '\n')
}

func (codec *codec) decimal(v int64) {
	sizeOffset := len(codec.buf)
	sizeSingleDigit := v > -1e8 && v < 1e9
	if sizeSingleDigit {
		codec.buf = append(codec.buf, 0, '\r', '\n')
	} else {
		codec.buf = append(codec.buf, 0, 0, '\r', '\n')
	}

	valueOffset := len(codec.buf)
	codec.buf = strconv.AppendInt(codec.buf, v, 10)
	size := len(codec.buf) - valueOffset
	if sizeSingleDigit {
		codec.buf[sizeOffset] = byte(size + '0')
	} else { // two digits
		codec.buf[sizeOffset] = byte(size/10 + '0')
		codec.buf[sizeOffset+1] = byte(size%10 + '0')
	}
}
