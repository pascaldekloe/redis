package redis

import (
	"bufio"
	"fmt"
	"strconv"
	"sync"
)

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
		switch {
		case line[0] == ':':
			c.result.integer = ParseInt(line[1 : len(line)-2])
			return true
		default:
			c.result.err = fmt.Errorf("%w; want an integer, received %.40q", errProtocol, line)
			return false
		}

	case bulkResult:
		switch {
		case line[0] == '$':
			c.result.bulk, c.result.err = readBulk(r, line)
			return c.result.err == nil
		default:
			c.result.err = fmt.Errorf("%w; want a bulk string, received %.40q", errProtocol, line)
			return false
		}

	case arrayResult:
		var array [][]byte
		switch {
		case line[0] == '*':
			// negative means null–zero must be non-nil
			if size := ParseInt(line[1 : len(line)-2]); size >= 0 {
				array = make([][]byte, size)
			}
		default:
			c.result.err = fmt.Errorf("%w; want an array, received %.40q", errProtocol, line)
			return false
		}

		// parse elements
		for i := range array {
			switch line, err := readCRLF(r); {
			case err != nil:
				c.result.err = err
				return false
			case len(line) > 2 && line[0] == '$':
				array[i], err = readBulk(r, line)
				if err != nil {
					c.result.err = err
					return false
				}
			default:
				c.result.err = fmt.Errorf("%w; element %d received %q", errProtocol, i, line)
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

func readBulk(r *bufio.Reader, line []byte) ([]byte, error) {
	if len(line) < 3 {
		return nil, fmt.Errorf("%w; received empty line: %q", errProtocol, line)
	}
	size := ParseInt(line[1 : len(line)-2])
	if size < 0 {
		return nil, nil
	}

	buf := make([]byte, size)
	if size != 0 {
		done, err := r.Read(buf)
		for done < len(buf) && err == nil {
			var more int
			more, err = r.Read(buf[done:])
			done += more
		}
		if err != nil {
			return nil, err
		}
	}

	_, err := r.Discard(2) // skip CRLF
	return buf, err
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
