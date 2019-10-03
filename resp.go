package redis

import (
	"bufio"
	"errors"
	"fmt"
	"strconv"
	"sync"
)

// ErrMapSlices rejects execution due to a broken mapping.
var errMapSlices = errors.New("redis: number of keys doesn't match number of values")

func decodeOK(r *bufio.Reader) (userErr, fatalErr error) {
	line, err := readCRLF(r)
	switch {
	case err != nil:
		return nil, err
	case len(line) < 3:
		return nil, fmt.Errorf("%w; received empty line %q", errProtocol, line)
	case line[0] == '-':
		return ServerError(line[1 : len(line)-2]), nil
	case line[0] == '+' && line[1] == 'O' && line[2] == 'K':
		return nil, nil
	case line[0] == '$' && line[1] == '-' && line[2] == '1':
		return errNull, nil
	}
	return nil, fmt.Errorf("%w; want OK simple string, received %.40q", errProtocol, line)
}

func decodeInteger(r *bufio.Reader) (integer int64, userErr, fatalErr error) {
	line, err := readCRLF(r)
	switch {
	case err != nil:
		return 0, nil, err
	case len(line) < 3:
		return 0, nil, fmt.Errorf("%w; received empty line %q", errProtocol, line)
	case line[0] == '-':
		return 0, ServerError(line[1 : len(line)-2]), nil
	case line[0] != ':':
		return 0, nil, fmt.Errorf("%w; want an integer, received %.40q", errProtocol, line)
	}
	return ParseInt(line[1 : len(line)-2]), nil, nil
}

func decodeBulk(r *bufio.Reader) (bulk []byte, userErr, fatalErr error) {
	line, err := readCRLF(r)
	switch {
	case err != nil:
		return nil, nil, err
	case len(line) < 3:
		return nil, nil, fmt.Errorf("%w; received empty line %q", errProtocol, line)
	case line[0] == '-':
		return nil, ServerError(line[1 : len(line)-2]), nil
	case line[0] != '$':
		return nil, nil, fmt.Errorf("%w; want a bulk string, received %.40q", errProtocol, line)
	}
	err = readBulk(r, &bulk, line)
	return bulk, nil, err
}

func decodeArray(r *bufio.Reader) (array [][]byte, userErr, fatalErr error) {
	line, err := readCRLF(r)
	switch {
	case err != nil:
		return nil, nil, err
	case len(line) < 3:
		return nil, nil, fmt.Errorf("%w; received empty line %q", errProtocol, line)
	case line[0] == '-':
		return nil, ServerError(line[1 : len(line)-2]), nil
	case line[0] != '*':
		return nil, nil, fmt.Errorf("%w; want an array, received %.40q", errProtocol, line)
	}

	// negative means null–zero must be non-nil
	if size := ParseInt(line[1 : len(line)-2]); size >= 0 {
		array = make([][]byte, size)
	}
	for i := range array {
		line, err := readCRLF(r)
		if err != nil {
			return nil, nil, err
		}
		if len(line) < 3 || line[0] != '$' {
			return nil, nil, fmt.Errorf("%w; array element %d received %q", errProtocol, i, line)
		}
		err = readBulk(r, &array[i], line)
		if err != nil {
			return nil, nil, err
		}
	}
	return array, nil, nil
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

type request struct {
	buf     []byte
	receive chan *bufio.Reader
}

func (r *request) free() {
	requestPool.Put(r)
}

var requestPool = sync.Pool{
	New: func() interface{} {
		return &request{
			buf:     make([]byte, 256),
			receive: make(chan *bufio.Reader),
		}
	},
}

func newRequest(prefix string) *request {
	r := requestPool.Get().(*request)
	r.buf = append(r.buf[:0], prefix...)
	return r
}

func newRequestSize(n int, prefix string) *request {
	r := requestPool.Get().(*request)
	r.buf = append(r.buf[:0], '*')
	r.buf = strconv.AppendUint(r.buf, uint64(n), 10)
	r.buf = append(r.buf, prefix...)
	return r
}

func (r *request) addBytes(a []byte) {
	r.buf = strconv.AppendUint(r.buf, uint64(len(a)), 10)
	r.buf = append(r.buf, '\r', '\n')
	r.buf = append(r.buf, a...)
	r.buf = append(r.buf, '\r', '\n')
}

func (r *request) addString(a string) {
	r.buf = strconv.AppendUint(r.buf, uint64(len(a)), 10)
	r.buf = append(r.buf, '\r', '\n')
	r.buf = append(r.buf, a...)
	r.buf = append(r.buf, '\r', '\n')
}

func (r *request) addBytesBytes(a1, a2 []byte) {
	r.buf = strconv.AppendUint(r.buf, uint64(len(a1)), 10)
	r.buf = append(r.buf, '\r', '\n')
	r.buf = append(r.buf, a1...)
	r.buf = append(r.buf, '\r', '\n', '$')
	r.buf = strconv.AppendUint(r.buf, uint64(len(a2)), 10)
	r.buf = append(r.buf, '\r', '\n')
	r.buf = append(r.buf, a2...)
	r.buf = append(r.buf, '\r', '\n')
}

func (r *request) addBytesBytesList(a1 []byte, a2 [][]byte) {
	r.buf = strconv.AppendUint(r.buf, uint64(len(a1)), 10)
	r.buf = append(r.buf, '\r', '\n')
	r.buf = append(r.buf, a1...)
	for _, b := range a2 {
		r.buf = append(r.buf, '\r', '\n', '$')
		r.buf = strconv.AppendUint(r.buf, uint64(len(b)), 10)
		r.buf = append(r.buf, '\r', '\n')
		r.buf = append(r.buf, b...)
	}
	r.buf = append(r.buf, '\r', '\n')
}

func (r *request) addBytesInt(a1 []byte, a2 int64) {
	r.buf = strconv.AppendUint(r.buf, uint64(len(a1)), 10)
	r.buf = append(r.buf, '\r', '\n')
	r.buf = append(r.buf, a1...)
	r.buf = append(r.buf, '\r', '\n', '$')

	r.decimal(a2)

	r.buf = append(r.buf, '\r', '\n')
}

func (r *request) addStringBytes(a1 string, a2 []byte) {
	r.buf = strconv.AppendUint(r.buf, uint64(len(a1)), 10)
	r.buf = append(r.buf, '\r', '\n')
	r.buf = append(r.buf, a1...)
	r.buf = append(r.buf, '\r', '\n', '$')
	r.buf = strconv.AppendUint(r.buf, uint64(len(a2)), 10)
	r.buf = append(r.buf, '\r', '\n')
	r.buf = append(r.buf, a2...)
	r.buf = append(r.buf, '\r', '\n')
}

func (r *request) addStringInt(a1 string, a2 int64) {
	r.buf = strconv.AppendUint(r.buf, uint64(len(a1)), 10)
	r.buf = append(r.buf, '\r', '\n')
	r.buf = append(r.buf, a1...)
	r.buf = append(r.buf, '\r', '\n', '$')

	r.decimal(a2)

	r.buf = append(r.buf, '\r', '\n')
}

func (r *request) addStringString(a1, a2 string) {
	r.buf = strconv.AppendUint(r.buf, uint64(len(a1)), 10)
	r.buf = append(r.buf, '\r', '\n')
	r.buf = append(r.buf, a1...)
	r.buf = append(r.buf, '\r', '\n', '$')
	r.buf = strconv.AppendUint(r.buf, uint64(len(a2)), 10)
	r.buf = append(r.buf, '\r', '\n')
	r.buf = append(r.buf, a2...)
	r.buf = append(r.buf, '\r', '\n')
}

func (r *request) addStringStringList(a1 string, a2 []string) {
	r.buf = strconv.AppendUint(r.buf, uint64(len(a1)), 10)
	r.buf = append(r.buf, '\r', '\n')
	r.buf = append(r.buf, a1...)
	for _, s := range a2 {
		r.buf = append(r.buf, '\r', '\n', '$')
		r.buf = strconv.AppendUint(r.buf, uint64(len(s)), 10)
		r.buf = append(r.buf, '\r', '\n')
		r.buf = append(r.buf, s...)
	}
	r.buf = append(r.buf, '\r', '\n')
}

func (r *request) addBytesBytesBytes(a1, a2, a3 []byte) {
	r.buf = strconv.AppendUint(r.buf, uint64(len(a1)), 10)
	r.buf = append(r.buf, '\r', '\n')
	r.buf = append(r.buf, a1...)
	r.buf = append(r.buf, '\r', '\n', '$')
	r.buf = strconv.AppendUint(r.buf, uint64(len(a2)), 10)
	r.buf = append(r.buf, '\r', '\n')
	r.buf = append(r.buf, a2...)
	r.buf = append(r.buf, '\r', '\n', '$')
	r.buf = strconv.AppendUint(r.buf, uint64(len(a3)), 10)
	r.buf = append(r.buf, '\r', '\n')
	r.buf = append(r.buf, a3...)
	r.buf = append(r.buf, '\r', '\n')
}

func (r *request) addBytesBytesBytesMapLists(a1 []byte, a2, a3 [][]byte) error {
	if len(a2) != len(a3) {
		return errMapSlices
	}
	r.buf = strconv.AppendUint(r.buf, uint64(len(a1)), 10)
	r.buf = append(r.buf, '\r', '\n')
	r.buf = append(r.buf, a1...)
	for i, key := range a2 {
		value := a3[i]
		r.buf = append(r.buf, '\r', '\n', '$')
		r.buf = strconv.AppendUint(r.buf, uint64(len(key)), 10)
		r.buf = append(r.buf, '\r', '\n')
		r.buf = append(r.buf, key...)
		r.buf = append(r.buf, '\r', '\n', '$')
		r.buf = strconv.AppendUint(r.buf, uint64(len(value)), 10)
		r.buf = append(r.buf, '\r', '\n')
		r.buf = append(r.buf, value...)
	}
	r.buf = append(r.buf, '\r', '\n')
	return nil
}

func (r *request) addBytesIntBytes(a1 []byte, a2 int64, a3 []byte) {
	r.buf = strconv.AppendUint(r.buf, uint64(len(a1)), 10)
	r.buf = append(r.buf, '\r', '\n')
	r.buf = append(r.buf, a1...)
	r.buf = append(r.buf, '\r', '\n', '$')

	r.decimal(a2)

	r.buf = append(r.buf, '\r', '\n', '$')
	r.buf = strconv.AppendUint(r.buf, uint64(len(a3)), 10)
	r.buf = append(r.buf, '\r', '\n')
	r.buf = append(r.buf, a3...)
	r.buf = append(r.buf, '\r', '\n')
}

func (r *request) addBytesIntInt(a1 []byte, a2, a3 int64) {
	r.buf = strconv.AppendUint(r.buf, uint64(len(a1)), 10)
	r.buf = append(r.buf, '\r', '\n')
	r.buf = append(r.buf, a1...)
	r.buf = append(r.buf, '\r', '\n', '$')

	r.decimal(a2)

	r.buf = append(r.buf, '\r', '\n', '$')

	r.decimal(a3)

	r.buf = append(r.buf, '\r', '\n')
}

func (r *request) addStringIntBytes(a1 string, a2 int64, a3 []byte) {
	r.buf = strconv.AppendUint(r.buf, uint64(len(a1)), 10)
	r.buf = append(r.buf, '\r', '\n')
	r.buf = append(r.buf, a1...)
	r.buf = append(r.buf, '\r', '\n', '$')

	r.decimal(a2)

	r.buf = append(r.buf, '\r', '\n', '$')
	r.buf = strconv.AppendUint(r.buf, uint64(len(a3)), 10)
	r.buf = append(r.buf, '\r', '\n')
	r.buf = append(r.buf, a3...)
	r.buf = append(r.buf, '\r', '\n')
}

func (r *request) addStringIntInt(a1 string, a2, a3 int64) {
	r.buf = strconv.AppendUint(r.buf, uint64(len(a1)), 10)
	r.buf = append(r.buf, '\r', '\n')
	r.buf = append(r.buf, a1...)
	r.buf = append(r.buf, '\r', '\n', '$')

	r.decimal(a2)

	r.buf = append(r.buf, '\r', '\n', '$')

	r.decimal(a3)

	r.buf = append(r.buf, '\r', '\n')
}

func (r *request) addStringIntString(a1 string, a2 int64, a3 string) {
	r.buf = strconv.AppendUint(r.buf, uint64(len(a1)), 10)
	r.buf = append(r.buf, '\r', '\n')
	r.buf = append(r.buf, a1...)
	r.buf = append(r.buf, '\r', '\n', '$')

	r.decimal(a2)

	r.buf = append(r.buf, '\r', '\n', '$')
	r.buf = strconv.AppendUint(r.buf, uint64(len(a3)), 10)
	r.buf = append(r.buf, '\r', '\n')
	r.buf = append(r.buf, a3...)
	r.buf = append(r.buf, '\r', '\n')
}

func (r *request) addStringStringBytes(a1, a2 string, a3 []byte) {
	r.buf = strconv.AppendUint(r.buf, uint64(len(a1)), 10)
	r.buf = append(r.buf, '\r', '\n')
	r.buf = append(r.buf, a1...)
	r.buf = append(r.buf, '\r', '\n', '$')
	r.buf = strconv.AppendUint(r.buf, uint64(len(a2)), 10)
	r.buf = append(r.buf, '\r', '\n')
	r.buf = append(r.buf, a2...)
	r.buf = append(r.buf, '\r', '\n', '$')
	r.buf = strconv.AppendUint(r.buf, uint64(len(a3)), 10)
	r.buf = append(r.buf, '\r', '\n')
	r.buf = append(r.buf, a3...)
	r.buf = append(r.buf, '\r', '\n')
}

func (r *request) addStringStringString(a1, a2, a3 string) {
	r.buf = strconv.AppendUint(r.buf, uint64(len(a1)), 10)
	r.buf = append(r.buf, '\r', '\n')
	r.buf = append(r.buf, a1...)
	r.buf = append(r.buf, '\r', '\n', '$')
	r.buf = strconv.AppendUint(r.buf, uint64(len(a2)), 10)
	r.buf = append(r.buf, '\r', '\n')
	r.buf = append(r.buf, a2...)
	r.buf = append(r.buf, '\r', '\n', '$')
	r.buf = strconv.AppendUint(r.buf, uint64(len(a3)), 10)
	r.buf = append(r.buf, '\r', '\n')
	r.buf = append(r.buf, a3...)
	r.buf = append(r.buf, '\r', '\n')
}

func (r *request) addStringStringBytesMapLists(a1 string, a2 []string, a3 [][]byte) error {
	if len(a2) != len(a3) {
		return errMapSlices
	}
	r.buf = strconv.AppendUint(r.buf, uint64(len(a1)), 10)
	r.buf = append(r.buf, '\r', '\n')
	r.buf = append(r.buf, a1...)
	for i, key := range a2 {
		value := a3[i]
		r.buf = append(r.buf, '\r', '\n', '$')
		r.buf = strconv.AppendUint(r.buf, uint64(len(key)), 10)
		r.buf = append(r.buf, '\r', '\n')
		r.buf = append(r.buf, key...)
		r.buf = append(r.buf, '\r', '\n', '$')
		r.buf = strconv.AppendUint(r.buf, uint64(len(value)), 10)
		r.buf = append(r.buf, '\r', '\n')
		r.buf = append(r.buf, value...)
	}
	r.buf = append(r.buf, '\r', '\n')
	return nil
}

func (r *request) addStringStringStringMapLists(a1 string, a2, a3 []string) error {
	if len(a2) != len(a3) {
		return errMapSlices
	}
	r.buf = strconv.AppendUint(r.buf, uint64(len(a1)), 10)
	r.buf = append(r.buf, '\r', '\n')
	r.buf = append(r.buf, a1...)
	for i, key := range a2 {
		value := a3[i]
		r.buf = append(r.buf, '\r', '\n', '$')
		r.buf = strconv.AppendUint(r.buf, uint64(len(key)), 10)
		r.buf = append(r.buf, '\r', '\n')
		r.buf = append(r.buf, key...)
		r.buf = append(r.buf, '\r', '\n', '$')
		r.buf = strconv.AppendUint(r.buf, uint64(len(value)), 10)
		r.buf = append(r.buf, '\r', '\n')
		r.buf = append(r.buf, value...)
	}
	r.buf = append(r.buf, '\r', '\n')
	return nil
}

func (r *request) addDecimal(v int64) {
	r.decimal(v)
	r.buf = append(r.buf, '\r', '\n')
}

func (r *request) decimal(v int64) {
	sizeOffset := len(r.buf)
	sizeSingleDigit := v > -1e8 && v < 1e9
	if sizeSingleDigit {
		r.buf = append(r.buf, 0, '\r', '\n')
	} else {
		r.buf = append(r.buf, 0, 0, '\r', '\n')
	}

	valueOffset := len(r.buf)
	r.buf = strconv.AppendInt(r.buf, v, 10)
	size := len(r.buf) - valueOffset
	if sizeSingleDigit {
		r.buf[sizeOffset] = byte(size + '0')
	} else { // two digits
		r.buf[sizeOffset] = byte(size/10 + '0')
		r.buf[sizeOffset+1] = byte(size%10 + '0')
	}
}
