package redis

import (
	"bufio"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
)

// ErrMapSlices rejects execution due to a broken mapping.
var errMapSlices = errors.New("redis: number of keys doesn't match number of values")

func decodeOK(r *bufio.Reader) error {
	line, err := readLF(r)
	switch {
	case err != nil:
		return err
	case len(line) < 3:
		return fmt.Errorf("%w; received empty line %q", errProtocol, line)
	case line[0] == '+' && line[1] == 'O' && line[2] == 'K':
		return nil
	case line[0] == '$' && line[1] == '-' && line[2] == '1':
		return errNull
	case line[0] == '-':
		return ServerError(line[1 : len(line)-2])
	}
	return fmt.Errorf("%w; want OK simple string, received %.40q", errProtocol, line)
}

func decodeInteger(r *bufio.Reader) (int64, error) {
	line, err := readLF(r)
	switch {
	case err != nil:
		return 0, err
	case len(line) < 3:
		return 0, fmt.Errorf("%w; received empty line %q", errProtocol, line)
	case line[0] == ':':
		return ParseInt(line[1 : len(line)-2]), nil
	case line[0] == '-':
		return 0, ServerError(line[1 : len(line)-2])
	}
	return 0, fmt.Errorf("%w; want an integer, received %.40q", errProtocol, line)
}

func decodeBulkBytes(r *bufio.Reader) ([]byte, error) {
	size, err := readBulkSize(r)
	if size < 0 {
		return nil, err
	}
	bulk := make([]byte, size)

	// read payload
	if size > 0 {
		done, err := r.Read(bulk)
		for done < len(bulk) && err == nil {
			var more int
			more, err = r.Read(bulk[done:])
			done += more
		}
		if err != nil {
			return nil, err
		}
	}

	// skip CRLF
	_, err = r.Discard(2)
	return bulk, err
}

func decodeBulkString(r *bufio.Reader) (string, bool, error) {
	size, err := readBulkSize(r)
	if size < 0 {
		return "", false, err
	}

	n := int(size)
	bufSize := r.Size()
	if n <= bufSize {
		slice, err := r.Peek(n)
		if err != nil {
			return "", false, err
		}
		s := string(slice)
		_, err = r.Discard(n + 2)
		return s, true, err
	}

	var bulk strings.Builder
	bulk.Grow(n)
	for {
		slice, err := r.Peek(bufSize)
		if err != nil {
			return "", false, err
		}
		bulk.Write(slice)
		r.Discard(bufSize) // guaranteed to succeed

		n = bulk.Cap() - bulk.Len()
		if n <= bufSize {
			break
		}
	}

	slice, err := r.Peek(n)
	if err != nil {
		return "", false, err
	}
	bulk.Write(slice)
	_, err = r.Discard(n + 2) // skip CRLF
	return bulk.String(), true, err
}

func decodeBytesArray(r *bufio.Reader) ([][]byte, error) {
	size, err := readArraySize(r)
	if size < 0 {
		return nil, err
	}
	array := make([][]byte, 0, size)

	for len(array) < cap(array) {
		bytes, err := decodeBulkBytes(r)
		if err != nil {
			return nil, err
		}
		array = append(array, bytes)
	}
	return array, nil
}

func decodeStringArray(r *bufio.Reader) ([]string, error) {
	size, err := readArraySize(r)
	if size < 0 {
		return nil, err
	}
	array := make([]string, 0, size)

	for len(array) < cap(array) {
		s, _, err := decodeBulkString(r)
		if err != nil {
			return nil, err
		}
		array = append(array, s)
	}
	return array, nil
}

func readLF(r *bufio.Reader) (line []byte, err error) {
	line, err = r.ReadSlice('\n')
	if err != nil {
		if err == bufio.ErrBufferFull {
			err = fmt.Errorf("%w; LF exceeds %d bytes: %.40q…", errProtocol, r.Size(), line)
		}
		return nil, err
	}
	return line, nil
}

// Errors cause a negative size (for null).
func readBulkSize(r *bufio.Reader) (int64, error) {
	line, err := readLF(r)
	switch {
	case err != nil:
		return -1, err
	case len(line) < 3:
		return -1, fmt.Errorf("%w; received empty line %q", errProtocol, line)
	case line[0] == '$':
		size := ParseInt(line[1 : len(line)-2])
		if size > SizeMax {
			return -1, fmt.Errorf("redis: bluk receive with %d bytes", size)
		}
		return size, nil
	case line[0] == '-':
		return -1, ServerError(line[1 : len(line)-2])
	}
	return -1, fmt.Errorf("%w; want a bulk string, received %.40q", errProtocol, line)
}

// Errors cause a negative size (for null).
func readArraySize(r *bufio.Reader) (int64, error) {
	line, err := readLF(r)
	switch {
	case err != nil:
		return -1, err
	case len(line) < 3:
		return -1, fmt.Errorf("%w; received empty line %q", errProtocol, line)
	case line[0] == '*':
		size := ParseInt(line[1 : len(line)-2])
		if size > ElementMax {
			return -1, fmt.Errorf("redis: array receive with %d elements", size)
		}
		return size, nil
	case line[0] == '-':
		return -1, ServerError(line[1 : len(line)-2])
	}
	return -1, fmt.Errorf("%w; want an array, received %.40q", errProtocol, line)
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

func (r *request) addBytesList(a [][]byte) {
	for _, b := range a {
		r.buf = append(r.buf, '\r', '\n', '$')
		r.buf = strconv.AppendUint(r.buf, uint64(len(b)), 10)
		r.buf = append(r.buf, '\r', '\n')
		r.buf = append(r.buf, b...)
	}
	r.buf = append(r.buf, '\r', '\n')
}

func (r *request) addStringList(a []string) {
	for _, s := range a {
		r.buf = append(r.buf, '\r', '\n', '$')
		r.buf = strconv.AppendUint(r.buf, uint64(len(s)), 10)
		r.buf = append(r.buf, '\r', '\n')
		r.buf = append(r.buf, s...)
	}
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

func (r *request) addBytesBytesMapLists(a1, a2 [][]byte) error {
	if len(a1) != len(a2) {
		return errMapSlices
	}
	for i, key := range a1 {
		value := a2[i]
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

func (r *request) addStringBytesMapLists(a1 []string, a2 [][]byte) error {
	if len(a1) != len(a2) {
		return errMapSlices
	}
	for i, key := range a1 {
		value := a2[i]
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

func (r *request) addStringStringMapLists(a1, a2 []string) error {
	if len(a1) != len(a2) {
		return errMapSlices
	}
	for i, key := range a1 {
		value := a2[i]
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

func (r *request) addBytesBytesStringList(a1, a2 []byte, a3 []string) {
	r.buf = strconv.AppendUint(r.buf, uint64(len(a1)), 10)
	r.buf = append(r.buf, '\r', '\n')
	r.buf = append(r.buf, a1...)
	r.buf = append(r.buf, '\r', '\n', '$')
	r.buf = strconv.AppendUint(r.buf, uint64(len(a2)), 10)
	r.buf = append(r.buf, '\r', '\n')
	r.buf = append(r.buf, a2...)
	for _, s := range a3 {
		r.buf = append(r.buf, '\r', '\n', '$')
		r.buf = strconv.AppendUint(r.buf, uint64(len(s)), 10)
		r.buf = append(r.buf, '\r', '\n')
		r.buf = append(r.buf, s...)
	}
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

func (r *request) addStringBytesStringList(a1 string, a2 []byte, a3 []string) {
	r.buf = strconv.AppendUint(r.buf, uint64(len(a1)), 10)
	r.buf = append(r.buf, '\r', '\n')
	r.buf = append(r.buf, a1...)
	r.buf = append(r.buf, '\r', '\n', '$')
	r.buf = strconv.AppendUint(r.buf, uint64(len(a2)), 10)
	r.buf = append(r.buf, '\r', '\n')
	r.buf = append(r.buf, a2...)
	for _, s := range a3 {
		r.buf = append(r.buf, '\r', '\n', '$')
		r.buf = strconv.AppendUint(r.buf, uint64(len(s)), 10)
		r.buf = append(r.buf, '\r', '\n')
		r.buf = append(r.buf, s...)
	}
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

func (r *request) addStringStringStringList(a1, a2 string, a3 []string) {
	r.buf = strconv.AppendUint(r.buf, uint64(len(a1)), 10)
	r.buf = append(r.buf, '\r', '\n')
	r.buf = append(r.buf, a1...)
	r.buf = append(r.buf, '\r', '\n', '$')
	r.buf = strconv.AppendUint(r.buf, uint64(len(a2)), 10)
	r.buf = append(r.buf, '\r', '\n')
	r.buf = append(r.buf, a2...)
	for _, s := range a3 {
		r.buf = append(r.buf, '\r', '\n', '$')
		r.buf = strconv.AppendUint(r.buf, uint64(len(s)), 10)
		r.buf = append(r.buf, '\r', '\n')
		r.buf = append(r.buf, s...)
	}
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
