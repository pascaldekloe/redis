// Package redis provides access to Redis nodes.
// See <https://redis.io/topics/introduction> for the concept.
package redis

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

// Server Limits
const (
	// SizeMax is the upper boundary for byte sizes.
	// A string value can be at most 512 MiB in length.
	SizeMax = 512 << 20

	// KeyMax is the upper boundary for key counts.
	// Redis can handle up to 2³² keys.
	KeyMax = 1 << 32

	// ElementMax is the upper boundary for element counts.
	// Every hash, list, set, and sorted set, can hold 2³² − 1 elements.
	ElementMax = 1<<32 - 1
)

// conservativeMMS uses the IPv6 minimum MTU of 1280 bytes, minus a 40 byte IP
// header, minus a 32 byte TCP header (with timestamps).
const conservativeMSS = 1208

func isUnixAddr(s string) bool {
	return len(s) != 0 && s[0] == '/'
}

func normalizeAddr(s string) string {
	if isUnixAddr(s) {
		return filepath.Clean(s)
	}

	host, port, err := net.SplitHostPort(s)
	if err != nil {
		host = s
	}
	if host == "" {
		host = "localhost"
	}
	if port == "" {
		port = "6379"
	}
	return net.JoinHostPort(host, port)
}

// ServerError is a command response from Redis.
type ServerError string

// Error honors the error interface.
func (e ServerError) Error() string {
	return fmt.Sprintf("redis: server error %q", string(e))
}

// Prefix returns the first word, which represents the error kind.
func (e ServerError) Prefix() string {
	s := string(e)
	for i, r := range s {
		if r == ' ' {
			return s[:i]
		}
	}
	return s
}

// ParseInt assumes a valid decimal string—no validation.
// The empty string returns zero.
func ParseInt(bytes []byte) int64 {
	if len(bytes) == 0 {
		return 0
	}
	u := uint64(bytes[0])

	neg := false
	if u == '-' {
		neg = true
		u = 0
	} else {
		u -= '0'
	}

	for i := 1; i < len(bytes); i++ {
		u = u*10 + uint64(bytes[i]-'0')
	}

	value := int64(u)
	if neg {
		value = -value
	}
	return value
}

// errProtocol signals invalid RESP reception.
var errProtocol = errors.New("redis: protocol violation")

// errNull represents the null bulk reply.
var errNull = errors.New("redis: null")

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

func decodePushArray(r *bufio.Reader) (pushType, dest string, message []byte, err error) {
	if size, err := readArraySize(r); size < 0 {
		return "", "", nil, err
	} else if size != 3 {
		return "", "", nil, fmt.Errorf("%w; received a push array with %d elements", errProtocol, size)
	}

	pushType, _, err = decodeBulkString(r)
	if err != nil {
		return "", "", nil, err
	}

	dest, _, err = decodeBulkString(r)
	if err != nil {
		return "", "", nil, err
	}
	if pushType == "message" {
		message, err = decodeBulkBytes(r)
	} else {
		_, err = decodeInteger(r)
	}
	return
}

// errMapSlices rejects execution due malformed invocation.
var errMapSlices = errors.New("redis: number of keys doesn't match number of values")

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
	r.bytes(a)
	r.buf = append(r.buf, '\r', '\n')
}

func (r *request) addString(a string) {
	r.string(a)
	r.buf = append(r.buf, '\r', '\n')
}

func (r *request) addBytesList(a [][]byte) {
	for _, b := range a {
		r.buf = append(r.buf, '\r', '\n', '$')
		r.bytes(b)
	}
	r.buf = append(r.buf, '\r', '\n')
}

func (r *request) addStringList(a []string) {
	for _, s := range a {
		r.buf = append(r.buf, '\r', '\n', '$')
		r.string(s)
	}
	r.buf = append(r.buf, '\r', '\n')
}

func (r *request) addBytesBytes(a1, a2 []byte) {
	r.bytes(a1)
	r.buf = append(r.buf, '\r', '\n', '$')
	r.bytes(a2)
	r.buf = append(r.buf, '\r', '\n')
}

func (r *request) addBytesBytesString(a1, a2 []byte, a3 string) {
	r.bytes(a1)
	r.buf = append(r.buf, '\r', '\n', '$')
	r.bytes(a2)
	r.buf = append(r.buf, '\r', '\n', '$')
	r.string(a3)
	r.buf = append(r.buf, '\r', '\n')
}

func (r *request) addBytesBytesStringInt(a1, a2 []byte, a3 string, a4 int64) {
	r.bytes(a1)
	r.buf = append(r.buf, '\r', '\n', '$')
	r.bytes(a2)
	r.buf = append(r.buf, '\r', '\n', '$')
	r.string(a3)
	r.buf = append(r.buf, '\r', '\n', '$')
	r.decimal(a4)
	r.buf = append(r.buf, '\r', '\n')
}

func (r *request) addBytesBytesStringStringInt(a1, a2 []byte, a3, a4 string, a5 int64) {
	r.bytes(a1)
	r.buf = append(r.buf, '\r', '\n', '$')
	r.bytes(a2)
	r.buf = append(r.buf, '\r', '\n', '$')
	r.string(a3)
	r.buf = append(r.buf, '\r', '\n', '$')
	r.string(a4)
	r.buf = append(r.buf, '\r', '\n', '$')
	r.decimal(a5)
	r.buf = append(r.buf, '\r', '\n')
}

func (r *request) addBytesBytesList(a1 []byte, a2 [][]byte) {
	r.bytes(a1)
	for _, b := range a2 {
		r.buf = append(r.buf, '\r', '\n', '$')
		r.bytes(b)
	}
	r.buf = append(r.buf, '\r', '\n')
}

func (r *request) addBytesBytesMapLists(a1, a2 [][]byte) error {
	if len(a1) != len(a2) {
		return errMapSlices
	}
	for i, key := range a1 {
		r.buf = append(r.buf, '\r', '\n', '$')
		r.bytes(key)
		r.buf = append(r.buf, '\r', '\n', '$')
		r.bytes(a2[i])
	}
	r.buf = append(r.buf, '\r', '\n')
	return nil
}

func (r *request) addBytesInt(a1 []byte, a2 int64) {
	r.bytes(a1)
	r.buf = append(r.buf, '\r', '\n', '$')
	r.decimal(a2)
	r.buf = append(r.buf, '\r', '\n')
}

func (r *request) addStringBytes(a1 string, a2 []byte) {
	r.string(a1)
	r.buf = append(r.buf, '\r', '\n', '$')
	r.bytes(a2)
	r.buf = append(r.buf, '\r', '\n')
}

func (r *request) addStringBytesString(a1 string, a2 []byte, a3 string) {
	r.string(a1)
	r.buf = append(r.buf, '\r', '\n', '$')
	r.bytes(a2)
	r.buf = append(r.buf, '\r', '\n', '$')
	r.string(a3)
	r.buf = append(r.buf, '\r', '\n')
}

func (r *request) addStringBytesStringInt(a1 string, a2 []byte, a3 string, a4 int64) {
	r.string(a1)
	r.buf = append(r.buf, '\r', '\n', '$')
	r.bytes(a2)
	r.buf = append(r.buf, '\r', '\n', '$')
	r.string(a3)
	r.buf = append(r.buf, '\r', '\n', '$')
	r.decimal(a4)
	r.buf = append(r.buf, '\r', '\n')
}

func (r *request) addStringBytesStringStringInt(a1 string, a2 []byte, a3, a4 string, a5 int64) {
	r.string(a1)
	r.buf = append(r.buf, '\r', '\n', '$')
	r.bytes(a2)
	r.buf = append(r.buf, '\r', '\n', '$')
	r.string(a3)
	r.buf = append(r.buf, '\r', '\n', '$')
	r.string(a4)
	r.buf = append(r.buf, '\r', '\n', '$')
	r.decimal(a5)
	r.buf = append(r.buf, '\r', '\n')
}

func (r *request) addStringBytesMapLists(a1 []string, a2 [][]byte) error {
	if len(a1) != len(a2) {
		return errMapSlices
	}
	for i, key := range a1 {
		r.buf = append(r.buf, '\r', '\n', '$')
		r.string(key)
		r.buf = append(r.buf, '\r', '\n', '$')
		r.bytes(a2[i])
	}
	r.buf = append(r.buf, '\r', '\n')
	return nil
}

func (r *request) addStringInt(a1 string, a2 int64) {
	r.string(a1)
	r.buf = append(r.buf, '\r', '\n', '$')
	r.decimal(a2)
	r.buf = append(r.buf, '\r', '\n')
}

func (r *request) addStringString(a1, a2 string) {
	r.string(a1)
	r.buf = append(r.buf, '\r', '\n', '$')
	r.string(a2)
	r.buf = append(r.buf, '\r', '\n')
}

func (r *request) addStringStringString(a1, a2, a3 string) {
	r.string(a1)
	r.buf = append(r.buf, '\r', '\n', '$')
	r.string(a2)
	r.buf = append(r.buf, '\r', '\n', '$')
	r.string(a3)
	r.buf = append(r.buf, '\r', '\n')
}

func (r *request) addStringStringStringInt(a1, a2, a3 string, a4 int64) {
	r.string(a1)
	r.buf = append(r.buf, '\r', '\n', '$')
	r.string(a2)
	r.buf = append(r.buf, '\r', '\n', '$')
	r.string(a3)
	r.buf = append(r.buf, '\r', '\n', '$')
	r.decimal(a4)
	r.buf = append(r.buf, '\r', '\n')
}

func (r *request) addStringStringStringStringInt(a1, a2, a3, a4 string, a5 int64) {
	r.string(a1)
	r.buf = append(r.buf, '\r', '\n', '$')
	r.string(a2)
	r.buf = append(r.buf, '\r', '\n', '$')
	r.string(a3)
	r.buf = append(r.buf, '\r', '\n', '$')
	r.string(a4)
	r.buf = append(r.buf, '\r', '\n', '$')
	r.decimal(a5)
	r.buf = append(r.buf, '\r', '\n')
}

func (r *request) addStringStringMapLists(a1, a2 []string) error {
	if len(a1) != len(a2) {
		return errMapSlices
	}
	for i, key := range a1 {
		r.buf = append(r.buf, '\r', '\n', '$')
		r.string(key)
		r.buf = append(r.buf, '\r', '\n', '$')
		r.string(a2[i])
	}
	r.buf = append(r.buf, '\r', '\n')
	return nil
}

func (r *request) addStringStringList(a1 string, a2 []string) {
	r.string(a1)
	for _, s := range a2 {
		r.buf = append(r.buf, '\r', '\n', '$')
		r.string(s)
	}
	r.buf = append(r.buf, '\r', '\n')
}

func (r *request) addBytesBytesBytes(a1, a2, a3 []byte) {
	r.bytes(a1)
	r.buf = append(r.buf, '\r', '\n', '$')
	r.bytes(a2)
	r.buf = append(r.buf, '\r', '\n', '$')
	r.bytes(a3)
	r.buf = append(r.buf, '\r', '\n')
}

func (r *request) addBytesBytesStringList(a1, a2 []byte, a3 []string) {
	r.bytes(a1)
	r.buf = append(r.buf, '\r', '\n', '$')
	r.bytes(a2)
	for _, s := range a3 {
		r.buf = append(r.buf, '\r', '\n', '$')
		r.string(s)
	}
	r.buf = append(r.buf, '\r', '\n')
}

func (r *request) addBytesBytesBytesMapLists(a1 []byte, a2, a3 [][]byte) error {
	if len(a2) != len(a3) {
		return errMapSlices
	}
	r.bytes(a1)
	for i, key := range a2 {
		r.buf = append(r.buf, '\r', '\n', '$')
		r.bytes(key)
		r.buf = append(r.buf, '\r', '\n', '$')
		r.bytes(a3[i])
	}
	r.buf = append(r.buf, '\r', '\n')
	return nil
}

func (r *request) addBytesIntBytes(a1 []byte, a2 int64, a3 []byte) {
	r.bytes(a1)
	r.buf = append(r.buf, '\r', '\n', '$')
	r.decimal(a2)
	r.buf = append(r.buf, '\r', '\n', '$')
	r.bytes(a3)
	r.buf = append(r.buf, '\r', '\n')
}

func (r *request) addBytesIntInt(a1 []byte, a2, a3 int64) {
	r.bytes(a1)
	r.buf = append(r.buf, '\r', '\n', '$')
	r.decimal(a2)
	r.buf = append(r.buf, '\r', '\n', '$')
	r.decimal(a3)
	r.buf = append(r.buf, '\r', '\n')
}

func (r *request) addStringBytesStringList(a1 string, a2 []byte, a3 []string) {
	r.string(a1)
	r.buf = append(r.buf, '\r', '\n', '$')
	r.bytes(a2)
	r.buf = append(r.buf, a2...)
	for _, s := range a3 {
		r.buf = append(r.buf, '\r', '\n', '$')
		r.string(s)
	}
	r.buf = append(r.buf, '\r', '\n')
}

func (r *request) addStringIntBytes(a1 string, a2 int64, a3 []byte) {
	r.string(a1)
	r.buf = append(r.buf, '\r', '\n', '$')
	r.decimal(a2)
	r.buf = append(r.buf, '\r', '\n', '$')
	r.bytes(a3)
	r.buf = append(r.buf, '\r', '\n')
}

func (r *request) addStringIntInt(a1 string, a2, a3 int64) {
	r.string(a1)
	r.buf = append(r.buf, '\r', '\n', '$')
	r.decimal(a2)
	r.buf = append(r.buf, '\r', '\n', '$')
	r.decimal(a3)
	r.buf = append(r.buf, '\r', '\n')
}

func (r *request) addStringIntString(a1 string, a2 int64, a3 string) {
	r.string(a1)
	r.buf = append(r.buf, '\r', '\n', '$')
	r.decimal(a2)
	r.buf = append(r.buf, '\r', '\n', '$')
	r.string(a3)
	r.buf = append(r.buf, '\r', '\n')
}

func (r *request) addStringStringBytes(a1, a2 string, a3 []byte) {
	r.string(a1)
	r.buf = append(r.buf, '\r', '\n', '$')
	r.string(a2)
	r.buf = append(r.buf, '\r', '\n', '$')
	r.bytes(a3)
	r.buf = append(r.buf, '\r', '\n')
}

func (r *request) addStringStringStringList(a1, a2 string, a3 []string) {
	r.string(a1)
	r.buf = append(r.buf, '\r', '\n', '$')
	r.string(a2)
	for _, s := range a3 {
		r.buf = append(r.buf, '\r', '\n', '$')
		r.string(s)
	}
	r.buf = append(r.buf, '\r', '\n')
}

func (r *request) addStringStringBytesMapLists(a1 string, a2 []string, a3 [][]byte) error {
	if len(a2) != len(a3) {
		return errMapSlices
	}
	r.string(a1)
	for i, key := range a2 {
		r.buf = append(r.buf, '\r', '\n', '$')
		r.string(key)
		r.buf = append(r.buf, '\r', '\n', '$')
		r.bytes(a3[i])
	}
	r.buf = append(r.buf, '\r', '\n')
	return nil
}

func (r *request) addStringStringStringMapLists(a1 string, a2, a3 []string) error {
	if len(a2) != len(a3) {
		return errMapSlices
	}
	r.string(a1)
	for i, key := range a2 {
		r.buf = append(r.buf, '\r', '\n', '$')
		r.string(key)
		r.buf = append(r.buf, '\r', '\n', '$')
		r.string(a3[i])
	}
	r.buf = append(r.buf, '\r', '\n')
	return nil
}

func (r *request) addDecimal(v int64) {
	r.decimal(v)
	r.buf = append(r.buf, '\r', '\n')
}

func (r *request) bytes(v []byte) {
	r.buf = strconv.AppendUint(r.buf, uint64(len(v)), 10)
	r.buf = append(r.buf, '\r', '\n')
	r.buf = append(r.buf, v...)
}

func (r *request) string(v string) {
	r.buf = strconv.AppendUint(r.buf, uint64(len(v)), 10)
	r.buf = append(r.buf, '\r', '\n')
	r.buf = append(r.buf, v...)
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
