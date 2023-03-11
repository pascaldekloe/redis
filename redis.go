// Package redis provides access to Redis nodes.
// See <https://redis.io/topics/introduction> for the concept.
package redis

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"path/filepath"
	"strconv"
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

// ErrClosed signals end-of-life due a call to Close.
var ErrClosed = errors.New("redis: connection establishment closed")

// errProtocol signals invalid RESP reception.
var errProtocol = errors.New("redis: protocol violation")

// errNull represents a null reply. This case shoud be contained internally.
// The API represents null with nil and ok booleans conform Go convention.
var errNull = errors.New("redis: null")

// ServerError is a response from Redis.
type ServerError string

// Error honors the error interface.
func (e ServerError) Error() string {
	return fmt.Sprintf("redis: error message %q", string(e))
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

// ParseInt reads bytes as a decimal string without any validation.
// Empty bytes return zero. The value for any other invalid input is
// undefined, and may be subject to change in the future.
func ParseInt(bytes []byte) int64 {
	switch len(bytes) {
	case 0:
		return 0
	case 1: // happens often
		return int64(bytes[0]) - '0'
	}

	u := uint64(bytes[1] - '0')
	head := bytes[0]
	if head != '-' {
		u += 10 * uint64(head-'0')
	}
	for i := 2; i < len(bytes); i++ {
		u = 10*u + uint64(bytes[i]-'0')
	}

	v := int64(u)
	if head == '-' {
		v = -v
	}
	return v
}

func readOK(r *bufio.Reader) error {
	line, err := readLine(r)
	if err != nil {
		return err
	}
	if len(line) == 5 {
		u := binary.LittleEndian.Uint32(line)
		if u == '+'|'O'<<8|'K'<<16|'\r'<<24 {
			return nil
		}
		if u == '$'|'-'<<8|'1'<<16|'\r'<<24 {
			return errNull
		}
	}
	if len(line) > 3 && line[0] == '-' {
		return ServerError(line[1 : len(line)-2])
	}
	return fmt.Errorf("%w; received %.40q for OK", errProtocol, line)
}

func readInteger(r *bufio.Reader) (int64, error) {
	line, err := readLine(r)
	switch {
	case err != nil:
		return 0, err
	case len(line) > 3 && line[0] == ':':
		return ParseInt(line[1 : len(line)-2]), nil
	case len(line) > 3 && line[0] == '-':
		return 0, ServerError(line[1 : len(line)-2])
	default:
		return 0, fmt.Errorf("%w; received %.40q for integer", errProtocol, line)
	}
}

func readBulkBytes(r *bufio.Reader) ([]byte, error) {
	size, err := readBulkSize(r)
	if err != nil {
		return nil, err
	}
	bytes := make([]byte, size)
	_, err = io.ReadFull(r, bytes)
	if err == nil {
		_, err = r.Discard(2) // skip CRLF
	}
	return bytes, err
}

func readBulkString(r *bufio.Reader) (string, error) {
	size, err := readBulkSize(r)
	if err != nil {
		return "", err
	}

	slice, err := r.Peek(int(size))
	switch err {
	case nil:
		s := string(slice)
		_, err = r.Discard(len(s) + 2) // skip peek + CRLF
		return s, err

	case bufio.ErrBufferFull:
		buf := make([]byte, size)
		_, err = io.ReadFull(r, buf)
		if err != nil {
			return "", err
		}
		_, err = r.Discard(2) // skip CRLF
		return string(buf), err
	}
	return "", err
}

func readBytesArray(r *bufio.Reader) ([][]byte, error) {
	l, err := readArrayLen(r)
	if err != nil {
		return nil, err
	}
	array := make([][]byte, l)
	for i := range array {
		bytes, err := readBulkBytes(r)
		switch err {
		case nil:
			array[i] = bytes
		case errNull:
			array[i] = nil
		default:
			return nil, err
		}
	}
	return array, nil
}

func readStringArray(r *bufio.Reader) ([]string, error) {
	l, err := readArrayLen(r)
	if err != nil {
		return nil, err
	}
	array := make([]string, l)
	for i := range array {
		s, err := readBulkString(r)
		switch err {
		case nil:
			array[i] = s
		case errNull:
			array[i] = ""
		default:
			return nil, err
		}
	}
	return array, nil
}

func readBulkSize(r *bufio.Reader) (int64, error) {
	line, err := readLine(r)
	switch {
	case err != nil:
		return 0, err

	case len(line) > 3 && line[0] == '$':
		size := ParseInt(line[1 : len(line)-2])
		if size >= 0 && size <= SizeMax {
			return size, nil
		}
		if size == -1 {
			// "null bulk string"
			return 0, errNull
		}

	case len(line) > 3 && line[0] == '-':
		return 0, ServerError(line[1 : len(line)-2])
	}

	return 0, fmt.Errorf("%w; received %.40q for bulk string", errProtocol, line)
}

func readArrayLen(r *bufio.Reader) (int64, error) {
	line, err := readLine(r)
	switch {
	case err != nil:
		return 0, err

	case len(line) > 3 && line[0] == '*':
		l := ParseInt(line[1 : len(line)-2])
		if l >= 0 && l <= ElementMax {
			return l, nil
		}
		if l == -1 {
			// "null array"
			return 0, errNull
		}

	case len(line) > 3 && line[0] == '-':
		return 0, ServerError(line[1 : len(line)-2])
	}

	return 0, fmt.Errorf("%w; received %.40q for array", errProtocol, line)
}

func readLine(r *bufio.Reader) (line []byte, err error) {
	line, err = r.ReadSlice('\n')
	if err == bufio.ErrBufferFull {
		err = fmt.Errorf("%w; line %.40q… exceeds %d bytes", errProtocol, line, r.Size())
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
