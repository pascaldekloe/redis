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
	"unsafe"
)

// Server Limits
const (
	// SizeMax is the upper boundary for byte sizes.
	// A string value can be at most 512 MiB in length.
	SizeMax = 512 << 20

	// ElementMax is the upper boundary for element counts.
	// Every hash, list, set, and sorted set, can hold 2³² − 1 elements.
	ElementMax = 1<<32 - 1
)

// String is a key and/or value abstraction.
type String interface {
	~string | ~[]byte
}

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

func readBulk[T String](r *bufio.Reader) (bulk T, err error) {
	size, err := readBulkSize(r)
	if err != nil {
		return bulk, err
	}
	bytes := make([]byte, size)
	_, err = io.ReadFull(r, bytes)
	if err == nil {
		_, err = r.Discard(2) // skip CRLF
	}
	return *(*T)(unsafe.Pointer(&bytes)), err
}

func readArray[T String](r *bufio.Reader) ([]T, error) {
	l, err := readArrayLen(r)
	if l == 0 {
		return nil, err
	}
	array := make([]T, l)
	for i := range array {
		array[i], err = readBulk[T](r)
		switch err {
		case nil, errNull:
			break // OK
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

func requestFix(prefix string) *request {
	r := requestPool.Get().(*request)
	r.buf = append(r.buf[:0], prefix...)
	return r
}

func requestSize(prefix string, size int) *request {
	r := requestPool.Get().(*request)
	r.buf = append(r.buf[:0], '*')
	r.buf = strconv.AppendUint(r.buf, uint64(uint(size)), 10)
	r.buf = append(r.buf, prefix...)
	return r
}

func requestWithString[T String](prefix string, s T) *request {
	r := requestFix(prefix)
	r.buf = appendStringToDollar(r.buf, s)
	return r
}

func requestWith2Strings[T1, T2 String](prefix string, s1 T1, s2 T2) *request {
	r := requestFix(prefix)
	r.buf = appendStringAndDollarToDollar(r.buf, s1)
	r.buf = appendStringToDollar(r.buf, s2)
	return r
}

func requestWith3Strings[T1, T2, T3 String](prefix string, s1 T1, s2 T2, s3 T3) *request {
	r := requestFix(prefix)
	r.buf = appendStringAndDollarToDollar(r.buf, s1)
	r.buf = appendStringAndDollarToDollar(r.buf, s2)
	r.buf = appendStringToDollar(r.buf, s3)
	return r
}

func requestWithDecimal(prefix string, n int64) *request {
	r := requestFix(prefix)
	r.addDecimalToDollar(n)
	return r
}

func requestWithStringAndDecimal[T String](prefix string, s T, n int64) *request {
	r := requestFix(prefix)
	r.buf = appendStringAndDollarToDollar(r.buf, s)
	r.addDecimalToDollar(n)
	return r
}

func requestWithStringAndDecimalAndString[T1, T2 String](prefix string, s1 T1, n int64, s2 T2) *request {
	r := requestFix(prefix)
	r.buf = appendStringAndDollarToDollar(r.buf, s1)
	r.addSizeCRLFDecimal(n)
	r.buf = append(r.buf, '\r', '\n', '$')
	r.buf = appendStringToDollar(r.buf, s2)
	return r
}

func requestWithStringAnd2Decimals[T String](prefix string, s T, n1, n2 int64) *request {
	r := requestFix(prefix)
	r.buf = appendStringAndDollarToDollar(r.buf, s)
	r.addSizeCRLFDecimal(n1)
	r.buf = append(r.buf, '\r', '\n', '$')
	r.addDecimalToDollar(n2)
	return r
}

func requestWith3StringsAndDecimal[T1, T2, T3 String](prefix string, s1 T1, s2 T2, s3 T3, n int64) *request {
	r := requestFix(prefix)
	r.buf = appendStringAndDollarToDollar(r.buf, s1)
	r.buf = appendStringAndDollarToDollar(r.buf, s2)
	r.buf = appendStringAndDollarToDollar(r.buf, s3)
	r.addDecimalToDollar(n)
	return r
}

func requestWith4StringsAndDecimal[T1, T2, T3, T4 String](prefix string, s1 T1, s2 T2, s3 T3, s4 T4, n int64) *request {
	r := requestFix(prefix)
	r.buf = appendStringAndDollarToDollar(r.buf, s1)
	r.buf = appendStringAndDollarToDollar(r.buf, s2)
	r.buf = appendStringAndDollarToDollar(r.buf, s3)
	r.buf = appendStringAndDollarToDollar(r.buf, s4)
	r.addDecimalToDollar(n)
	return r
}

// Prefix must exclude both the size header and the command CRLF.
func requestWithList[T String](prefix string, list []T) *request {
	r := requestSize(prefix, len(list)+1)
	r.buf = appendCRLFAndList(r.buf, list)
	return r
}

// Prefix must exclude the size header and it must include the '$' prefix for s.
func requestWithStringAndList[T1, T2 String](prefix string, s T1, list []T2) *request {
	r := requestSize(prefix, len(list)+2)
	r.buf = appendSizeCRLFString(r.buf, s)
	r.buf = appendCRLFAndList(r.buf, list)
	return r
}

// AppendCRLFAndList follows dst up with a CRLF and each list T.
func appendCRLFAndList[T String](dst []byte, list []T) []byte {
	for _, s := range list {
		dst = append(dst, '\r', '\n', '$')
		dst = appendSizeCRLFString(dst, s)
	}
	return append(dst, '\r', '\n')
}

// ErrMapSlices rejects execution due malformed invocation.
var errMapSlices = errors.New("redis: number of keys doesn't match the number of values")

// Prefix must omit both the size header and the command CRLF.
func requestWithMap[Key, Value String](prefix string, keys []Key, values []Value) (*request, error) {
	r := requestSize(prefix, len(keys)*2+1)
	var err error
	r.buf, err = appendCRLFAndMap(r.buf, keys, values)
	if err != nil {
		return nil, err
	}
	return r, nil
}

// Prefix must omit both the size header and the command CRLF.
func requestWithStringAndMap[T1, Key, Value String](prefix string, s T1, keys []Key, values []Value) (*request, error) {
	r := requestSize(prefix, len(keys)*2+2)
	r.buf = appendSizeCRLFString(r.buf, s)
	var err error
	r.buf, err = appendCRLFAndMap(r.buf, keys, values)
	if err != nil {
		return nil, err
	}
	return r, nil
}

// AppendCRLFAndMap follows dst up with a CRLF and each Key–Value pair.
func appendCRLFAndMap[Key, Value String](dst []byte, keys []Key, values []Value) ([]byte, error) {
	if len(keys) != len(values) {
		return nil, errMapSlices
	}
	for i := range keys {
		dst = append(dst, '\r', '\n', '$')
		dst = appendSizeCRLFString(dst, keys[i])
		dst = append(dst, '\r', '\n', '$')
		dst = appendSizeCRLFString(dst, values[i])
	}
	return append(dst, '\r', '\n'), nil
}

// AppendStringToDollar follows a '$' in dst up with one payload.
func appendStringToDollar[T String](dst []byte, s T) []byte {
	dst = appendSizeCRLFString(dst, s)
	return append(dst, '\r', '\n')
}

// AppendStringAndDollarToDollar follows a '$' in dst up with one payload and a '$'.
func appendStringAndDollarToDollar[T String](dst []byte, s T) []byte {
	dst = appendSizeCRLFString(dst, s)
	return append(dst, '\r', '\n', '$')
}

// AppendSizeCRLFString follows a '$' in dst up with the length of v, a CRLF, and the
// bytes of v.
func appendSizeCRLFString[T String](dst []byte, s T) []byte {
	dst = strconv.AppendUint(dst, uint64(len(s)), 10)
	dst = append(dst, '\r', '\n')
	return append(dst, s...)
}

func (r *request) addDecimalToDollar(v int64) {
	r.addSizeCRLFDecimal(v)
	r.buf = append(r.buf, '\r', '\n')
}

func (r *request) addSizeCRLFDecimal(v int64) {
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
