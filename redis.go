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
	"time"
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

func initAUTH(password string, conn net.Conn, r *bufio.Reader, timeout time.Duration) error {
	req := newRequest("*2\r\n$4\r\nAUTH\r\n$")
	req.addString(password)

	if timeout != 0 {
		conn.SetDeadline(time.Now().Add(timeout))
		defer conn.SetDeadline(time.Time{})
	}

	_, err := conn.Write(req.buf)
	if err == nil {
		err = decodeOK(r)
	}
	if err != nil {
		return fmt.Errorf("redis: initial connection AUTH with %w", err)
	}
	return nil
}

func initSELECT(db int64, conn net.Conn, r *bufio.Reader, timeout time.Duration) error {
	if db == 0 {
		return nil // matches the default
	}

	req := newRequest("*2\r\n$6\r\nSELECT\r\n$")
	req.addDecimal(db)

	if timeout != 0 {
		conn.SetDeadline(time.Now().Add(timeout))
		defer conn.SetDeadline(time.Time{})
	}

	_, err := conn.Write(req.buf)
	if err == nil {
		err = decodeOK(r)
	}
	if err != nil {
		return fmt.Errorf("redis: initial connection SELECT with %w", err)
	}
	return nil
}

// errProtocol signals invalid RESP reception.
var errProtocol = errors.New("redis: protocol violation")

// errNull represents a null reply.
var errNull = errors.New("redis: null")

func decodeOK(r *bufio.Reader) error {
	line, err := readLF(r)
	switch {
	case err != nil:
		return err
	case len(line) == 5 && line[0] == '+' && line[1] == 'O' && line[2] == 'K':
		return nil
	case len(line) == 5 && line[0] == '$' && line[1] == '-' && line[2] == '1',
		len(line) == 3 && line[0] == '_':
		return errNull
	default:
		return readError(r, line, "OK")
	}
}

func decodeInteger(r *bufio.Reader) (int64, error) {
	line, err := readLF(r)
	switch {
	case err != nil:
		return 0, err
	case len(line) > 3 && line[0] == ':':
		return ParseInt(line[1 : len(line)-2]), nil
	default:
		return 0, readError(r, line, "integer")
	}
}

func decodeBlobBytes(r *bufio.Reader) ([]byte, error) {
	l, err := readBlobLen(r)
	if err != nil {
		return nil, err
	}
	return readBytesSize(r, l)
}

func decodeBlobString(r *bufio.Reader) (string, error) {
	l, err := readBlobLen(r)
	if err != nil {
		return "", err
	}
	return readStringSize(r, l)
}

func decodeBytesArray(r *bufio.Reader) ([][]byte, error) {
	l, err := readArrayLen(r)
	if err != nil {
		return nil, err
	}
	array := make([][]byte, 0, l)

	for len(array) < cap(array) {
		bytes, err := decodeBlobBytes(r)
		switch err {
		case nil:
			array = append(array, bytes)
		case errNull:
			array = append(array, nil)
		default:
			return nil, err
		}
	}
	return array, nil
}

func decodeStringArray(r *bufio.Reader) ([]string, error) {
	l, err := readArrayLen(r)
	if err != nil {
		return nil, err
	}
	array := make([]string, 0, l)

	for len(array) < cap(array) {
		s, err := decodeBlobString(r)
		switch err {
		case nil:
			array = append(array, s)
		case errNull:
			array = append(array, "")
		default:
			return nil, err
		}
	}
	return array, nil
}

func decodePushArray(r *bufio.Reader) (pushType, dest string, message []byte, err error) {
	l, err := readArrayLen(r)
	if err != nil {
		return "", "", nil, err
	}
	if l != 3 {
		return "", "", nil, fmt.Errorf("%w; received a push array with %d elements", errProtocol, l)
	}

	pushType, err = decodeBlobString(r)
	if err != nil {
		return "", "", nil, err
	}

	dest, err = decodeBlobString(r)
	if err != nil {
		return "", "", nil, err
	}
	if pushType == "message" {
		message, err = decodeBlobBytes(r)
	} else {
		_, err = decodeInteger(r)
	}
	return
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

func readBlobLen(r *bufio.Reader) (int64, error) {
	line, err := readLF(r)
	if err != nil {
		return 0, err
	}

	if len(line) > 3 && line[0] == '$' {
		l := ParseInt(line[1 : len(line)-2])
		switch {
		case l >= 0 && l <= SizeMax:
			return l, nil
		case l == -1:
			return 0, errNull
		}
	}
	return 0, readError(r, line, "blob")
}

func readArrayLen(r *bufio.Reader) (int64, error) {
	line, err := readLF(r)
	if err != nil {
		return 0, err
	}

	if len(line) > 3 && line[0] == '*' {
		l := ParseInt(line[1 : len(line)-2])
		switch {
		case l >= 0 && l <= ElementMax:
			return l, nil
		case l == -1:
			return 0, errNull
		}
	}
	return 0, readError(r, line, "array")
}

func readError(r *bufio.Reader, line []byte, want string) error {
	switch {
	case len(line) > 3 && line[0] == '-':
		return ServerError(line[1 : len(line)-2])
	case len(line) > 3 && line[0] == '!':
		l := ParseInt(line[1 : len(line)-2])
		if l < 0 || l > SizeMax {
			break
		}
		s, err := readStringSize(r, l)
		if err != nil {
			return fmt.Errorf("%w; blob error unavailable", err)
		}
		return ServerError(s)
	}

	return fmt.Errorf("%w; %s expected–received %.40q", errProtocol, want, line)
}

func readBytesSize(r *bufio.Reader, size int64) ([]byte, error) {
	blob := make([]byte, size)

	// read payload
	if size > 0 {
		done, err := r.Read(blob)
		for done < len(blob) && err == nil {
			var more int
			more, err = r.Read(blob[done:])
			done += more
		}
		if err != nil {
			return nil, err
		}
	}

	// skip CRLF
	_, err := r.Discard(2)
	return blob, err
}

func readStringSize(r *bufio.Reader, size int64) (string, error) {
	n := int(size)
	bufSize := r.Size()
	if n <= bufSize {
		slice, err := r.Peek(n)
		if err != nil {
			return "", err
		}
		s := string(slice)
		_, err = r.Discard(n + 2)
		return s, err
	}

	var blob strings.Builder
	blob.Grow(n)
	for {
		slice, err := r.Peek(bufSize)
		if err != nil {
			return "", err
		}
		blob.Write(slice)
		r.Discard(bufSize) // guaranteed to succeed

		n = blob.Cap() - blob.Len()
		if n <= bufSize {
			break
		}
	}

	slice, err := r.Peek(n)
	if err != nil {
		return "", err
	}
	blob.Write(slice)
	_, err = r.Discard(n + 2) // skip CRLF
	return blob.String(), err
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
