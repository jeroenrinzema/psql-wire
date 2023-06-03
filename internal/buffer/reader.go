package buffer

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"io"
	"unsafe"

	"github.com/jeroenrinzema/psql-wire/internal/types"
	"go.uber.org/zap"
)

// DefaultBufferSize represents the default buffer size whenever the buffer size
// is not set or a negative value is presented.
const DefaultBufferSize = 1 << 24 // 16777216 bytes

// BufferedReader extended io.Reader with some convenience methods.
type BufferedReader interface {
	io.Reader
	ReadString(delim byte) (string, error)
	ReadByte() (byte, error)
}

// Reader provides a convenient way to read pgwire protocol messages
type Reader struct {
	logger         *zap.Logger
	Buffer         BufferedReader
	Msg            []byte
	MaxMessageSize int
	header         [4]byte
}

// NewReader constructs a new Postgres wire buffer for the given io.Reader
func NewReader(logger *zap.Logger, reader io.Reader, bufferSize int) *Reader {
	if reader == nil {
		return nil
	}

	if bufferSize <= 0 {
		bufferSize = DefaultBufferSize
	}

	return &Reader{
		logger:         logger,
		Buffer:         bufio.NewReaderSize(reader, bufferSize),
		MaxMessageSize: bufferSize,
	}
}

// reset sets reader.Msg to exactly size, attempting to use spare capacity
// at the end of the existing slice when possible and allocating a new
// slice when necessary.
func (reader *Reader) reset(size int) {
	if reader.Msg != nil {
		reader.Msg = reader.Msg[len(reader.Msg):]
	}

	if cap(reader.Msg) >= size {
		reader.Msg = reader.Msg[:size]
		return
	}

	allocSize := size
	if allocSize < 4096 {
		allocSize = 4096
	}
	reader.Msg = make([]byte, size, allocSize)
}

// ReadTypedMsg reads a message from the provided reader, returning its type code and body.
// It returns the message type, number of bytes read, and an error if there was one.
func (reader *Reader) ReadTypedMsg() (types.ClientMessage, int, error) {
	b, err := reader.Buffer.ReadByte()
	if err != nil {
		return 0, 0, err
	}

	n, err := reader.ReadUntypedMsg()
	if err != nil {
		return 0, 0, err
	}

	reader.logger.Debug("reading typed message", zap.String("type", string(b)))
	return types.ClientMessage(b), n, nil
}

// Slurp reads the remaining
func (reader *Reader) Slurp(size int) error {
	remaining := size
	for remaining > 0 {
		reading := remaining

		if reading > reader.MaxMessageSize {
			reading = reader.MaxMessageSize
		}

		reader.reset(reading)

		n, err := io.ReadFull(reader.Buffer, reader.Msg)
		if err != nil {
			return err
		}

		remaining -= n
	}

	return nil
}

// ReadUntypedMsg reads a length-prefixed message. It is only used directly
// during the authentication phase of the protocol; ReadTypedMsg is used at all
// other times. This returns the number of bytes read and an error, if there
// was one. The number of bytes returned can be non-zero even with an error
// (e.g. if data was read but didn't validate) so that we can more accurately
// measure network traffic.
//
// If the error is related to consuming a buffer that is larger than the
// maxMessageSize, the remaining bytes will be read but discarded.
func (reader *Reader) ReadUntypedMsg() (int, error) {
	nread, err := io.ReadFull(reader.Buffer, reader.header[:])
	if err != nil {
		return nread, err
	}

	size := int(binary.BigEndian.Uint32(reader.header[:]))
	// size includes itself.
	size -= 4

	if size > reader.MaxMessageSize || size < 0 {
		return nread, NewMessageSizeExceeded(reader.MaxMessageSize, size)
	}

	reader.reset(size)
	n, err := io.ReadFull(reader.Buffer, reader.Msg)
	return nread + n, err
}

// GetString reads a null-terminated string.
func (reader *Reader) GetString() (string, error) {
	pos := bytes.IndexByte(reader.Msg, 0)
	if pos == -1 {
		return "", NewMissingNulTerminator()
	}

	// Note: this is a conversion from a byte slice to a string which avoids
	// allocation and copying. It is safe because we never reuse the bytes in our
	// read buffer. It is effectively the same as: "s := string(b.Msg[:pos])"
	s := reader.Msg[:pos]
	reader.Msg = reader.Msg[pos+1:]
	return *((*string)(unsafe.Pointer(&s))), nil
}

// GetPrepareType returns the buffer's contents as a PrepareType.
func (reader *Reader) GetPrepareType() (PrepareType, error) {
	v, err := reader.GetBytes(1)
	if err != nil {
		return 0, err
	}

	return PrepareType(v[0]), nil
}

// GetBytes returns the buffer's contents as a []byte.
func (reader *Reader) GetBytes(n int) ([]byte, error) {
	if len(reader.Msg) < n {
		return nil, NewInsufficientData(len(reader.Msg))
	}

	v := reader.Msg[:n]
	reader.Msg = reader.Msg[n:]
	return v, nil
}

// GetUint16 returns the buffer's contents as a uint16.
func (reader *Reader) GetUint16() (uint16, error) {
	if len(reader.Msg) < 2 {
		return 0, NewInsufficientData(len(reader.Msg))
	}

	v := binary.BigEndian.Uint16(reader.Msg[:2])
	reader.Msg = reader.Msg[2:]
	return v, nil
}

// GetUint32 returns the buffer's contents as a uint32.
func (reader *Reader) GetUint32() (uint32, error) {
	if len(reader.Msg) < 4 {
		return 0, NewInsufficientData(len(reader.Msg))
	}

	v := binary.BigEndian.Uint32(reader.Msg[:4])
	reader.Msg = reader.Msg[4:]
	return v, nil
}
