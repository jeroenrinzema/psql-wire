package mock

import (
	"io"

	"github.com/jeroenrinzema/psql-wire/internal/buffer"
	"github.com/jeroenrinzema/psql-wire/internal/types"
	"go.uber.org/zap"
)

// NewWriter constructs a new PostgreSQL wire protocol writer.
func NewWriter(writer io.Writer) *Writer {
	return &Writer{buffer.NewWriter(zap.NewNop(), writer)}
}

// Writer represents a low level PostgreSQL client writer allowing a user to
// write messages using the PostgreSQL wire protocol. This implementation is
// mainly used for mocking/testing purposes.
type Writer struct {
	*buffer.Writer
}

// Start resets the buffer writer and starts a new message with the given
// message type. The message type (byte) and reserved message length bytes (int32)
// are written to the underlaying bytes buffer.
func (buffer *Writer) Start(t types.ClientMessage) {
	buffer.Writer.Start(types.ServerMessage(t))
}

// NewReader constructs a new PostgreSQL wire protocol reader using the default
// buffer size.
func NewReader(reader io.Reader) *Reader {
	return &Reader{buffer.NewReader(zap.NewNop(), reader, buffer.DefaultBufferSize)}
}

// Reader represents a low level PostgreSQL client reader allowing a user to
// read messages through the PostgreSQL wire protocol. This implementation is
// mainly used for mocking/testing purposes.
type Reader struct {
	*buffer.Reader
}

// ReadTypedMsg reads a message from the provided reader, returning its type code and body.
// It returns the message type, number of bytes read, and an error if there was one.
func (buffer *Reader) ReadTypedMsg() (types.ServerMessage, int, error) {
	t, l, err := buffer.Reader.ReadTypedMsg()
	return types.ServerMessage(t), l, err
}
