package mock

import (
	"bytes"
	"io"
	"log/slog"
	"testing"

	"github.com/jeroenrinzema/psql-wire/pkg/buffer"
	"github.com/jeroenrinzema/psql-wire/pkg/types"
	"github.com/neilotoole/slogt"
)

// NewWriter constructs a new PostgreSQL wire protocol writer.
func NewWriter(t *testing.T, writer io.Writer) *Writer {
	return &Writer{buffer.NewWriter(slogt.New(t), writer)}
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
func NewReader(t *testing.T, reader io.Reader) *Reader {
	return &Reader{buffer.NewReader(slogt.New(t), reader, buffer.DefaultBufferSize)}
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

// NewParseReader creates a buffer.Reader containing a Parse message ready to be processed.
func NewParseReader(t *testing.T, logger *slog.Logger, name, query string, paramTypes int16) *buffer.Reader {
	t.Helper()

	inputBuf := &bytes.Buffer{}
	writer := NewWriter(t, inputBuf)
	writer.Start(types.ClientParse)
	writer.AddString(name)
	writer.AddNullTerminate()
	writer.AddString(query)
	writer.AddNullTerminate()
	writer.AddInt16(paramTypes)
	if err := writer.End(); err != nil {
		t.Fatalf("failed to write parse message: %v", err)
	}

	reader := buffer.NewReader(logger, inputBuf, buffer.DefaultBufferSize)
	if _, _, err := reader.ReadTypedMsg(); err != nil {
		t.Fatalf("failed to read parse message: %v", err)
	}

	return reader
}

// NewBindReader creates a buffer.Reader containing a Bind message ready to be processed.
func NewBindReader(t *testing.T, logger *slog.Logger, portal, statement string, paramFormats, paramValues, resultFormats int16) *buffer.Reader {
	t.Helper()

	inputBuf := &bytes.Buffer{}
	writer := NewWriter(t, inputBuf)
	writer.Start(types.ClientBind)
	writer.AddString(portal)
	writer.AddNullTerminate()
	writer.AddString(statement)
	writer.AddNullTerminate()
	writer.AddInt16(paramFormats)
	writer.AddInt16(paramValues)
	writer.AddInt16(resultFormats)
	if err := writer.End(); err != nil {
		t.Fatalf("failed to write bind message: %v", err)
	}

	reader := buffer.NewReader(logger, inputBuf, buffer.DefaultBufferSize)
	if _, _, err := reader.ReadTypedMsg(); err != nil {
		t.Fatalf("failed to read bind message: %v", err)
	}

	return reader
}

// NewDescribeReader creates a buffer.Reader containing a Describe message ready to be processed.
func NewDescribeReader(t *testing.T, logger *slog.Logger, describeType types.DescribeMessage, name string) *buffer.Reader {
	t.Helper()

	inputBuf := &bytes.Buffer{}
	writer := NewWriter(t, inputBuf)
	writer.Start(types.ClientDescribe)
	writer.AddByte(byte(describeType))
	writer.AddString(name)
	writer.AddNullTerminate()
	if err := writer.End(); err != nil {
		t.Fatalf("failed to write describe message: %v", err)
	}

	reader := buffer.NewReader(logger, inputBuf, buffer.DefaultBufferSize)
	if _, _, err := reader.ReadTypedMsg(); err != nil {
		t.Fatalf("failed to read describe message: %v", err)
	}

	return reader
}

// NewExecuteReader creates a buffer.Reader containing an Execute message ready to be processed.
func NewExecuteReader(t *testing.T, logger *slog.Logger, portal string, limit int32) *buffer.Reader {
	t.Helper()

	inputBuf := &bytes.Buffer{}
	writer := NewWriter(t, inputBuf)
	writer.Start(types.ClientExecute)
	writer.AddString(portal)
	writer.AddNullTerminate()
	writer.AddInt32(limit)
	if err := writer.End(); err != nil {
		t.Fatalf("failed to write execute message: %v", err)
	}

	reader := buffer.NewReader(logger, inputBuf, buffer.DefaultBufferSize)
	if _, _, err := reader.ReadTypedMsg(); err != nil {
		t.Fatalf("failed to read execute message: %v", err)
	}

	return reader
}
