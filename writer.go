package wire

import (
	"context"
	"errors"
	"io"

	"github.com/jeroenrinzema/psql-wire/pkg/buffer"
	"github.com/jeroenrinzema/psql-wire/pkg/types"
)

// DataWriter represents a writer interface for writing columns and data rows
// using the Postgres wire to the connected client.
type DataWriter interface {
	// Row writes a single data row containing the values inside the given slice to
	// the underlaying Postgres client. The column headers have to be written before
	// sending rows. Each item inside the slice represents a single column value.
	// The slice length needs to be the same length as the defined columns. Nil
	// values are encoded as NULL values.
	Row([]any) error

	// Written returns the number of rows written to the client.
	Written() uint64

	// Empty announces to the client an empty response and that no data rows should
	// be expected.
	Empty() error

	// Complete announces to the client that the command has been completed and
	// no further data should be expected.
	//
	// See [CommandComplete] for the expected format for different queries.
	//
	// [CommandComplete]: https://www.postgresql.org/docs/current/protocol-message-formats.html#PROTOCOL-MESSAGE-FORMATS-COMMANDCOMPLETE
	Complete(description string) error

	// CopyIn sends a [CopyInResponse] to the client, to initiate a CopyIn
	// operation. All format values must be either [TextFormat] or [BinaryFormat].
	// When overallFormat is [TextFormat], all columnFormats must be [TextFormat]. When
	// overallFormat is BinaryFormat, columnFormats may be either [TextFormat] or
	// [BinaryFormat]. You must provide one columnFormat value for each column
	// expected by the CopyIn operation.
	CopyIn(overallFormat FormatCode, columnFormats []FormatCode) (io.Reader, error)
}

// ErrDataWritten is returned when an empty result is attempted to be sent to the
// client while data has already been written.
var ErrDataWritten = errors.New("data has already been written")

// ErrClosedWriter is returned when the data writer has been closed.
var ErrClosedWriter = errors.New("closed writer")

// NewDataWriter constructs a new data writer using the given context and
// buffer. The returned writer should be handled with caution as it is not safe
// for concurrent use. Concurrent access to the same data without proper
// synchronization can result in unexpected behavior and data corruption.
func NewDataWriter(ctx context.Context, columns Columns, formats []FormatCode, writer *buffer.Writer, copyData io.Reader) DataWriter {
	return &dataWriter{
		ctx:      ctx,
		columns:  columns,
		formats:  formats,
		client:   writer,
		copyData: copyData,
	}
}

// dataWriter is a implementation of the DataWriter interface.
type dataWriter struct {
	ctx      context.Context
	columns  Columns
	formats  []FormatCode
	client   *buffer.Writer
	closed   bool
	written  uint64
	copyData io.Reader
}

func (writer *dataWriter) Define(columns Columns) error {
	if writer.closed {
		return ErrClosedWriter
	}

	writer.columns = columns
	return writer.columns.Define(writer.ctx, writer.client, writer.formats)
}

func (writer *dataWriter) Row(values []any) error {
	if writer.closed {
		return ErrClosedWriter
	}

	writer.written++

	return writer.columns.Write(writer.ctx, writer.formats, writer.client, values)
}

func (writer *dataWriter) CopyIn(overallFormat FormatCode, columnFormats []FormatCode) (io.Reader, error) {
	if writer.closed {
		return nil, ErrClosedWriter
	}
	if writer.copyData == nil {
		return nil, errors.New("DataCopyFn is nil; use PortalCacheCopy to execute CopyIn")
	}
	if len(columnFormats) == 0 {
		return nil, errors.New("CopyIn must have at least one column")
	}

	if err := writer.sendCopyInResponse(overallFormat, columnFormats); err != nil {
		return nil, err
	}

	return writer.copyData, nil
}

// sendCopyInResponse sends a [CopyInResponse] to the client, to initiate a
// CopyIn operation. format must be either [TextFormat] or [BinaryFormat], and
// columnCount must be >= 1.
//
// [CopyInResponse]: https://www.postgresql.org/docs/current/protocol-message-formats.html#PROTOCOL-MESSAGE-FORMATS-COPYINRESPONSE
func (writer *dataWriter) sendCopyInResponse(format FormatCode, columnFormats []FormatCode) error {
	writer.client.Start(types.ServerCopyInResponse)
	writer.client.AddByte(byte(format))
	writer.client.AddInt16(int16(len(columnFormats)))
	for _, columnFormat := range columnFormats {
		writer.client.AddInt16(int16(columnFormat))
	}
	return writer.client.End()
}

func (writer *dataWriter) Empty() error {
	if writer.closed {
		return ErrClosedWriter
	}

	if writer.written != 0 {
		return ErrDataWritten
	}

	defer writer.close()
	return nil
}

func (writer *dataWriter) Written() uint64 {
	return writer.written
}

func (writer *dataWriter) Complete(description string) error {
	if writer.closed {
		return ErrClosedWriter
	}

	if writer.written == 0 && writer.columns != nil {
		err := writer.Empty()
		if err != nil {
			return err
		}
	}

	defer writer.close()
	return commandComplete(writer.client, description)
}

func (writer *dataWriter) close() {
	writer.closed = true
}

// commandComplete announces that the requested command has successfully been executed.
// The given description is written back to the client and could be used to send
// additional meta data to the user.
func commandComplete(writer *buffer.Writer, description string) error {
	writer.Start(types.ServerCommandComplete)
	writer.AddString(description)
	writer.AddNullTerminate()
	return writer.End()
}
