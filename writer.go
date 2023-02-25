package wire

import (
	"context"
	"errors"

	"github.com/jeroenrinzema/psql-wire/internal/buffer"
	"github.com/jeroenrinzema/psql-wire/internal/types"
)

// DataWriter represents a writer interface for writing columns and data rows
// using the Postgres wire to the connected client.
type DataWriter interface {
	// Define writes the column headers containing their type definitions, width
	// type oid, etc. to the underlaying Postgres client. The column headers
	// could only be written once. An error will be returned whenever this
	// method is called twice.
	Define(Columns) error
	// Row writes a single data row containing the values inside the given slice to
	// the underlaying Postgres client. The column headers have to be written before
	// sending rows. Each item inside the slice represents a single column value.
	// The slice length needs to be the same length as the defined columns. Nil
	// values are encoded as NULL values.
	Row([]any) error

	// Empty announces to the client a empty response and that no data rows should
	// be expected.
	Empty() error

	// Complete announces to the client that the command has been completed and
	// no further data should be expected.
	Complete(description string) error
}

// ErrUndefinedColumns is thrown when the columns inside the data writer have not
// yet been defined.
var ErrUndefinedColumns = errors.New("columns have not been defined")

// ErrDataWritten is thrown when an empty result is attempted to be send to the
// client while data has already been written.
var ErrDataWritten = errors.New("data has already been written")

// ErrClosedWriter is thrown when the data writer has been closed
var ErrClosedWriter = errors.New("closed writer")

// NewDataWriter constructs a new data writer using the given context and
// buffer. The returned writer should be handled with caution as it is not safe
// for concurrent use. Concurrent access to the same data without proper
// synchronization can result in unexpected behavior and data corruption.
func NewDataWriter(ctx context.Context, writer *buffer.Writer) DataWriter {
	return &dataWriter{
		ctx:    ctx,
		client: writer,
	}
}

// dataWriter is a implementation of the DataWriter interface.
type dataWriter struct {
	columns Columns
	ctx     context.Context
	client  *buffer.Writer
	closed  bool
	written uint64
}

func (writer *dataWriter) Define(columns Columns) error {
	if writer.closed {
		return ErrClosedWriter
	}

	writer.columns = columns
	return writer.columns.Define(writer.ctx, writer.client)
}

func (writer *dataWriter) Row(values []any) error {
	if writer.closed {
		return ErrClosedWriter
	}

	if writer.columns == nil {
		return ErrUndefinedColumns
	}

	writer.written++

	return writer.columns.Write(writer.ctx, writer.client, values)
}

func (writer *dataWriter) Empty() error {
	if writer.closed {
		return ErrClosedWriter
	}

	if writer.columns == nil {
		return ErrUndefinedColumns
	}

	if writer.written != 0 {
		return ErrDataWritten
	}

	defer writer.close()
	return emptyQuery(writer.client)
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

// emptyQuery indicates a empty query response by sending a emptyQuery message.
func emptyQuery(writer *buffer.Writer) error {
	writer.Start(types.ServerEmptyQuery)
	return writer.End()
}
