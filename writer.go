package wire

import (
	"context"
	"errors"

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
	Written() uint32

	// Empty announces to the client an empty response and that no data rows should
	// be expected.
	Empty() error

	// Columns returns the columns that are currently defined within the writer.
	Columns() Columns

	// Complete announces to the client that the command has been completed and
	// no further data should be expected.
	//
	// See [CommandComplete] for the expected format for different queries.
	//
	// [CommandComplete]: https://www.postgresql.org/docs/current/protocol-message-formats.html#PROTOCOL-MESSAGE-FORMATS-COMMANDCOMPLETE
	Complete(description string) error

	// CopyIn sends a [CopyInResponse] to the client, to initiate a CopyIn
	// operation. The copy operation can be used to send large amounts of data to
	// the server in a single transaction. A column reader has to be used to read
	// the data that is sent by the client to the CopyReader.
	CopyIn(format FormatCode) (*CopyReader, error)
}

// ErrDataWritten is returned when an empty result is attempted to be sent to the
// client while data has already been written.
var ErrDataWritten = errors.New("data has already been written")

// ErrClosedWriter is returned when the data writer has been closed.
var ErrClosedWriter = errors.New("closed writer")

// dataWriter implements DataWriter for use inside an iter.Seq push
// iterator. Row encodes the row to the wire and then yields to the pull
// consumer for flow control. Complete writes CommandComplete to the wire.
// This approach allows portal suspension: when the pull consumer stops
// pulling (row limit reached), the handler goroutine blocks in yield
// until the next Execute.
type dataWriter struct {
	ctx     context.Context
	session *Session
	columns Columns
	formats []FormatCode
	client  *buffer.Writer
	reader  *buffer.Reader
	yield   func(struct{}) bool
	tag     *string
	closed  bool
	written uint32
}

func (writer *dataWriter) Columns() Columns {
	return writer.columns
}

func (writer *dataWriter) Row(values []any) error {
	if writer.closed {
		return ErrClosedWriter
	}

	err := writer.columns.Write(writer.ctx, writer.formats, writer.client, values)
	if err != nil {
		return err
	}

	writer.written++
	// The yield call "teleports" us back the next call of the pull consumer in
	// Portal.execute. The yield function returns true when the pull consumer
	// calls next again, and returns false when stop is called.
	if !writer.yield(struct{}{}) {
		return ErrSuspendedHandlerClosed
	}
	return nil
}

func (writer *dataWriter) CopyIn(format FormatCode) (*CopyReader, error) {
	if writer.closed {
		return nil, ErrClosedWriter
	}

	err := writer.columns.CopyIn(writer.ctx, writer.client, format)
	if err != nil {
		return nil, err
	}
	return NewCopyReader(writer.session, writer.reader, writer.client, writer.columns), nil
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

func (writer *dataWriter) Written() uint32 {
	return writer.written
}

func (writer *dataWriter) Complete(description string) error {
	if writer.closed {
		return ErrClosedWriter
	}

	defer writer.close()
	*writer.tag = description
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

// ErrSuspendedHandlerClosed is returned from DataWriter.Row when a suspended
// portal is closed (or re-bound) before the handler finished producing rows.
// Handlers can check for this error to distinguish graceful portal teardown
// from real failures and skip error logging.
var ErrSuspendedHandlerClosed = errors.New("suspended handler closed")
