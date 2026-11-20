package wire

import (
	"context"
	"errors"
	"slices"

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

	// Define sets the result columns of a statement prepared with
	// [WithDeferredColumns], and answers a Describe that is waiting for these
	// columns. The handler of such a statement has to call it before writing
	// any rows or starting a COPY, also when the statement doesn't return any
	// rows. Not doing so is a bug in the handler, which closes the connection
	// with a FATAL error once the handler finishes.
	//
	// Once defined, the columns of a prepared statement can't change anymore,
	// just like in PostgreSQL, because some clients cache the response to a
	// Describe of the statement and use it to decode the rows of later
	// executions. So for a statement whose columns are already known, Define
	// returns an error if the names or types of the columns are different.
	// A client has to prepare the statement again to get different columns.
	Define(columns Columns) error

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

// RowWriter is an optional DataWriter extension. Rows must be observably
// identical to calling Row once per row, in order.
type RowWriter interface {
	Rows(rows [][]any) error
}

// WriteRows writes rows in order, using batching when supported by w.
func WriteRows(w DataWriter, rows [][]any) error {
	if bw, ok := w.(RowWriter); ok {
		return bw.Rows(rows)
	}
	for _, row := range rows {
		if err := w.Row(row); err != nil {
			return err
		}
	}
	return nil
}

// ErrDataWritten is returned when an empty result is attempted to be sent to the
// client while data has already been written.
var ErrDataWritten = errors.New("data has already been written")

// ErrClosedWriter is returned when the data writer has been closed.
var ErrClosedWriter = errors.New("closed writer")

// Bound per-writer scratch so a single large value is not retained.
const maxEncodeScratchCapacity = 64 << 10

// dataWriter implements DataWriter for use inside an iter.Seq push
// iterator. Row encodes the row to the wire and then yields to the pull
// consumer for flow control. Complete writes CommandComplete to the wire.
// This approach allows portal suspension: when the pull consumer stops
// pulling (row limit reached), the handler goroutine blocks in yield
// until the next Execute.
type dataWriter struct {
	ctx     context.Context
	session *Session
	portal  *Portal
	columns Columns
	formats []FormatCode
	client  *buffer.Writer
	reader  *buffer.Reader
	yield   func(struct{}) bool
	tag     *string
	closed  bool
	written uint32
	// Only the first unlimited Execute can enable batching. Limited portals
	// must continue yielding per row, including after subsequent Executes.
	batchable     bool
	encodeScratch []byte
}

func (writer *dataWriter) Columns() Columns {
	return writer.columns
}

func (writer *dataWriter) Define(columns Columns) error {
	if writer.closed {
		return ErrClosedWriter
	}

	// The columns of the statement are already known, either up front or
	// from an earlier execution, and the client might have received them
	// in a Describe. So the rows have to match those.
	statement := writer.portal.statement
	if !statement.deferredColumns {
		if !slices.EqualFunc(statement.columns, columns, sameRowType) {
			return errResultTypeChanged
		}
		return nil
	}

	// The statement keeps the columns for later executions, so it needs its
	// own copy in case the handler reuses the slice.
	columns = slices.Clone(columns)
	statement.columns = columns
	statement.deferredColumns = false
	writer.columns = columns
	if writer.portal.simpleQuery {
		return columns.Define(writer.ctx, writer.client, writer.formats)
	}
	return writer.session.resolveDescribes(writer.ctx, writer.client, writer.portal, columns)
}

// sameRowType reports whether two columns are the same in the result type of a
// statement. Like equalRowTypes in PostgreSQL, it only compares the name and
// the type, and not the table metadata. That metadata can change without
// breaking a client that cached the columns, because it isn't needed to
// decode the rows.
func sameRowType(a, b Column) bool {
	return a.Name == b.Name && a.Oid == b.Oid && a.TypeModifier == b.TypeModifier
}

// checkWritable returns an error if the writer is closed, or if the columns of
// a statement with deferred columns aren't defined yet. Writing anything
// before they are would end up before the RowDescription, or would wait for
// the client while the Describe is still held back. Completing the statement
// before is refused too, because a statement without rows has to define
// that, by calling Define with no columns, to answer a Describe with NoData.
func (writer *dataWriter) checkWritable() error {
	if writer.closed {
		return ErrClosedWriter
	}
	if writer.portal != nil && writer.portal.statement.deferredColumns {
		return errWriteBeforeDefine
	}
	return nil
}

func (writer *dataWriter) Row(values []any) error {
	if err := writer.checkWritable(); err != nil {
		return err
	}

	err := writer.columns.write(writer.ctx, writer.formats, writer.client, values, TypeMap(writer.ctx), &writer.encodeScratch)
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

// Rows writes materialized rows, batching frames only when portal flow control
// permits it. Limited portals keep Row's per-row suspension checkpoints.
func (writer *dataWriter) Rows(rows [][]any) (err error) {
	if err := writer.checkWritable(); err != nil {
		return err
	}
	if !writer.batchable {
		for _, row := range rows {
			if err := writer.Row(row); err != nil {
				return err
			}
		}
		return nil
	}

	writer.client.StartBatch(32 << 10)
	defer func() {
		pending := writer.client.Buffered() > 0
		flushErr := writer.client.EndBatch()
		if err == nil {
			err = flushErr
		}
		if pending && flushErr == nil && !writer.yield(struct{}{}) && err == nil {
			err = ErrSuspendedHandlerClosed
		}
	}()

	tm := TypeMap(writer.ctx)
	for _, row := range rows {
		if err := writer.columns.write(writer.ctx, writer.formats, writer.client, row, tm, &writer.encodeScratch); err != nil {
			return err
		}
		writer.written++
		if writer.client.Buffered() == 0 && !writer.yield(struct{}{}) {
			return ErrSuspendedHandlerClosed
		}
	}
	return nil
}

func (writer *dataWriter) CopyIn(format FormatCode) (*CopyReader, error) {
	if err := writer.checkWritable(); err != nil {
		return nil, err
	}

	err := writer.columns.CopyIn(writer.ctx, writer.client, format)
	if err != nil {
		return nil, err
	}
	return NewCopyReader(writer.session, writer.reader, writer.client, writer.columns), nil
}

func (writer *dataWriter) Empty() error {
	if err := writer.checkWritable(); err != nil {
		return err
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
	if err := writer.checkWritable(); err != nil {
		return err
	}

	defer writer.close()
	*writer.tag = description
	return commandComplete(writer.client, description)
}

func (writer *dataWriter) close() {
	writer.closed = true
	writer.encodeScratch = nil
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
