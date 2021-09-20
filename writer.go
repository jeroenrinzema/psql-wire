package wire

import (
	"context"
	"errors"

	"github.com/jeroenrinzema/psql-wire/buffer"
)

// DataWriter represents a writer interface for writing columns and data rows
// using the Postgres wire to the connected client.
type DataWriter interface {
	// Define writes the column headers containing their type definitions, width
	// type oid, etc. to the underlaying Postgres client.
	Define(Columns) error
	// Row writes a single data row containing the values inside the given slice to
	// the underlaying Postgres client. The column headers have to be written before
	// sending rows. Each item inside the slice represents a single column value.
	// The slice length needs to be the same length as the defined columns. Nil
	// values are encoded as NULL values.
	Row([]interface{}) error

	// Empty announces to the client a empty response and that no data rows should
	// be expected.
	Empty() error

	// Complete announces to the client that the command has been completed and
	// no further data should be expected.
	Complete(description string) error
}

// ErrColumnsDefined is thrown when columns already have been defined inside the
// given data writer.
var ErrColumnsDefined = errors.New("columns have already been defined")

// ErrUndefinedColumns is thrown when the columns inside the data writer have not
// yet been defined.
var ErrUndefinedColumns = errors.New("columns have not been defined")

// ErrClosedWriter is thrown when the data writer has been closed
var ErrClosedWriter = errors.New("closed writer")

type dataWriter struct {
	columns Columns
	ctx     context.Context
	client  *buffer.Writer
	closed  bool
}

func (writer *dataWriter) Define(columns Columns) error {
	if writer.closed {
		return ErrClosedWriter
	}

	if writer.columns != nil {
		return ErrColumnsDefined
	}

	writer.columns = columns
	return writer.columns.Define(writer.ctx, writer.client)
}

func (writer *dataWriter) Row(values []interface{}) error {
	if writer.closed {
		return ErrClosedWriter
	}

	if writer.columns == nil {
		return ErrUndefinedColumns
	}

	return writer.columns.Write(writer.ctx, writer.client, values)
}

func (writer *dataWriter) Empty() error {
	if writer.closed {
		return ErrClosedWriter
	}

	if writer.columns == nil {
		return ErrUndefinedColumns
	}

	defer writer.close()
	return EmptyQuery(writer.client)
}

func (writer *dataWriter) Complete(description string) error {
	if writer.closed {
		return ErrClosedWriter
	}

	defer writer.close()
	return CommandComplete(writer.client, description)
}

func (writer *dataWriter) close() {
	writer.closed = true
}
