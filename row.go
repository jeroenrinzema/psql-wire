package wire

import (
	"context"
	"errors"
	"fmt"

	"github.com/jeroenrinzema/psql-wire/buffer"
	"github.com/jeroenrinzema/psql-wire/types"
	"github.com/lib/pq/oid"
)

// Columns represent a collection of columns
type Columns []Column

// Define writes the table RowDescription headers for the given table and the containing
// columns. The headers have to be written before any data rows could be send back
// to the client.
func (columns Columns) Define(ctx context.Context, writer *buffer.Writer) error {
	writer.Start(types.ServerRowDescription)
	writer.AddInt16(int16(len(columns)))

	for _, column := range columns {
		column.Define(ctx, writer)
	}

	return writer.End()
}

// Write writes the given column values back to the client using the predefined
// table column types and format encoders (text/binary).
func (columns Columns) Write(ctx context.Context, writer *buffer.Writer, srcs []interface{}) (err error) {
	if len(srcs) != len(columns) {
		return fmt.Errorf("unexpected columns, %d columns are defined inside the given table but %d were given", len(columns), len(srcs))
	}

	writer.Start(types.ServerDataRow)
	writer.AddInt16(int16(len(columns)))

	for index, column := range columns {
		err = column.Write(ctx, writer, srcs[index])
		if err != nil {
			return err
		}
	}

	return writer.End()
}

// Column represents a table column and its attributes such as name, type and
// encode formatter.
// https://www.postgresql.org/docs/8.3/catalog-pg-attribute.html
type Column struct {
	Table        int32  // table id
	Name         string // column name
	AttrNo       int16  // column attribute no (optional)
	Oid          oid.Oid
	Width        int16
	TypeModifier int32
	Format       FormatCode
}

// Define writes the column header values to the given writer.
// This method is used to define a column inside RowDescription message defining
// the column type, width, and name.
func (column Column) Define(ctx context.Context, writer *buffer.Writer) {
	writer.AddString(column.Name)
	writer.AddNullTerminate()
	writer.AddInt32(column.Table)
	writer.AddInt16(column.AttrNo)
	writer.AddInt32(int32(column.Oid))
	writer.AddInt16(column.Width)
	writer.AddInt32(-1) // TODO(Jeroen): type modifiers have not yet been fully implemented. Setting -1 to indicate a undefined value
	writer.AddInt16(int16(column.Format))
}

// Write encodes the given source value using the column type definition and connection
// info. The encoded byte buffer is added to the given write buffer. This method
// Is used to encode values and return them inside a DataRow message.
func (column Column) Write(ctx context.Context, writer *buffer.Writer, src interface{}) (err error) {
	if ctx.Err() != nil {
		return ctx.Err()
	}

	ci := TypeInfo(ctx)
	if ci == nil {
		return errors.New("postgres connection info has not been defined inside the given context")
	}

	typed, has := ci.DataTypeForOID(uint32(column.Oid))
	if !has {
		return fmt.Errorf("unknown data type: %T", column)
	}

	err = typed.Value.Set(src)
	if err != nil {
		return err
	}

	encoder := column.Format.Encoder(typed)
	bb, err := encoder(ci, nil)
	if err != nil {
		return err
	}

	writer.AddInt32(int32(len(bb)))
	writer.AddBytes(bb)

	return nil
}
