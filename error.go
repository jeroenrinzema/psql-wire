package wire

import (
	psqlerr "github.com/jeroenrinzema/psql-wire/errors"
	"github.com/jeroenrinzema/psql-wire/internal/buffer"
	"github.com/jeroenrinzema/psql-wire/internal/types"
)

// errFieldType represents the error fields.
type errFieldType byte

// http://www.postgresql.org/docs/current/static/protocol-error-fields.html
//nolint:varcheck,deadcode
const (
	errFieldSeverity       errFieldType = 'S'
	errFieldMsgPrimary     errFieldType = 'M'
	errFieldSQLState       errFieldType = 'C'
	errFieldDetail         errFieldType = 'D'
	errFieldHint           errFieldType = 'H'
	errFieldSrcFile        errFieldType = 'F'
	errFieldSrcLine        errFieldType = 'L'
	errFieldSrcFunction    errFieldType = 'R'
	errFieldConstraintName errFieldType = 'n'
)

// ErrorCode writes a error message as response to a command with the given severity and error message
// https://www.postgresql.org/docs/current/static/protocol-error-fields.html
func ErrorCode(writer *buffer.Writer, err error) error {
	desc := psqlerr.Flatten(err)

	writer.Start(types.ServerErrorResponse)

	writer.AddByte(byte(errFieldSeverity))
	writer.AddString(string(desc.Severity))
	writer.AddNullTerminate()
	writer.AddByte(byte(errFieldSQLState))
	writer.AddString(string(desc.Code))
	writer.AddNullTerminate()
	writer.AddByte(byte(errFieldMsgPrimary))
	writer.AddString(desc.Message)
	writer.AddNullTerminate()

	writer.AddNullTerminate()
	return writer.End()
}
