package wire

import (
	psqlerr "github.com/jeroenrinzema/psql-wire/errors"
	"github.com/jeroenrinzema/psql-wire/pkg/buffer"
	"github.com/jeroenrinzema/psql-wire/pkg/types"
)

// errFieldType represents the error fields.
type errFieldType byte

// http://www.postgresql.org/docs/current/static/protocol-error-fields.html
//
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

// WriteUnterminatedError writes an ErrorResponse message to the client without
// a trailing ReadyForQuery. Use this in contexts where no session is available
// (e.g. authentication) or where you need to control ReadyForQuery yourself.
func WriteUnterminatedError(writer *buffer.Writer, err error) error {
	if writer.ErrorSanitizer != nil {
		err = writer.ErrorSanitizer(err)
	}

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

	if desc.Hint != "" {
		writer.AddByte(byte(errFieldHint))
		writer.AddString(desc.Hint)
		writer.AddNullTerminate()
	}

	if desc.Detail != "" {
		writer.AddByte(byte(errFieldDetail))
		writer.AddString(desc.Detail)
		writer.AddNullTerminate()
	}

	if desc.Source != nil {
		writer.AddByte(byte(errFieldSrcFile))
		writer.AddString(desc.Source.File)
		writer.AddNullTerminate()

		writer.AddByte(byte(errFieldSrcLine))
		writer.AddInt32(desc.Source.Line)
		writer.AddNullTerminate()

		writer.AddByte(byte(errFieldSrcFunction))
		writer.AddString(desc.Source.Function)
		writer.AddNullTerminate()
	}

	writer.AddNullTerminate()
	return writer.End()
}

// WriteError on Session is protocol-aware: in extended query mode it writes
// ErrorResponse and sets `discardUntilSync` (ReadyForQuery comes from Sync).
// In simple query mode it writes ErrorResponse + ReadyForQuery.
func (srv *Session) WriteError(writer *buffer.Writer, err error) error {
	if werr := WriteUnterminatedError(writer, err); werr != nil {
		return werr
	}

	desc := psqlerr.Flatten(err)

	// FATAL and PANIC errors terminate the connection. The client expects the
	// server to close after sending the ErrorResponse, so we must not wait
	// for a Sync or send ReadyForQuery.
	if desc.Severity == psqlerr.LevelFatal || desc.Severity == psqlerr.LevelPanic {
		return err
	}

	if srv.inExtendedQuery {
		srv.discardUntilSync = true
		return nil
	}

	return readyForQuery(writer, types.ServerIdle)
}
