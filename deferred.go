package wire

import (
	"bytes"
	"context"
	"errors"
	"io"

	"github.com/jeroenrinzema/psql-wire/codes"
	psqlerr "github.com/jeroenrinzema/psql-wire/errors"
	"github.com/jeroenrinzema/psql-wire/pkg/buffer"
)

// errDeferredColumns answers a Describe of a statement with deferred columns
// that can't wait for its execution, unless the DeferredColumnsError option
// is used. It uses the same SQLSTATE as PostgreSQL
// uses when it can't determine the type of a parameter, which is the most
// common reason for the columns to be deferred.
var errDeferredColumns = psqlerr.WithCode(
	errors.New("the result columns of the statement are only known once it executes"),
	codes.IndeterminateDatatype,
)

// errDeferredColumnsPipeline is returned when parsing a statement with
// deferred columns while the parallel pipeline is enabled.
var errDeferredColumnsPipeline = psqlerr.WithCode(
	errors.New("statements whose result columns are only known once they execute are not supported with the parallel pipeline"),
	codes.FeatureNotSupported,
)

// errColumnsNotDefined is returned when a handler of a statement with deferred
// columns finishes without calling DataWriter.Define. That's a bug in the
// handler, and there's no good way to recover from it: the client might be
// waiting for a Describe, but the statement already ran, so failing just the
// Describe would make the client believe that the statement was skipped. So
// the connection is closed instead.
var errColumnsNotDefined = psqlerr.WithSeverity(psqlerr.WithCode(
	errors.New("the handler of a statement with deferred result columns finished without calling DataWriter.Define"),
	codes.Internal,
), psqlerr.LevelFatal)

// errResultTypeChanged is returned when the handler of a statement with
// deferred columns defines different columns than an earlier execution of the
// statement did. It mirrors the error that PostgreSQL returns when the result
// columns of a prepared statement change, e.g. due to a schema change,
// including its source location. Some clients detect this error to prepare
// the statement again, e.g. pgjdbc checks that the routine is
// RevalidateCachedQuery.
var errResultTypeChanged = psqlerr.WithSource(psqlerr.WithCode(
	errors.New("cached plan must not change result type"),
	codes.FeatureNotSupported,
), "plancache.c", 886, "RevalidateCachedQuery")

// errWriteBeforeDefine is returned when a handler of a statement with deferred
// columns writes rows or starts a COPY before calling DataWriter.Define.
var errWriteBeforeDefine = errors.New("DataWriter.Define has to be called before writing the result of a statement with deferred result columns")

// heldOutput is everything written to the client since a Describe of a
// statement with deferred columns, which waits for a portal of the statement
// to execute. The output is held back until then, so that it ends up after
// the RowDescription.
//
// A client can send several such Describes before the Execute, for example a
// Describe Statement before the Bind and a Describe Portal after it. So the
// held output is split up at each of those Describes, and the RowDescription
// of each is written before the output that came after it. Only an Execute
// that answers all of them may run, because the client expects any other one,
// and its side effects, to be skipped if one of the Describes fails.
type heldOutput struct {
	// original is the io.Writer of the session's buffer.Writer, which is
	// replaced by heldOutput itself while output is held, and restored once
	// the Describes are answered or failed.
	original  io.Writer
	describes []*heldDescribe
}

// deferredDescribe is a Describe of a statement with deferred columns, or of
// a portal bound to such a statement. Exactly one of statement and portal is
// set.
type deferredDescribe struct {
	// statement is set for a Describe Statement, which an Execute of any
	// portal of the statement can answer.
	statement *Statement
	// portal is set for a Describe Portal, which only an Execute of that
	// portal can answer.
	portal *Portal
}

func (d deferredDescribe) answeredBy(portal *Portal) bool {
	if d.portal != nil {
		return d.portal == portal
	}
	return d.statement == portal.statement
}

// write writes the response to the Describe, now that the columns are known.
// The ParameterDescription of a Describe Statement is held back together
// with the RowDescription, so that a Describe that fails is only answered
// with an ErrorResponse, just like in PostgreSQL.
func (d deferredDescribe) write(ctx context.Context, srv *Session, writer *buffer.Writer, columns Columns) error {
	if d.portal != nil {
		return srv.writeColumnDescription(ctx, writer, d.portal.formats, columns)
	}

	if err := srv.writeParameterDescription(writer, d.statement.parameters); err != nil {
		return err
	}
	// The formats to use for the result columns are only known once the
	// statement is bound, so a Describe Statement reports them as text.
	return srv.writeColumnDescription(ctx, writer, nil, columns)
}

// heldDescribe is one of the Describes in heldOutput, together with the
// output that came after it.
type heldDescribe struct {
	deferredDescribe
	// output is what was written to the client after this Describe and
	// before the next one.
	output bytes.Buffer
}

func (h *heldOutput) Write(p []byte) (int, error) {
	return h.describes[len(h.describes)-1].output.Write(p)
}

// releasedBy reports whether executing the given portal releases the held
// output, because it answers all held Describes.
func (h *heldOutput) releasedBy(portal *Portal) bool {
	for _, describe := range h.describes {
		if !describe.answeredBy(portal) {
			return false
		}
	}
	return true
}

func (srv *Session) holdDescribe(writer *buffer.Writer, describe deferredDescribe) {
	if srv.held == nil {
		srv.held = &heldOutput{original: writer.Writer}
		writer.Writer = srv.held
	}
	srv.held.describes = append(srv.held.describes, &heldDescribe{deferredDescribe: describe})
}

func (srv *Session) releaseHeld(writer *buffer.Writer) *heldOutput {
	held := srv.held
	srv.held = nil
	writer.Writer = held.original
	return held
}

// resolveDescribes answers the held Describes with the columns that the given
// portal defined while executing, and sends the output that was held back.
func (srv *Session) resolveDescribes(ctx context.Context, writer *buffer.Writer, portal *Portal, columns Columns) error {
	if srv == nil || srv.held == nil || !srv.held.releasedBy(portal) {
		return nil
	}

	held := srv.releaseHeld(writer)
	for _, describe := range held.describes {
		if err := describe.write(ctx, srv, writer, columns); err != nil {
			return err
		}
		if _, err := held.original.Write(describe.output.Bytes()); err != nil {
			return err
		}
	}
	return nil
}

// failDescribes answers the first held Describe with an error. The output that
// was held back is dropped, together with the later Describes, because they
// belong to messages that the client expects to be skipped after that error.
//
// This doesn't use WriteError, because whatever message triggered this, the
// error belongs to the extended query messages before it. So the client
// expects the messages until the next Sync to be skipped, even if this is
// e.g. a simple Query.
func (srv *Session) failDescribes(writer *buffer.Writer) error {
	srv.releaseHeld(writer)
	srv.discardUntilSync = true
	err := errDeferredColumns
	if srv.DeferredColumnsError != nil {
		err = srv.DeferredColumnsError
	}
	return WriteUnterminatedError(writer, err)
}

// executeHeld handles an Execute while Describes are held.
func (srv *Session) executeHeld(ctx context.Context, name string, limit Limit, reader *buffer.Reader, writer *buffer.Writer) error {
	// Any other Execute can't run before the Describes are answered, because
	// the client expects it to be skipped if one of them fails. An unknown
	// portal is left to Portals.Execute, whose error then answers the
	// Describes like any execution that fails before defining its columns.
	portal, err := srv.Portals.Get(ctx, name)
	if err == nil && portal != nil && !srv.held.releasedBy(portal) {
		return srv.failDescribes(writer)
	}

	err = srv.Portals.Execute(ctx, name, limit, reader, writer)
	if srv.held != nil {
		if err == nil {
			// Portal.execute makes sure that a handler that finishes
			// without defining its columns fails, and Row and CopyIn fail
			// before they are defined. So this can only happen when a
			// custom PortalCache doesn't execute the portal.
			err = errColumnsNotDefined
		}
		// The execution failed before it defined its columns (otherwise held
		// would be nil), so its error is the best answer to the Describes.
		srv.releaseHeld(writer)
	}
	if err != nil {
		return srv.WriteError(ctx, writer, err)
	}
	return nil
}
