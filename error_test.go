package wire

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"math"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jeroenrinzema/psql-wire/codes"
	psqlerr "github.com/jeroenrinzema/psql-wire/errors"
	"github.com/jeroenrinzema/psql-wire/pkg/buffer"
	"github.com/jeroenrinzema/psql-wire/pkg/mock"
	"github.com/jeroenrinzema/psql-wire/pkg/types"
	"github.com/neilotoole/slogt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestErrorCode(t *testing.T) {
	handler := func(ctx context.Context, query Query) (PreparedStatements, error) {
		stmt := NewStatement(func(ctx context.Context, writer DataWriter, parameters []Parameter) error {
			return psqlerr.WithSeverity(psqlerr.WithCode(errors.New("unimplemented feature"), codes.FeatureNotSupported), psqlerr.LevelFatal)
		})

		return Prepared(stmt), nil
	}

	server, err := NewServer(handler, Logger(slogt.New(t)))
	assert.NoError(t, err)

	address := TListenAndServe(t, server)

	t.Run("lib/pq", func(t *testing.T) {
		connstr := fmt.Sprintf("host=%s port=%d sslmode=disable", address.IP, address.Port)
		conn, err := sql.Open("postgres", connstr)
		assert.NoError(t, err)

		_, err = conn.Query("SELECT *;")
		assert.Error(t, err)

		err = conn.Close()
		assert.NoError(t, err)
	})

	t.Run("jackc/pgx", func(t *testing.T) {
		ctx := context.Background()
		connstr := fmt.Sprintf("postgres://%s:%d", address.IP, address.Port)
		conn, err := pgx.Connect(ctx, connstr)
		assert.NoError(t, err)

		rows, _ := conn.Query(ctx, "SELECT *;")
		rows.Close()
		assert.Error(t, rows.Err())

		err = conn.Close(ctx)
		assert.NoError(t, err)
	})
}

// TestExtendedQueryParseErrorRecovery verifies that a non-fatal error during
// the extended query protocol doesn't desynchronize the connection. pgx uses
// the extended query protocol (Parse/Bind/Describe/Execute/Sync) and after an
// error it expects: ErrorResponse, then ReadyForQuery from Sync only. If the
// server sends an extra ReadyForQuery inside ErrorCode, pgx's protocol state
// gets out of sync and subsequent queries on the same connection break.
func TestExtendedQueryParseErrorRecovery(t *testing.T) {
	t.Parallel()

	handler := func(ctx context.Context, query Query) (PreparedStatements, error) {
		if query.Query == "SELECT error" {
			return nil, psqlerr.WithCode(errors.New("test error"), codes.Syntax)
		}

		stmt := NewStatement(func(ctx context.Context, writer DataWriter, parameters []Parameter) error {
			return writer.Complete("OK")
		})
		return Prepared(stmt), nil
	}

	server, err := NewServer(handler, Logger(slogt.New(t)))
	assert.NoError(t, err)

	address := TListenAndServe(t, server)

	ctx := context.Background()
	connstr := fmt.Sprintf("postgres://%s:%d?default_query_exec_mode=cache_statement", address.IP, address.Port)
	conn, err := pgx.Connect(ctx, connstr)
	assert.NoError(t, err)

	// First query: triggers a non-fatal error
	rows, _ := conn.Query(ctx, "SELECT error")
	rows.Close()
	assert.Error(t, rows.Err())

	// Second query on the same connection must succeed. In the extended query
	// protocol the server should only send ErrorResponse (no ReadyForQuery) and
	// discard messages until Sync, which sends the single ReadyForQuery. If the
	// server sends a spurious ReadyForQuery with the error, pgx's protocol
	// state gets out of sync and this second query will fail.
	rows, err = conn.Query(ctx, "SELECT 1;")
	assert.NoError(t, err)
	rows.Close()
	assert.NoError(t, rows.Err())

	err = conn.Close(ctx)
	assert.NoError(t, err)
}

// TestExtendedQueryExecuteErrorRecovery is similar to
// TestExtendedQueryParseErrorRecovery but the error occurs during Execute rather
// than Parse. Parse/Bind/Describe succeed, so pgx receives
// ParseComplete+BindComplete+RowDescription before the ErrorResponse from
// Execute. The extra ReadyForQuery that ErrorCode sends causes pgx to see it
// where it expects the response to its Close+Sync deallocate cycle.
func TestExtendedQueryExecuteErrorRecovery(t *testing.T) {
	t.Parallel()

	handler := func(ctx context.Context, query Query) (PreparedStatements, error) {
		columns := Columns{{Name: "result", Oid: 25}} // text

		if query.Query == "SELECT error" {
			stmt := NewStatement(func(ctx context.Context, writer DataWriter, parameters []Parameter) error {
				return psqlerr.WithCode(errors.New("execution failed"), codes.DataException)
			}, WithColumns(columns))
			return Prepared(stmt), nil
		}

		stmt := NewStatement(func(ctx context.Context, writer DataWriter, parameters []Parameter) error {
			return writer.Complete("OK")
		}, WithColumns(columns))
		return Prepared(stmt), nil
	}

	server, err := NewServer(handler, Logger(slogt.New(t)))
	assert.NoError(t, err)

	address := TListenAndServe(t, server)

	ctx := context.Background()
	connstr := fmt.Sprintf("postgres://%s:%d?default_query_exec_mode=cache_statement", address.IP, address.Port)
	conn, err := pgx.Connect(ctx, connstr)
	assert.NoError(t, err)

	// First query: parses successfully but fails during Execute
	rows, _ := conn.Query(ctx, "SELECT error")
	rows.Close()
	assert.Error(t, rows.Err())

	// Second query must succeed. If ErrorCode sent a spurious ReadyForQuery
	// during the Execute error, pgx's protocol state is desynchronized and
	// this query will fail.
	rows, err = conn.Query(ctx, "SELECT 1;")
	assert.NoError(t, err)
	rows.Close()
	assert.NoError(t, rows.Err())

	err = conn.Close(ctx)
	assert.NoError(t, err)
}

func TestSessionErrorCode(t *testing.T) {
	t.Run("simple query error includes ready for query", func(t *testing.T) {
		logger := slogt.New(t)
		sink := bytes.NewBuffer([]byte{})
		writer := buffer.NewWriter(logger, sink)

		session := &Session{
			Server:     &Server{logger: logger},
			Statements: &DefaultStatementCache{},
			Portals:    &DefaultPortalCache{},
		}

		err := session.WriteError(context.Background(), writer, psqlerr.WithCode(errors.New("some error"), codes.Syntax))
		assert.NoError(t, err)

		reader := buffer.NewReader(logger, sink, buffer.DefaultBufferSize)

		msgType, _, err := reader.ReadTypedMsg()
		assert.NoError(t, err)
		assert.Equal(t, types.ServerMessage(msgType), types.ServerErrorResponse)

		msgType, _, err = reader.ReadTypedMsg()
		assert.NoError(t, err)
		assert.Equal(t, types.ServerMessage(msgType), types.ServerReady)
	})

	t.Run("extended query error sets discard flag without ready for query", func(t *testing.T) {
		logger := slogt.New(t)
		sink := bytes.NewBuffer([]byte{})
		writer := buffer.NewWriter(logger, sink)

		session := &Session{
			Server:          &Server{logger: logger},
			Statements:      &DefaultStatementCache{},
			Portals:         &DefaultPortalCache{},
			inExtendedQuery: true,
		}

		err := session.WriteError(context.Background(), writer, psqlerr.WithCode(errors.New("some error"), codes.Syntax))
		assert.NoError(t, err)
		assert.True(t, session.discardUntilSync)

		reader := buffer.NewReader(logger, sink, buffer.DefaultBufferSize)

		msgType, _, err := reader.ReadTypedMsg()
		assert.NoError(t, err)
		assert.Equal(t, types.ServerMessage(msgType), types.ServerErrorResponse)

		// No ReadyForQuery in extended query mode
		_, _, err = reader.ReadTypedMsg()
		assert.Error(t, err)
	})

	t.Run("fatal error skips ready for query", func(t *testing.T) {
		logger := slogt.New(t)
		sink := bytes.NewBuffer([]byte{})
		writer := buffer.NewWriter(logger, sink)

		session := &Session{
			Server:     &Server{logger: logger},
			Statements: &DefaultStatementCache{},
			Portals:    &DefaultPortalCache{},
		}

		inputErr := psqlerr.WithSeverity(psqlerr.WithCode(errors.New("invalid username/password"), codes.InvalidPassword), psqlerr.LevelFatal)
		err := session.WriteError(context.Background(), writer, inputErr)
		assert.ErrorIs(t, err, inputErr)

		reader := buffer.NewReader(logger, sink, buffer.DefaultBufferSize)

		msgType, _, err := reader.ReadTypedMsg()
		assert.NoError(t, err)
		assert.Equal(t, types.ServerMessage(msgType), types.ServerErrorResponse)

		_, _, err = reader.ReadTypedMsg()
		assert.Error(t, err)
	})

	t.Run("fatal error in extended query returns error instead of waiting for sync", func(t *testing.T) {
		logger := slogt.New(t)
		sink := bytes.NewBuffer([]byte{})
		writer := buffer.NewWriter(logger, sink)

		session := &Session{
			Server:          &Server{logger: logger},
			Statements:      &DefaultStatementCache{},
			Portals:         &DefaultPortalCache{},
			inExtendedQuery: true,
		}

		inputErr := psqlerr.WithSeverity(psqlerr.WithCode(errors.New("fatal failure"), codes.FeatureNotSupported), psqlerr.LevelFatal)
		err := session.WriteError(context.Background(), writer, inputErr)
		assert.ErrorIs(t, err, inputErr)
		assert.False(t, session.discardUntilSync)

		reader := buffer.NewReader(logger, sink, buffer.DefaultBufferSize)

		msgType, _, err := reader.ReadTypedMsg()
		assert.NoError(t, err)
		assert.Equal(t, types.ServerMessage(msgType), types.ServerErrorResponse)

		_, _, err = reader.ReadTypedMsg()
		assert.Error(t, err)
	})

	t.Run("passes ctx to TxStatus when sending ReadyForQuery", func(t *testing.T) {
		logger := slogt.New(t)
		sink := bytes.NewBuffer([]byte{})
		writer := buffer.NewWriter(logger, sink)

		type ctxKey struct{}
		var seen context.Context
		session := &Session{
			Server: &Server{
				logger: logger,
				TxStatus: func(ctx context.Context) types.ServerStatus {
					seen = ctx
					return types.ServerIdle
				},
			},
			Statements: &DefaultStatementCache{},
			Portals:    &DefaultPortalCache{},
		}

		ctx := context.WithValue(context.Background(), ctxKey{}, "marker")
		err := session.WriteError(ctx, writer, psqlerr.WithCode(errors.New("boom"), codes.Syntax))
		require.NoError(t, err)
		require.NotNil(t, seen, "TxStatus callback was not invoked")
		assert.Equal(t, "marker", seen.Value(ctxKey{}), "TxStatus received a different ctx than the one passed to WriteError")
	})
}

func TestDiscardUntilSync(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	typeMap := pgtype.NewMap()
	ctx = setTypeInfo(ctx, typeMap)
	logger := slogt.New(t)

	mockParse := func(ctx context.Context, query Query) (PreparedStatements, error) {
		stmt := NewStatement(
			func(ctx context.Context, writer DataWriter, parameters []Parameter) error {
				return errors.New("query failed")
			},
			WithParameters([]uint32{}),
			WithColumns(Columns{
				{Name: "col", Oid: pgtype.TextOID},
			}),
		)
		return PreparedStatements{stmt}, nil
	}

	session := &Session{
		Server: &Server{
			logger: logger,
			parse:  mockParse,
		},
		Statements:      &DefaultStatementCache{},
		Portals:         &DefaultPortalCache{},
		inExtendedQuery: true,
	}

	outBuf := &bytes.Buffer{}
	writer := buffer.NewWriter(logger, outBuf)

	// First cycle: Parse, Bind, Execute (error), Sync
	err := session.handleParse(ctx, mock.NewParseReader(t, logger, "stmt1", "SELECT 1", 0), writer)
	require.NoError(t, err)
	err = session.handleBind(ctx, mock.NewBindReader(t, logger, "portal1", "stmt1", 0, 0, 0), writer)
	require.NoError(t, err)
	err = session.handleExecute(ctx, mock.NewExecuteReader(t, logger, "portal1", 0), writer)
	require.NoError(t, err)
	assert.True(t, session.discardUntilSync)
	err = session.handleSync(ctx, writer)
	require.NoError(t, err)
	assert.False(t, session.discardUntilSync)

	// Second cycle: Close, Sync (deallocate the failed statement)
	err = session.handleClose(ctx, mock.NewCloseReader(t, logger, types.CloseStatement, "stmt1"), writer)
	require.NoError(t, err)

	// The statement should be removed from the cache after Close
	stmt, err := session.Statements.Get(ctx, "stmt1")
	require.NoError(t, err)
	assert.Nil(t, stmt, "statement should be removed from cache after Close")

	err = session.handleSync(ctx, writer)
	require.NoError(t, err)

	responseReader := mock.NewReader(t, outBuf)

	// First cycle responses
	msgType, _, err := responseReader.ReadTypedMsg()
	require.NoError(t, err)
	assert.Equal(t, types.ServerParseComplete, msgType)

	msgType, _, err = responseReader.ReadTypedMsg()
	require.NoError(t, err)
	assert.Equal(t, types.ServerBindComplete, msgType)

	msgType, _, err = responseReader.ReadTypedMsg()
	require.NoError(t, err)
	assert.Equal(t, types.ServerErrorResponse, msgType)

	msgType, _, err = responseReader.ReadTypedMsg()
	require.NoError(t, err)
	assert.Equal(t, types.ServerReady, msgType)

	// Second cycle responses: CloseComplete, ReadyForQuery
	msgType, _, err = responseReader.ReadTypedMsg()
	require.NoError(t, err)
	assert.Equal(t, types.ServerCloseComplete, msgType)

	msgType, _, err = responseReader.ReadTypedMsg()
	require.NoError(t, err)
	assert.Equal(t, types.ServerReady, msgType)

	// No extra messages
	_, _, err = responseReader.ReadTypedMsg()
	require.Error(t, err)
}

// TestRowReturnsEncodeError verifies that when columns.Write fails on the pull
// side (e.g. a type the pgx TypeMap can't encode), the encoding error is
// returned from DataWriter.Row so the handler can see and wrap it.
func TestRowReturnsEncodeError(t *testing.T) {
	t.Parallel()

	var rowErr error

	handler := func(ctx context.Context, query Query) (PreparedStatements, error) {
		columns := Columns{{Name: "val", Oid: pgtype.Int4OID}}

		stmt := NewStatement(func(ctx context.Context, writer DataWriter, parameters []Parameter) error {
			rowErr = writer.Row([]any{struct{}{}})
			if rowErr != nil {
				return rowErr
			}
			return writer.Complete("SELECT 1")
		}, WithColumns(columns))

		return Prepared(stmt), nil
	}

	server, err := NewServer(handler, Logger(slogt.New(t)))
	require.NoError(t, err)

	address := TListenAndServe(t, server)

	ctx := context.Background()
	connstr := fmt.Sprintf("postgres://%s:%d", address.IP, address.Port)
	conn, err := pgx.Connect(ctx, connstr)
	require.NoError(t, err)

	rows, _ := conn.Query(ctx, "SELECT 1;")
	rows.Close()
	assert.Error(t, rows.Err())

	require.NotNil(t, rowErr)
	assert.Contains(t, rowErr.Error(), "unable to encode")

	err = conn.Close(ctx)
	assert.NoError(t, err)
}

func TestErrorResponseFields(t *testing.T) {
	t.Parallel()

	for _, line := range []int32{0, 123, math.MaxInt32} {
		t.Run(fmt.Sprintf("line_%d", line), func(t *testing.T) {
			t.Parallel()

			err := psqlerr.WithCode(errors.New("duplicate key"), codes.UniqueViolation)
			err = psqlerr.WithDetail(err, "Key already exists.")
			err = psqlerr.WithHint(err, "Choose a different key.")
			err = psqlerr.WithConstraintName(err, "items_pkey")
			err = psqlerr.WithSource(err, "constraints.go", line, "checkUnique")
			err = psqlerr.WithPosition(err, 42)

			var sink bytes.Buffer
			writer := buffer.NewWriter(slogt.New(t), &sink)
			require.NoError(t, WriteUnterminatedError(writer, err))

			// Decode the complete frame as a client would, including its length.
			client := pgproto3.NewFrontend(&sink, io.Discard)
			message, decodeErr := client.Receive()
			require.NoError(t, decodeErr)
			require.Equal(t, &pgproto3.ErrorResponse{
				Severity:            "ERROR",
				SeverityUnlocalized: "ERROR",
				Code:                "23505",
				Message:             "duplicate key",
				Detail:              "Key already exists.",
				Hint:                "Choose a different key.",
				Position:            42,
				ConstraintName:      "items_pkey",
				File:                "constraints.go",
				Line:                line,
				Routine:             "checkUnique",
			}, message)
		})
	}
}

func TestErrorResponseOptionalFields(t *testing.T) {
	t.Parallel()

	for _, sanitize := range []bool{false, true} {
		t.Run(fmt.Sprintf("sanitize_%t", sanitize), func(t *testing.T) {
			t.Parallel()

			publicErr := psqlerr.WithCode(errors.New("request failed"), codes.Internal)
			inputErr := publicErr
			var sink bytes.Buffer
			writer := buffer.NewWriter(slogt.New(t), &sink)
			if sanitize {
				inputErr = psqlerr.WithSource(inputErr, "private.go", 123, "privateRoutine")
				inputErr = psqlerr.WithConstraintName(inputErr, "private_constraint")
				inputErr = psqlerr.WithPosition(inputErr, 42)
				writer.ErrorSanitizer = func(error) error { return publicErr }
			}
			require.NoError(t, WriteUnterminatedError(writer, inputErr))
			// None of the public field values contain P, so its absence also
			// verifies that position was omitted rather than encoded as zero.
			require.NotContains(t, sink.Bytes()[5:], byte('P'))
			message, err := pgproto3.NewFrontend(&sink, io.Discard).Receive()
			require.NoError(t, err)
			require.Equal(t, &pgproto3.ErrorResponse{
				Severity:            "ERROR",
				SeverityUnlocalized: "ERROR",
				Code:                "XX000",
				Message:             "request failed",
			}, message)
		})
	}
}

func TestErrorResponsePosition(t *testing.T) {
	t.Parallel()

	for _, position := range []int32{-1, 0, 1, 123, math.MaxInt32} {
		t.Run(fmt.Sprintf("position_%d", position), func(t *testing.T) {
			t.Parallel()

			err := psqlerr.WithPosition(psqlerr.WithCode(errors.New("syntax error"), codes.Syntax), position)
			var sink bytes.Buffer
			writer := buffer.NewWriter(slogt.New(t), &sink)
			require.NoError(t, WriteUnterminatedError(writer, err))
			if position <= 0 {
				// The remaining field values contain no P bytes.
				require.NotContains(t, sink.Bytes()[5:], byte('P'))
			}
			message, decodeErr := pgproto3.NewFrontend(&sink, io.Discard).Receive()
			require.NoError(t, decodeErr)
			require.Equal(t, &pgproto3.ErrorResponse{
				Severity:            "ERROR",
				SeverityUnlocalized: "ERROR",
				Code:                "42601",
				Message:             "syntax error",
				Position:            max(position, 0),
			}, message)
		})
	}
}
