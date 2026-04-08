package wire

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5"
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
	handler := func(ctx context.Context, query string) (PreparedStatements, error) {
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

	handler := func(ctx context.Context, query string) (PreparedStatements, error) {
		if query == "SELECT error" {
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

	handler := func(ctx context.Context, query string) (PreparedStatements, error) {
		columns := Columns{{Name: "result", Oid: 25}} // text

		if query == "SELECT error" {
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

		err := session.WriteError(writer, psqlerr.WithCode(errors.New("some error"), codes.Syntax))
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

		err := session.WriteError(writer, psqlerr.WithCode(errors.New("some error"), codes.Syntax))
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
		err := session.WriteError(writer, inputErr)
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
		err := session.WriteError(writer, inputErr)
		assert.ErrorIs(t, err, inputErr)
		assert.False(t, session.discardUntilSync)

		reader := buffer.NewReader(logger, sink, buffer.DefaultBufferSize)

		msgType, _, err := reader.ReadTypedMsg()
		assert.NoError(t, err)
		assert.Equal(t, types.ServerMessage(msgType), types.ServerErrorResponse)

		_, _, err = reader.ReadTypedMsg()
		assert.Error(t, err)
	})
}

func TestDiscardUntilSync(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	typeMap := pgtype.NewMap()
	ctx = setTypeInfo(ctx, typeMap)
	logger := slogt.New(t)

	mockParse := func(ctx context.Context, query string) (PreparedStatements, error) {
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

	handler := func(ctx context.Context, query string) (PreparedStatements, error) {
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
