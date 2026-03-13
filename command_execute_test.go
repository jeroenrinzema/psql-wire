package wire

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jeroenrinzema/psql-wire/pkg/buffer"
	"github.com/jeroenrinzema/psql-wire/pkg/mock"
	"github.com/jeroenrinzema/psql-wire/pkg/types"
	"github.com/neilotoole/slogt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandleExecute_ParallelPipeline_Success(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	typeMap := pgtype.NewMap()
	ctx = setTypeInfo(ctx, typeMap)

	logger := slogt.New(t)

	mockParse := func(ctx context.Context, query string) (PreparedStatements, error) {
		stmt := NewStatement(
			func(ctx context.Context, writer DataWriter, parameters []Parameter) error {
				if err := writer.Row([]any{"Hello World"}); err != nil {
					return err
				}
				return writer.Complete("SELECT 1")
			},
			WithParameters([]uint32{}),
			WithColumns(Columns{
				{Name: "greeting", Oid: pgtype.TextOID},
			}),
		)
		return PreparedStatements{stmt}, nil
	}

	session := &Session{
		Server: &Server{
			logger: logger,
			parse:  mockParse,
		},
		Statements:       &DefaultStatementCache{},
		Portals:          &DefaultPortalCache{},
		ParallelPipeline: ParallelPipelineConfig{Enabled: true},
		ResponseQueue:    NewResponseQueue(),
	}

	outBuf := &bytes.Buffer{}
	writer := buffer.NewWriter(logger, outBuf)

	// Parse
	err := session.handleParse(ctx, mock.NewParseReader(t, logger, "stmt1", "SELECT 'Hello World'", 0), writer)
	require.NoError(t, err)

	// Bind
	err = session.handleBind(ctx, mock.NewBindReader(t, logger, "portal1", "stmt1", 0, 0, 0), writer)
	require.NoError(t, err)

	// Describe portal
	err = session.handleDescribe(ctx, mock.NewDescribeReader(t, logger, types.DescribePortal, "portal1"), writer)
	require.NoError(t, err)

	// Execute
	err = session.handleExecute(ctx, mock.NewExecuteReader(t, logger, "portal1", 0), writer)
	require.NoError(t, err)

	// Queue should have: ParseComplete, BindComplete, PortalDescribe, Execute
	assert.Equal(t, 4, session.ResponseQueue.Len())

	// Use Sync path to flush queue and write results
	err = session.handleSync(ctx, writer)
	require.NoError(t, err)
	assert.Equal(t, 0, session.ResponseQueue.Len())

	responseReader := mock.NewReader(t, outBuf)

	// 1. ParseComplete
	msgType, _, err := responseReader.ReadTypedMsg()
	require.NoError(t, err)
	assert.Equal(t, types.ServerParseComplete, msgType)

	// 2. BindComplete
	msgType, _, err = responseReader.ReadTypedMsg()
	require.NoError(t, err)
	assert.Equal(t, types.ServerBindComplete, msgType)

	// 3. RowDescription
	msgType, _, err = responseReader.ReadTypedMsg()
	require.NoError(t, err)
	assert.Equal(t, types.ServerRowDescription, msgType)

	// 4. DataRow
	msgType, _, err = responseReader.ReadTypedMsg()
	require.NoError(t, err)
	assert.Equal(t, types.ServerDataRow, msgType)

	// Verify row content "Hello World"
	colCount, err := responseReader.GetUint16()
	require.NoError(t, err)
	assert.Equal(t, uint16(1), colCount)

	colLen, err := responseReader.GetInt32()
	require.NoError(t, err)
	assert.Equal(t, int32(11), colLen) // "Hello World" length

	val, err := responseReader.GetBytes(11)
	require.NoError(t, err)
	assert.Equal(t, "Hello World", string(val))

	// 5. CommandComplete
	msgType, _, err = responseReader.ReadTypedMsg()
	require.NoError(t, err)
	assert.Equal(t, types.ServerCommandComplete, msgType)

	// 6. Ready after Sync
	msgType, _, err = responseReader.ReadTypedMsg()
	require.NoError(t, err)
	assert.Equal(t, types.ServerReady, msgType)

	// No extra messages
	_, _, err = responseReader.ReadTypedMsg()
	require.Error(t, err)
}

func TestHandleExecute_ParallelPipeline_StatementError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	typeMap := pgtype.NewMap()
	ctx = setTypeInfo(ctx, typeMap)

	logger := slogt.New(t)

	stmtErr := errors.New("statement failed")
	stmt := &Statement{
		fn:         func(ctx context.Context, writer DataWriter, params []Parameter) error { return stmtErr },
		parameters: []uint32{},
		columns: Columns{
			{Name: "greeting", Oid: pgtype.TextOID},
		},
	}

	portals := &DefaultPortalCache{}
	err := portals.Bind(ctx, "err_portal", stmt, nil, nil)
	require.NoError(t, err)

	session := &Session{
		Server:           &Server{logger: logger},
		Statements:       &DefaultStatementCache{},
		Portals:          portals,
		ParallelPipeline: ParallelPipelineConfig{Enabled: true},
		ResponseQueue:    NewResponseQueue(),
		inExtendedQuery:  true,
	}

	outBuf := &bytes.Buffer{}
	writer := buffer.NewWriter(logger, outBuf)

	reader := mock.NewExecuteReader(t, logger, "err_portal", 0)

	err = session.handleExecute(ctx, reader, writer)
	require.NoError(t, err)

	// Execute enqueued
	assert.Equal(t, 1, session.ResponseQueue.Len())

	// Sync should flush error
	err = session.handleSync(ctx, writer)
	require.NoError(t, err)
	assert.Equal(t, 0, session.ResponseQueue.Len())

	responseReader := mock.NewReader(t, outBuf)

	msgType, _, err := responseReader.ReadTypedMsg()
	require.NoError(t, err)
	assert.Equal(t, types.ServerErrorResponse, msgType)

	msgType, _, err = responseReader.ReadTypedMsg()
	require.NoError(t, err)
	assert.Equal(t, types.ServerReady, msgType)
}

func TestHandleExecute_ParallelPipeline_UnknownPortal(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	typeMap := pgtype.NewMap()
	ctx = setTypeInfo(ctx, typeMap)

	logger := slogt.New(t)

	session := &Session{
		Server:           &Server{logger: logger},
		Statements:       &DefaultStatementCache{},
		Portals:          &DefaultPortalCache{},
		ParallelPipeline: ParallelPipelineConfig{Enabled: true},
		ResponseQueue:    NewResponseQueue(),
		inExtendedQuery:  true,
	}

	// Pre-enqueue a successful event to ensure it's flushed before the error
	session.ResponseQueue.Enqueue(NewParseCompleteEvent())

	outBuf := &bytes.Buffer{}
	writer := buffer.NewWriter(logger, outBuf)

	reader := mock.NewExecuteReader(t, logger, "missing_portal", 0)

	err := session.handleExecute(ctx, reader, writer)
	require.NoError(t, err)

	assert.Equal(t, 0, session.ResponseQueue.Len())
	assert.True(t, session.discardUntilSync)

	// Sync sends ReadyForQuery and resets discardUntilSync
	err = session.handleSync(ctx, writer)
	require.NoError(t, err)
	assert.False(t, session.discardUntilSync)

	responseReader := mock.NewReader(t, outBuf)

	// 1. Flushed ParseComplete
	msgType, _, err := responseReader.ReadTypedMsg()
	require.NoError(t, err)
	assert.Equal(t, types.ServerParseComplete, msgType)

	// 2. Error for unknown portal
	msgType, _, err = responseReader.ReadTypedMsg()
	require.NoError(t, err)
	assert.Equal(t, types.ServerErrorResponse, msgType)

	// 3. ReadyForQuery from Sync
	msgType, _, err = responseReader.ReadTypedMsg()
	require.NoError(t, err)
	assert.Equal(t, types.ServerReady, msgType)

	// No extra messages
	_, _, err = responseReader.ReadTypedMsg()
	require.Error(t, err)
}

func TestHandleExecute_ParallelPipeline_AsyncPanic(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	typeMap := pgtype.NewMap()
	ctx = setTypeInfo(ctx, typeMap)

	logger := slogt.New(t)

	stmt := &Statement{
		fn: func(ctx context.Context, writer DataWriter, params []Parameter) error {
			panic("boom")
		},
		parameters: []uint32{},
		columns: Columns{
			{Name: "greeting", Oid: pgtype.TextOID},
		},
	}

	portals := &DefaultPortalCache{}
	err := portals.Bind(ctx, "panic_portal", stmt, nil, nil)
	require.NoError(t, err)

	session := &Session{
		Server:           &Server{logger: logger},
		Statements:       &DefaultStatementCache{},
		Portals:          portals,
		ParallelPipeline: ParallelPipelineConfig{Enabled: true},
		ResponseQueue:    NewResponseQueue(),
		inExtendedQuery:  true,
	}

	outBuf := &bytes.Buffer{}
	writer := buffer.NewWriter(logger, outBuf)

	reader := mock.NewExecuteReader(t, logger, "panic_portal", 0)

	err = session.handleExecute(ctx, reader, writer)
	require.NoError(t, err)

	// Panic should be captured as an error result and flushed via Sync
	err = session.handleSync(ctx, writer)
	require.NoError(t, err)
	assert.Equal(t, 0, session.ResponseQueue.Len())

	responseReader := mock.NewReader(t, outBuf)

	msgType, _, err := responseReader.ReadTypedMsg()
	require.NoError(t, err)
	assert.Equal(t, types.ServerErrorResponse, msgType)
}

func TestHandleExecute_ParallelPipeline_CloseWhilePending(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	typeMap := pgtype.NewMap()
	ctx = setTypeInfo(ctx, typeMap)

	logger := slogt.New(t)

	started := make(chan struct{})
	proceed := make(chan struct{})

	stmt := &Statement{
		fn: func(ctx context.Context, writer DataWriter, params []Parameter) error {
			close(started)
			<-proceed
			if err := writer.Row([]any{"hello"}); err != nil {
				return err
			}
			return writer.Complete("SELECT 1")
		},
		parameters: []uint32{},
		columns: Columns{
			{Name: "greeting", Oid: pgtype.TextOID},
		},
	}

	portals := &DefaultPortalCache{}
	err := portals.Bind(ctx, "portal1", stmt, nil, nil)
	require.NoError(t, err)

	session := &Session{
		Server:           &Server{logger: logger},
		Statements:       &DefaultStatementCache{},
		Portals:          portals,
		ParallelPipeline: ParallelPipelineConfig{Enabled: true},
		ResponseQueue:    NewResponseQueue(),
		inExtendedQuery:  true,
	}

	outBuf := &bytes.Buffer{}
	writer := buffer.NewWriter(logger, outBuf)

	// Launch async execute
	err = session.handleExecute(ctx, mock.NewExecuteReader(t, logger, "portal1", 0), writer)
	require.NoError(t, err)

	// Wait for the handler to start
	<-started

	// Close the portal while the goroutine is still running — should not block
	err = session.handleClose(ctx, mock.NewCloseReader(t, logger, 'P', "portal1"), writer)
	require.NoError(t, err)

	// Let the handler finish
	close(proceed)

	// Sync should flush the execute result and close complete
	err = session.handleSync(ctx, writer)
	require.NoError(t, err)

	responseReader := mock.NewReader(t, outBuf)

	// Execute result (DataRow + CommandComplete)
	msgType, _, err := responseReader.ReadTypedMsg()
	require.NoError(t, err)
	assert.Equal(t, types.ServerDataRow, msgType)

	msgType, _, err = responseReader.ReadTypedMsg()
	require.NoError(t, err)
	assert.Equal(t, types.ServerCommandComplete, msgType)

	// CloseComplete
	msgType, _, err = responseReader.ReadTypedMsg()
	require.NoError(t, err)
	assert.Equal(t, types.ServerCloseComplete, msgType)

	// ReadyForQuery
	msgType, _, err = responseReader.ReadTypedMsg()
	require.NoError(t, err)
	assert.Equal(t, types.ServerReady, msgType)
}

func TestHandleExecute_ParallelPipeline_SamePortalSerializes(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	typeMap := pgtype.NewMap()
	ctx = setTypeInfo(ctx, typeMap)

	logger := slogt.New(t)

	// Handler produces 3 rows. Two limited Execute(limit=1) calls on the same
	// portal must serialize: the second waits for the first to finish before
	// pulling the next row from the suspended iterator.
	stmt := &Statement{
		fn: func(ctx context.Context, writer DataWriter, params []Parameter) error {
			for i := 1; i <= 3; i++ {
				if err := writer.Row([]any{fmt.Sprintf("row %d", i)}); err != nil {
					return err
				}
			}
			return writer.Complete("SELECT 3")
		},
		parameters: []uint32{},
		columns: Columns{
			{Name: "result", Oid: pgtype.TextOID},
		},
	}

	portals := &DefaultPortalCache{}
	err := portals.Bind(ctx, "portal1", stmt, nil, nil)
	require.NoError(t, err)

	session := &Session{
		Server:           &Server{logger: logger},
		Statements:       &DefaultStatementCache{},
		Portals:          portals,
		ParallelPipeline: ParallelPipelineConfig{Enabled: true},
		ResponseQueue:    NewResponseQueue(),
		inExtendedQuery:  true,
	}

	outBuf := &bytes.Buffer{}
	writer := buffer.NewWriter(logger, outBuf)

	// Queue two Execute(limit=1) on the same portal — second must wait for first
	err = session.handleExecute(ctx, mock.NewExecuteReader(t, logger, "portal1", 1), writer)
	require.NoError(t, err)
	err = session.handleExecute(ctx, mock.NewExecuteReader(t, logger, "portal1", 1), writer)
	require.NoError(t, err)

	assert.Equal(t, 2, session.ResponseQueue.Len())

	err = session.handleSync(ctx, writer)
	require.NoError(t, err)

	// Both executes should produce results: each gets 1 row + PortalSuspended.
	responseReader := mock.NewReader(t, outBuf)

	// First execute: DataRow + PortalSuspended
	msgType, _, err := responseReader.ReadTypedMsg()
	require.NoError(t, err)
	assert.Equal(t, types.ServerDataRow, msgType)

	msgType, _, err = responseReader.ReadTypedMsg()
	require.NoError(t, err)
	assert.Equal(t, types.ServerPortalSuspended, msgType)

	// Second execute: DataRow + PortalSuspended
	msgType, _, err = responseReader.ReadTypedMsg()
	require.NoError(t, err)
	assert.Equal(t, types.ServerDataRow, msgType)

	msgType, _, err = responseReader.ReadTypedMsg()
	require.NoError(t, err)
	assert.Equal(t, types.ServerPortalSuspended, msgType)

	// ReadyForQuery
	msgType, _, err = responseReader.ReadTypedMsg()
	require.NoError(t, err)
	assert.Equal(t, types.ServerReady, msgType)
}

func TestHandleExecute_ParallelPipeline_DifferentPortalsParallel(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	typeMap := pgtype.NewMap()
	ctx = setTypeInfo(ctx, typeMap)

	logger := slogt.New(t)

	bothStarted := make(chan struct{})
	portal1Started := make(chan struct{})
	portal2Started := make(chan struct{})

	go func() {
		<-portal1Started
		<-portal2Started
		close(bothStarted)
	}()

	makeStmt := func(started chan struct{}) *Statement {
		return &Statement{
			fn: func(ctx context.Context, writer DataWriter, params []Parameter) error {
				close(started)
				// Wait until both portals have started — proves they run in parallel
				<-bothStarted
				if err := writer.Row([]any{"ok"}); err != nil {
					return err
				}
				return writer.Complete("SELECT 1")
			},
			parameters: []uint32{},
			columns: Columns{
				{Name: "result", Oid: pgtype.TextOID},
			},
		}
	}

	portals := &DefaultPortalCache{}
	err := portals.Bind(ctx, "p1", makeStmt(portal1Started), nil, nil)
	require.NoError(t, err)
	err = portals.Bind(ctx, "p2", makeStmt(portal2Started), nil, nil)
	require.NoError(t, err)

	session := &Session{
		Server:           &Server{logger: logger},
		Statements:       &DefaultStatementCache{},
		Portals:          portals,
		ParallelPipeline: ParallelPipelineConfig{Enabled: true},
		ResponseQueue:    NewResponseQueue(),
		inExtendedQuery:  true,
	}

	outBuf := &bytes.Buffer{}
	writer := buffer.NewWriter(logger, outBuf)

	err = session.handleExecute(ctx, mock.NewExecuteReader(t, logger, "p1", 0), writer)
	require.NoError(t, err)
	err = session.handleExecute(ctx, mock.NewExecuteReader(t, logger, "p2", 0), writer)
	require.NoError(t, err)

	assert.Equal(t, 2, session.ResponseQueue.Len())

	// Sync drains both — if they didn't run in parallel, they'd deadlock
	// waiting on bothStarted
	err = session.handleSync(ctx, writer)
	require.NoError(t, err)

	responseReader := mock.NewReader(t, outBuf)

	// Portal 1: DataRow + CommandComplete
	msgType, _, err := responseReader.ReadTypedMsg()
	require.NoError(t, err)
	assert.Equal(t, types.ServerDataRow, msgType)

	msgType, _, err = responseReader.ReadTypedMsg()
	require.NoError(t, err)
	assert.Equal(t, types.ServerCommandComplete, msgType)

	// Portal 2: DataRow + CommandComplete
	msgType, _, err = responseReader.ReadTypedMsg()
	require.NoError(t, err)
	assert.Equal(t, types.ServerDataRow, msgType)

	msgType, _, err = responseReader.ReadTypedMsg()
	require.NoError(t, err)
	assert.Equal(t, types.ServerCommandComplete, msgType)

	// ReadyForQuery
	msgType, _, err = responseReader.ReadTypedMsg()
	require.NoError(t, err)
	assert.Equal(t, types.ServerReady, msgType)
}
