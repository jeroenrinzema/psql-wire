package wire

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jeroenrinzema/psql-wire/pkg/buffer"
	"github.com/jeroenrinzema/psql-wire/pkg/mock"
	"github.com/jeroenrinzema/psql-wire/pkg/types"
	"github.com/lib/pq/oid"
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
			WithParameters([]oid.Oid{}),
			WithColumns(Columns{
				{Name: "greeting", Oid: oid.T_text},
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
		parameters: []oid.Oid{},
		columns: Columns{
			{Name: "greeting", Oid: oid.T_text},
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
	}

	// Pre-enqueue a successful event to ensure it's flushed before the error
	session.ResponseQueue.Enqueue(NewParseCompleteEvent())

	outBuf := &bytes.Buffer{}
	writer := buffer.NewWriter(logger, outBuf)

	reader := mock.NewExecuteReader(t, logger, "missing_portal", 0)

	err := session.handleExecute(ctx, reader, writer)
	require.NoError(t, err)

	assert.Equal(t, 0, session.ResponseQueue.Len())

	responseReader := mock.NewReader(t, outBuf)

	// 1. Flushed ParseComplete
	msgType, _, err := responseReader.ReadTypedMsg()
	require.NoError(t, err)
	assert.Equal(t, types.ServerParseComplete, msgType)

	// 2. Error for unknown portal
	msgType, _, err = responseReader.ReadTypedMsg()
	require.NoError(t, err)
	assert.Equal(t, types.ServerErrorResponse, msgType)

	// 3. Ready from ErrorCode
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
		parameters: []oid.Oid{},
		columns: Columns{
			{Name: "greeting", Oid: oid.T_text},
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
