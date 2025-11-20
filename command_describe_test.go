package wire

import (
	"bytes"
	"context"
	"testing"

	"github.com/jeroenrinzema/psql-wire/pkg/buffer"
	"github.com/jeroenrinzema/psql-wire/pkg/mock"
	"github.com/jeroenrinzema/psql-wire/pkg/types"
	"github.com/lib/pq/oid"
	"github.com/neilotoole/slogt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestHandleDescribeStatementSuccess verifies that successful describe statement enqueues the right event
func TestHandleDescribeStatementSuccess(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	logger := slogt.New(t)

	statements := &DefaultStatementCache{}
	stmt := NewStatement(
		func(ctx context.Context, writer DataWriter, parameters []Parameter) error { return nil },
		WithParameters([]oid.Oid{oid.T_int4}),
		WithColumns(Columns{{Name: "col1", Oid: oid.T_int4}}),
	)
	require.NoError(t, statements.Set(ctx, "test_stmt", stmt))

	session := &Session{
		Server:        &Server{logger: logger},
		Statements:    statements,
		ResponseQueue: NewResponseQueue(),
	}

	inputBuf := &bytes.Buffer{}
	mockWriter := mock.NewWriter(t, inputBuf)
	mockWriter.Start(types.ClientDescribe)
	mockWriter.AddByte(byte(types.DescribeStatement))
	mockWriter.AddString("test_stmt")
	mockWriter.AddNullTerminate()
	require.NoError(t, mockWriter.End())

	reader := buffer.NewReader(logger, inputBuf, buffer.DefaultBufferSize)
	_, _, err := reader.ReadTypedMsg()
	require.NoError(t, err)

	outBuf := &bytes.Buffer{}
	writer := buffer.NewWriter(logger, outBuf)

	err = session.handleDescribe(ctx, reader, writer)
	require.NoError(t, err)

	assert.Equal(t, 1, session.ResponseQueue.Len())
	events := session.ResponseQueue.DrainAll()
	require.Len(t, events, 1)

	event := events[0]
	assert.Equal(t, ResponseStmtDescribe, event.Kind)
	assert.Equal(t, []oid.Oid{oid.T_int4}, event.Parameters)
	assert.Len(t, event.Columns, 1)
	assert.Equal(t, "col1", event.Columns[0].Name)
}

// TestHandleDescribePortalSuccess verifies that successful describe portal enqueues the right event
func TestHandleDescribePortalSuccess(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	logger := slogt.New(t)

	portals := &DefaultPortalCache{}
	stmt := NewStatement(
		func(ctx context.Context, writer DataWriter, parameters []Parameter) error { return nil },
		WithParameters([]oid.Oid{oid.T_int4}),
		WithColumns(Columns{{Name: "col1", Oid: oid.T_int4}}),
	)

	formats := []FormatCode{BinaryFormat}
	err := portals.Bind(ctx, "test_portal", &Statement{
		parameters: []oid.Oid{},
		columns:    stmt.columns,
	}, []Parameter{}, formats)
	require.NoError(t, err)

	session := &Session{
		Server:        &Server{logger: logger},
		Portals:       portals,
		ResponseQueue: NewResponseQueue(),
	}

	inputBuf := &bytes.Buffer{}
	mockWriter := mock.NewWriter(t, inputBuf)
	mockWriter.Start(types.ClientDescribe)
	mockWriter.AddByte(byte(types.DescribePortal))
	mockWriter.AddString("test_portal")
	mockWriter.AddNullTerminate()
	require.NoError(t, mockWriter.End())

	reader := buffer.NewReader(logger, inputBuf, buffer.DefaultBufferSize)
	_, _, err = reader.ReadTypedMsg()
	require.NoError(t, err)

	outBuf := &bytes.Buffer{}
	writer := buffer.NewWriter(logger, outBuf)

	err = session.handleDescribe(ctx, reader, writer)
	require.NoError(t, err)

	assert.Equal(t, 1, session.ResponseQueue.Len())
	events := session.ResponseQueue.DrainAll()
	require.Len(t, events, 1)

	event := events[0]
	assert.Equal(t, ResponsePortalDescribe, event.Kind)
	assert.Len(t, event.Columns, 1)
	assert.Equal(t, "col1", event.Columns[0].Name)
	assert.Equal(t, formats, event.Formats)
}

// TestHandleDescribeError verifies error handling drains the queue
func TestHandleDescribeError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	logger := slogt.New(t)

	session := &Session{
		Server:        &Server{logger: logger},
		Statements:    &DefaultStatementCache{statements: map[string]*Statement{"unknown_stmt": nil}},
		ResponseQueue: NewResponseQueue(),
	}

	// Enqueue a previous event
	session.ResponseQueue.Enqueue(NewParseCompleteEvent())

	inputBuf := &bytes.Buffer{}
	mockWriter := mock.NewWriter(t, inputBuf)
	mockWriter.Start(types.ClientDescribe)
	mockWriter.AddByte(byte(types.DescribeStatement))
	mockWriter.AddString("unknown_stmt")
	mockWriter.AddNullTerminate()
	require.NoError(t, mockWriter.End())

	reader := buffer.NewReader(logger, inputBuf, buffer.DefaultBufferSize)
	_, _, err := reader.ReadTypedMsg()
	require.NoError(t, err)

	outBuf := &bytes.Buffer{}
	writer := buffer.NewWriter(logger, outBuf)

	err = session.handleDescribe(ctx, reader, writer)
	require.NoError(t, err)

	assert.Equal(t, 0, session.ResponseQueue.Len())

	responseReader := mock.NewReader(t, outBuf)

	// 1. ParseComplete
	msgType, _, err := responseReader.ReadTypedMsg()
	require.NoError(t, err)
	assert.Equal(t, types.ServerParseComplete, msgType)

	// 2. ErrorResponse
	msgType, _, err = responseReader.ReadTypedMsg()
	require.NoError(t, err)
	assert.Equal(t, types.ServerErrorResponse, msgType)
}
