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

// TestHandleBindSuccess verifies that successful bind operations enqueue the right events
func TestHandleBindSuccess(t *testing.T) {
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
		Server: &Server{
			logger: logger,
		},
		Statements:    statements,
		Portals:       &DefaultPortalCache{},
		ResponseQueue: NewResponseQueue(),
	}

	inputBuf := &bytes.Buffer{}
	mockWriter := mock.NewWriter(t, inputBuf)
	mockWriter.Start(types.ClientBind)
	mockWriter.AddString("test_portal")
	mockWriter.AddNullTerminate()
	mockWriter.AddString("test_stmt")
	mockWriter.AddNullTerminate()
	mockWriter.AddInt16(0) // Param formats
	mockWriter.AddInt16(0) // Param values
	mockWriter.AddInt16(0) // Result formats
	require.NoError(t, mockWriter.End())

	reader := buffer.NewReader(logger, inputBuf, buffer.DefaultBufferSize)
	msgType, _, err := reader.ReadTypedMsg()
	require.NoError(t, err)
	require.Equal(t, types.ClientMessage(types.ClientBind), msgType)

	err = session.handleBind(ctx, reader, buffer.NewWriter(logger, &bytes.Buffer{}))
	require.NoError(t, err)

	assert.Equal(t, 1, session.ResponseQueue.Len())
	events := session.ResponseQueue.DrainAll()
	require.Len(t, events, 1)

	event := events[0]
	assert.Equal(t, ResponseBindComplete, event.Kind)
}

// TestHandleBindError verifies error handling drains the queue
func TestHandleBindError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	logger := slogt.New(t)

	session := &Session{
		Server: &Server{
			logger: logger,
		},
		Statements: &DefaultStatementCache{
			statements: map[string]*Statement{
				"unknown_stmt": nil,
			},
		},
		Portals:       &DefaultPortalCache{},
		ResponseQueue: NewResponseQueue(),
	}

	// Enqueue a previous event
	session.ResponseQueue.Enqueue(NewParseCompleteEvent())

	// Prepare Bind message for unknown statement
	inputBuf := &bytes.Buffer{}
	mockWriter := mock.NewWriter(t, inputBuf)
	mockWriter.Start(types.ClientBind)
	mockWriter.AddString("test_portal")
	mockWriter.AddNullTerminate()
	mockWriter.AddString("unknown_stmt")
	mockWriter.AddNullTerminate()
	mockWriter.AddInt16(0)
	mockWriter.AddInt16(0)
	mockWriter.AddInt16(0)
	require.NoError(t, mockWriter.End())

	reader := buffer.NewReader(logger, inputBuf, buffer.DefaultBufferSize)
	_, _, err := reader.ReadTypedMsg()
	require.NoError(t, err)

	outBuf := &bytes.Buffer{}
	writer := buffer.NewWriter(logger, outBuf)

	err = session.handleBind(ctx, reader, writer)
	require.NoError(t, err)

	assert.Equal(t, 0, session.ResponseQueue.Len())
	responseReader := mock.NewReader(t, outBuf)

	msgType, _, err := responseReader.ReadTypedMsg()
	require.NoError(t, err)
	assert.Equal(t, types.ServerParseComplete, msgType)

	msgType, _, err = responseReader.ReadTypedMsg()
	require.NoError(t, err)
	assert.Equal(t, types.ServerErrorResponse, msgType)
}
