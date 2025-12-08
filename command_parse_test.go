package wire

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/jeroenrinzema/psql-wire/pkg/buffer"
	"github.com/jeroenrinzema/psql-wire/pkg/mock"
	"github.com/jeroenrinzema/psql-wire/pkg/types"
	"github.com/lib/pq/oid"
	"github.com/neilotoole/slogt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestHandleParseSuccess verifies that successful parse operations enqueue the right events
func TestHandleParseSuccess(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	mockParse := func(ctx context.Context, query string) (PreparedStatements, error) {
		stmt := NewStatement(
			func(ctx context.Context, writer DataWriter, parameters []Parameter) error { return nil },
			WithParameters([]oid.Oid{oid.T_text, oid.T_int4}),
			WithColumns(Columns{{Name: "id", Oid: oid.T_int4}, {Name: "name", Oid: oid.T_text}}),
		)
		return PreparedStatements{stmt}, nil
	}

	session := &Session{
		Server:           &Server{logger: slogt.New(t), parse: mockParse},
		Statements:       &DefaultStatementCache{},
		ParallelPipeline: ParallelPipelineConfig{Enabled: true},
		ResponseQueue:    NewResponseQueue(),
	}

	inputBuf := &bytes.Buffer{}
	mockWriter := mock.NewWriter(t, inputBuf)
	mockWriter.Start(types.ClientParse)
	mockWriter.AddString("test_stmt")
	mockWriter.AddNullTerminate()
	mockWriter.AddString("SELECT 1")
	mockWriter.AddNullTerminate()
	mockWriter.AddInt16(0)
	require.NoError(t, mockWriter.End())

	reader := buffer.NewReader(slogt.New(t), inputBuf, buffer.DefaultBufferSize)
	msgType, _, err := reader.ReadTypedMsg()
	require.NoError(t, err)
	require.Equal(t, types.ClientMessage(types.ClientParse), msgType)

	outBuf := &bytes.Buffer{}
	writer := buffer.NewWriter(slogt.New(t), outBuf)

	err = session.handleParse(ctx, reader, writer)
	require.NoError(t, err)

	assert.Equal(t, 1, session.ResponseQueue.Len())
	events := session.ResponseQueue.DrainAll()
	require.Len(t, events, 1)
	assert.Equal(t, ResponseParseComplete, events[0].Kind)

	stmt, err := session.Statements.Get(ctx, "test_stmt")
	require.NoError(t, err)
	assert.NotNil(t, stmt)
}

// TestHandleParseMultipleCommands verifies queue accumulates multiple parse events
func TestHandleParseMultipleCommands(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	mockParse := func(ctx context.Context, query string) (PreparedStatements, error) {
		stmt := NewStatement(func(ctx context.Context, writer DataWriter, parameters []Parameter) error { return nil })
		return PreparedStatements{stmt}, nil
	}

	session := &Session{
		Server:           &Server{logger: slogt.New(t), parse: mockParse},
		Statements:       &DefaultStatementCache{},
		ParallelPipeline: ParallelPipelineConfig{Enabled: true},
		ResponseQueue:    NewResponseQueue(),
	}

	queries := []struct {
		name  string
		query string
	}{
		{"stmt1", "SELECT 1"},
		{"stmt2", "SELECT 2"},
		{"stmt3", "SELECT 3"},
	}

	for _, q := range queries {
		inputBuf := &bytes.Buffer{}
		mockWriter := mock.NewWriter(t, inputBuf)
		mockWriter.Start(types.ClientParse)
		mockWriter.AddString(q.name)
		mockWriter.AddNullTerminate()
		mockWriter.AddString(q.query)
		mockWriter.AddNullTerminate()
		mockWriter.AddInt16(0)
		require.NoError(t, mockWriter.End())

		reader := buffer.NewReader(slogt.New(t), inputBuf, buffer.DefaultBufferSize)
		_, _, err := reader.ReadTypedMsg()
		require.NoError(t, err)

		err = session.handleParse(ctx, reader, buffer.NewWriter(slogt.New(t), &bytes.Buffer{}))
		require.NoError(t, err)
	}

	events := session.ResponseQueue.DrainAll()
	require.Len(t, events, 3)
	for _, event := range events {
		assert.Equal(t, ResponseParseComplete, event.Kind)
	}
}

// TestHandleParseError verifies error handling drains the queue
func TestHandleParseError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	mockParse := func(ctx context.Context, query string) (PreparedStatements, error) {
		if query == "INVALID SQL" {
			return nil, errors.New("syntax error at or near 'INVALID'")
		}
		return PreparedStatements{NewStatement(func(ctx context.Context, w DataWriter, p []Parameter) error { return nil })}, nil
	}

	session := &Session{
		Server:           &Server{logger: slogt.New(t), parse: mockParse},
		Statements:       &DefaultStatementCache{},
		ParallelPipeline: ParallelPipelineConfig{Enabled: true},
		ResponseQueue:    NewResponseQueue(),
	}

	// Enqueue a previous event
	session.ResponseQueue.Enqueue(NewBindCompleteEvent())

	inputBuf := &bytes.Buffer{}
	mockWriter := mock.NewWriter(t, inputBuf)
	mockWriter.Start(types.ClientParse)
	mockWriter.AddString("bad_stmt")
	mockWriter.AddNullTerminate()
	mockWriter.AddString("INVALID SQL")
	mockWriter.AddNullTerminate()
	mockWriter.AddInt16(0)
	require.NoError(t, mockWriter.End())

	reader := buffer.NewReader(slogt.New(t), inputBuf, buffer.DefaultBufferSize)
	_, _, err := reader.ReadTypedMsg()
	require.NoError(t, err)

	outBuf := &bytes.Buffer{}
	writer := buffer.NewWriter(slogt.New(t), outBuf)

	err = session.handleParse(ctx, reader, writer)
	require.NoError(t, err)

	assert.Equal(t, 0, session.ResponseQueue.Len())

	responseReader := mock.NewReader(t, outBuf)

	// 1. Expect BindComplete
	msgType, _, err := responseReader.ReadTypedMsg()
	require.NoError(t, err)
	assert.Equal(t, types.ServerBindComplete, msgType)

	// 2. Expect ErrorResponse
	msgType, _, err = responseReader.ReadTypedMsg()
	require.NoError(t, err)
	assert.Equal(t, types.ServerErrorResponse, msgType)
}
