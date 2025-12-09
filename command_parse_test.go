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

// TestHandleParse_ParallelPipeline_Success verifies that successful parse operations enqueue the right events
func TestHandleParse_ParallelPipeline_Success(t *testing.T) {
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

	logger := slogt.New(t)

	session := &Session{
		Server:           &Server{logger: logger, parse: mockParse},
		Statements:       &DefaultStatementCache{},
		ParallelPipeline: ParallelPipelineConfig{Enabled: true},
		ResponseQueue:    NewResponseQueue(),
	}

	reader := mock.NewParseReader(t, logger, "test_stmt", "SELECT 1", 0)

	outBuf := &bytes.Buffer{}
	writer := buffer.NewWriter(logger, outBuf)

	err := session.handleParse(ctx, reader, writer)
	require.NoError(t, err)

	// In parallel pipeline mode, nothing should be written to the wire immediately
	assert.Equal(t, 0, outBuf.Len(), "parallel pipeline should not write to wire on success")

	assert.Equal(t, 1, session.ResponseQueue.Len())
	events := session.ResponseQueue.DrainAll()
	require.Len(t, events, 1)
	assert.Equal(t, ResponseParseComplete, events[0].Kind)

	stmt, err := session.Statements.Get(ctx, "test_stmt")
	require.NoError(t, err)
	assert.NotNil(t, stmt)
}

// TestHandleParse_ParallelPipeline_MultipleCommands verifies queue accumulates multiple parse events
func TestHandleParse_ParallelPipeline_MultipleCommands(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	logger := slogt.New(t)
	mockParse := func(ctx context.Context, query string) (PreparedStatements, error) {
		stmt := NewStatement(func(ctx context.Context, writer DataWriter, parameters []Parameter) error { return nil })
		return PreparedStatements{stmt}, nil
	}

	session := &Session{
		Server:           &Server{logger: logger, parse: mockParse},
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
		reader := mock.NewParseReader(t, logger, q.name, q.query, 0)

		err := session.handleParse(ctx, reader, buffer.NewWriter(logger, &bytes.Buffer{}))
		require.NoError(t, err)
	}

	events := session.ResponseQueue.DrainAll()
	require.Len(t, events, 3)
	for _, event := range events {
		assert.Equal(t, ResponseParseComplete, event.Kind)
	}
}

// TestHandleParse_ParallelPipeline_Error verifies error handling drains the queue
func TestHandleParse_ParallelPipeline_Error(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	logger := slogt.New(t)

	mockParse := func(ctx context.Context, query string) (PreparedStatements, error) {
		if query == "INVALID SQL" {
			return nil, errors.New("syntax error at or near 'INVALID'")
		}
		return PreparedStatements{NewStatement(func(ctx context.Context, w DataWriter, p []Parameter) error { return nil })}, nil
	}

	session := &Session{
		Server:           &Server{logger: logger, parse: mockParse},
		Statements:       &DefaultStatementCache{},
		ParallelPipeline: ParallelPipelineConfig{Enabled: true},
		ResponseQueue:    NewResponseQueue(),
	}

	// Enqueue a previous event
	session.ResponseQueue.Enqueue(NewBindCompleteEvent())

	reader := mock.NewParseReader(t, logger, "bad_stmt", "INVALID SQL", 0)

	outBuf := &bytes.Buffer{}
	writer := buffer.NewWriter(logger, outBuf)

	err := session.handleParse(ctx, reader, writer)
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
