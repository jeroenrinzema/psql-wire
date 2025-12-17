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

// TestHandleBind_ParallelPipeline_Success verifies that successful bind operations enqueue the right events
func TestHandleBind_ParallelPipeline_Success(t *testing.T) {
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
		Statements:       statements,
		Portals:          &DefaultPortalCache{},
		ParallelPipeline: ParallelPipelineConfig{Enabled: true},
		ResponseQueue:    NewResponseQueue(),
	}

	reader := mock.NewBindReader(t, logger, "test_portal", "test_stmt", 0, 0, 0)

	outBuf := &bytes.Buffer{}
	err := session.handleBind(ctx, reader, buffer.NewWriter(logger, outBuf))
	require.NoError(t, err)

	// In parallel pipeline mode, nothing should be written to the wire immediately
	assert.Equal(t, 0, outBuf.Len(), "parallel pipeline should not write to wire on success")

	assert.Equal(t, 1, session.ResponseQueue.Len())
	events := session.ResponseQueue.DrainAll()
	require.Len(t, events, 1)

	event := events[0]
	assert.Equal(t, ResponseBindComplete, event.Kind)
}

// TestHandleBind_ParallelPipeline_Error verifies error handling drains the queue
func TestHandleBind_ParallelPipeline_Error(t *testing.T) {
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
		Portals:          &DefaultPortalCache{},
		ParallelPipeline: ParallelPipelineConfig{Enabled: true},
		ResponseQueue:    NewResponseQueue(),
	}

	// Enqueue a previous event
	session.ResponseQueue.Enqueue(NewParseCompleteEvent())

	reader := mock.NewBindReader(t, logger, "test_portal", "unknown_stmt", 0, 0, 0)

	outBuf := &bytes.Buffer{}
	writer := buffer.NewWriter(logger, outBuf)

	err := session.handleBind(ctx, reader, writer)
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
