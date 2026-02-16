package wire

import (
	"bytes"
	"context"
	"testing"

	"github.com/jeroenrinzema/psql-wire/pkg/buffer"
	"github.com/jeroenrinzema/psql-wire/pkg/mock"
	"github.com/jeroenrinzema/psql-wire/pkg/types"
	"github.com/neilotoole/slogt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandleClose_StatementRemovesFromCache(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	logger := slogt.New(t)

	stmtCache := &DefaultStatementCache{}
	portalCache := &DefaultPortalCache{}

	stmt := &PreparedStatement{
		fn: func(ctx context.Context, writer DataWriter, parameters []Parameter) error {
			return writer.Complete("OK")
		},
	}

	err := stmtCache.Set(ctx, "stmt1", stmt)
	require.NoError(t, err)

	// Bind two portals to the same statement
	cached, err := stmtCache.Get(ctx, "stmt1")
	require.NoError(t, err)
	require.NotNil(t, cached)

	err = portalCache.Bind(ctx, "p1", cached, nil, nil)
	require.NoError(t, err)
	err = portalCache.Bind(ctx, "p2", cached, nil, nil)
	require.NoError(t, err)

	// Bind a portal to a different statement so we can verify it survives
	otherStmt := &PreparedStatement{
		fn: func(ctx context.Context, writer DataWriter, parameters []Parameter) error {
			return writer.Complete("OK")
		},
	}
	err = stmtCache.Set(ctx, "other", otherStmt)
	require.NoError(t, err)
	otherCached, err := stmtCache.Get(ctx, "other")
	require.NoError(t, err)
	err = portalCache.Bind(ctx, "p3", otherCached, nil, nil)
	require.NoError(t, err)

	session := &Session{
		Server:     &Server{logger: logger},
		Statements: stmtCache,
		Portals:    portalCache,
	}

	outBuf := &bytes.Buffer{}
	writer := buffer.NewWriter(logger, outBuf)

	err = session.handleClose(ctx, mock.NewCloseReader(t, logger, types.CloseStatement, "stmt1"), writer)
	require.NoError(t, err)

	// Statement should be removed
	got, err := stmtCache.Get(ctx, "stmt1")
	require.NoError(t, err)
	assert.Nil(t, got)

	// Portals bound to the closed statement should be removed
	p1, err := portalCache.Get(ctx, "p1")
	require.NoError(t, err)
	assert.Nil(t, p1)

	p2, err := portalCache.Get(ctx, "p2")
	require.NoError(t, err)
	assert.Nil(t, p2)

	// Portal bound to a different statement should still exist
	p3, err := portalCache.Get(ctx, "p3")
	require.NoError(t, err)
	assert.NotNil(t, p3)

	// CloseComplete should still be sent
	responseReader := mock.NewReader(t, outBuf)
	msgType, _, err := responseReader.ReadTypedMsg()
	require.NoError(t, err)
	assert.Equal(t, types.ServerCloseComplete, msgType)
}

func TestHandleClose_NonexistentNameSendsCloseComplete(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	logger := slogt.New(t)

	session := &Session{
		Server:     &Server{logger: logger},
		Statements: &DefaultStatementCache{},
		Portals:    &DefaultPortalCache{},
	}

	outBuf := &bytes.Buffer{}
	writer := buffer.NewWriter(logger, outBuf)

	// Close a statement that doesn't exist
	err := session.handleClose(ctx, mock.NewCloseReader(t, logger, types.CloseStatement, "nonexistent"), writer)
	require.NoError(t, err)

	responseReader := mock.NewReader(t, outBuf)
	msgType, _, err := responseReader.ReadTypedMsg()
	require.NoError(t, err)
	assert.Equal(t, types.ServerCloseComplete, msgType)

	// Close a portal that doesn't exist
	outBuf.Reset()
	err = session.handleClose(ctx, mock.NewCloseReader(t, logger, types.ClosePortal, "nonexistent"), writer)
	require.NoError(t, err)

	responseReader = mock.NewReader(t, outBuf)
	msgType, _, err = responseReader.ReadTypedMsg()
	require.NoError(t, err)
	assert.Equal(t, types.ServerCloseComplete, msgType)
}

func TestHandleClose_PortalRemovesFromCache(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	logger := slogt.New(t)

	stmtCache := &DefaultStatementCache{}
	portalCache := &DefaultPortalCache{}

	stmt := &PreparedStatement{
		fn: func(ctx context.Context, writer DataWriter, parameters []Parameter) error {
			return writer.Complete("OK")
		},
	}

	err := stmtCache.Set(ctx, "stmt1", stmt)
	require.NoError(t, err)
	cached, err := stmtCache.Get(ctx, "stmt1")
	require.NoError(t, err)

	err = portalCache.Bind(ctx, "portal1", cached, nil, nil)
	require.NoError(t, err)

	session := &Session{
		Server:     &Server{logger: logger},
		Statements: stmtCache,
		Portals:    portalCache,
	}

	outBuf := &bytes.Buffer{}
	writer := buffer.NewWriter(logger, outBuf)

	err = session.handleClose(ctx, mock.NewCloseReader(t, logger, types.ClosePortal, "portal1"), writer)
	require.NoError(t, err)

	// Portal should be removed
	portal, err := portalCache.Get(ctx, "portal1")
	require.NoError(t, err)
	assert.Nil(t, portal)

	// Statement should still exist
	got, err := stmtCache.Get(ctx, "stmt1")
	require.NoError(t, err)
	assert.NotNil(t, got)

	responseReader := mock.NewReader(t, outBuf)
	msgType, _, err := responseReader.ReadTypedMsg()
	require.NoError(t, err)
	assert.Equal(t, types.ServerCloseComplete, msgType)
}

func TestHandleClose_ParallelPipeline(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	logger := slogt.New(t)

	session := &Session{
		Server:           &Server{logger: logger},
		Statements:       &DefaultStatementCache{},
		Portals:          &DefaultPortalCache{},
		ParallelPipeline: ParallelPipelineConfig{Enabled: true},
		ResponseQueue:    NewResponseQueue(),
	}

	outBuf := &bytes.Buffer{}
	writer := buffer.NewWriter(logger, outBuf)

	reader := mock.NewCloseReader(t, logger, types.CloseStatement, "stmt1")

	err := session.handleClose(ctx, reader, writer)
	require.NoError(t, err)

	// Should be queued, not written directly
	assert.Equal(t, 1, session.ResponseQueue.Len())
	assert.Equal(t, 0, outBuf.Len())

	// Sync flushes the queue
	err = session.handleSync(ctx, writer)
	require.NoError(t, err)

	responseReader := mock.NewReader(t, outBuf)

	msgType, _, err := responseReader.ReadTypedMsg()
	require.NoError(t, err)
	assert.Equal(t, types.ServerCloseComplete, msgType)

	msgType, _, err = responseReader.ReadTypedMsg()
	require.NoError(t, err)
	assert.Equal(t, types.ServerReady, msgType)
}
