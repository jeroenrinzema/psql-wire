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

func TestHandleClose_Statement(t *testing.T) {
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

	reader := mock.NewCloseReader(t, logger, 'S', "stmt1")

	err := session.handleClose(ctx, reader, writer)
	require.NoError(t, err)

	responseReader := mock.NewReader(t, outBuf)
	msgType, _, err := responseReader.ReadTypedMsg()
	require.NoError(t, err)
	assert.Equal(t, types.ServerCloseComplete, msgType)
}

func TestHandleClose_Portal(t *testing.T) {
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

	reader := mock.NewCloseReader(t, logger, 'P', "portal1")

	err := session.handleClose(ctx, reader, writer)
	require.NoError(t, err)

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

	reader := mock.NewCloseReader(t, logger, 'S', "stmt1")

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
