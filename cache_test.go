package wire

import (
	"bytes"
	"context"
	"log/slog"
	"testing"

	"github.com/jeroenrinzema/psql-wire/pkg/buffer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newDiscardWriter() *buffer.Writer {
	return buffer.NewWriter(slog.Default(), &bytes.Buffer{})
}

// TestCloseFromHandler verifies that calling Close from within a handler does
// not deadlock. The currently executing portal is automatically deferred and
// closed after Execute returns.
func TestCloseFromHandler(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cache := &DefaultPortalCache{}

	stmt := &Statement{
		fn: func(ctx context.Context, writer DataWriter, _ []Parameter) error {
			cache.Close()
			return writer.Complete("OK")
		},
	}

	require.NoError(t, cache.Bind(ctx, "", stmt, nil, nil))
	err := cache.Execute(ctx, "", NoLimit, nil, newDiscardWriter())
	require.NoError(t, err)

	portal, err := cache.Get(ctx, "")
	require.NoError(t, err)
	assert.Nil(t, portal)
}

// TestDeleteByStatementFromHandler verifies that calling DeleteByStatement
// from within a handler does not deadlock when the currently executing portal
// is bound to the deleted statement.
func TestDeleteByStatementFromHandler(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cache := &DefaultPortalCache{}

	var selfStmt *Statement
	selfStmt = &Statement{
		fn: func(ctx context.Context, writer DataWriter, _ []Parameter) error {
			err := cache.DeleteByStatement(ctx, selfStmt)
			require.NoError(t, err)
			return writer.Complete("OK")
		},
	}

	require.NoError(t, cache.Bind(ctx, "portal", selfStmt, nil, nil))
	err := cache.Execute(ctx, "portal", NoLimit, nil, newDiscardWriter())
	require.NoError(t, err)

	portal, err := cache.Get(ctx, "portal")
	require.NoError(t, err)
	assert.Nil(t, portal)
}
