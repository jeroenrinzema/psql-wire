package wire

import (
	"bytes"
	"context"
	"log/slog"
	"testing"

	"github.com/jeroenrinzema/psql-wire/codes"
	psqlerr "github.com/jeroenrinzema/psql-wire/errors"
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

// TestExecuteUnknownPortalReturnsError verifies that Execute on a portal that
// was never bound (or has already been closed) returns an error tagged with
// the InvalidCursorName code, matching real PostgreSQL behavior. Previously
// this was a silent no-op, which made it impossible for clients to detect
// that their portal had been invalidated.
func TestExecuteUnknownPortalReturnsError(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	t.Run("never bound", func(t *testing.T) {
		t.Parallel()
		cache := &DefaultPortalCache{}
		err := cache.Execute(ctx, "missing", NoLimit, nil, newDiscardWriter())
		require.Error(t, err)
		assert.Equal(t, codes.InvalidCursorName, psqlerr.GetCode(err))
		assert.Contains(t, err.Error(), `"missing"`)
	})

	t.Run("after close", func(t *testing.T) {
		t.Parallel()
		cache := &DefaultPortalCache{}
		stmt := &Statement{
			fn: func(ctx context.Context, writer DataWriter, _ []Parameter) error {
				return writer.Complete("OK")
			},
		}
		require.NoError(t, cache.Bind(ctx, "p", stmt, nil, nil))
		require.NoError(t, cache.Delete(ctx, "p"))

		err := cache.Execute(ctx, "p", NoLimit, nil, newDiscardWriter())
		require.Error(t, err)
		assert.Equal(t, codes.InvalidCursorName, psqlerr.GetCode(err))
	})
}
