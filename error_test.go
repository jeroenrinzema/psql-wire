package wire

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jeroenrinzema/psql-wire/codes"
	psqlerr "github.com/jeroenrinzema/psql-wire/errors"
	"github.com/jeroenrinzema/psql-wire/pkg/buffer"
	"github.com/jeroenrinzema/psql-wire/pkg/types"
	"github.com/neilotoole/slogt"
	"github.com/stretchr/testify/assert"
)

func TestErrorCode(t *testing.T) {
	handler := func(ctx context.Context, query string) (PreparedStatements, error) {
		stmt := NewStatement(func(ctx context.Context, writer DataWriter, parameters []Parameter) error {
			return psqlerr.WithSeverity(psqlerr.WithCode(errors.New("unimplemented feature"), codes.FeatureNotSupported), psqlerr.LevelFatal)
		})

		return Prepared(stmt), nil
	}

	server, err := NewServer(handler, Logger(slogt.New(t)))
	assert.NoError(t, err)

	address := TListenAndServe(t, server)

	t.Run("lib/pq", func(t *testing.T) {
		connstr := fmt.Sprintf("host=%s port=%d sslmode=disable", address.IP, address.Port)
		conn, err := sql.Open("postgres", connstr)
		assert.NoError(t, err)

		_, err = conn.Query("SELECT *;")
		assert.Error(t, err)

		err = conn.Close()
		assert.NoError(t, err)
	})

	t.Run("jackc/pgx", func(t *testing.T) {
		ctx := context.Background()
		connstr := fmt.Sprintf("postgres://%s:%d", address.IP, address.Port)
		conn, err := pgx.Connect(ctx, connstr)
		assert.NoError(t, err)

		rows, _ := conn.Query(ctx, "SELECT *;")
		rows.Close()
		assert.Error(t, rows.Err())

		err = conn.Close(ctx)
		assert.NoError(t, err)
	})
}

func TestErrorCodeAuthFailure(t *testing.T) {
	t.Run("regular error includes ready for query", func(t *testing.T) {
		sink := bytes.NewBuffer([]byte{})
		writer := buffer.NewWriter(slogt.New(t), sink)

		// Regular error should include ready for query message
		err := ErrorCode(writer, psqlerr.WithCode(errors.New("some error"), codes.Syntax))
		assert.NoError(t, err)

		// Check that we have both ErrorResponse and Ready messages
		reader := buffer.NewReader(slogt.New(t), sink, buffer.DefaultBufferSize)

		// First message should be ErrorResponse
		msgType, _, err := reader.ReadTypedMsg()
		assert.NoError(t, err)
		assert.Equal(t, types.ServerMessage(msgType), types.ServerErrorResponse)

		// Second message should be Ready
		msgType, _, err = reader.ReadTypedMsg()
		assert.NoError(t, err)
		assert.Equal(t, types.ServerMessage(msgType), types.ServerReady)
	})

	t.Run("auth failure skips ready for query", func(t *testing.T) {
		sink := bytes.NewBuffer([]byte{})
		writer := buffer.NewWriter(slogt.New(t), sink)

		// Authentication error should NOT include ready for query message
		err := ErrorCode(writer, psqlerr.WithCode(errors.New("invalid username/password"), codes.InvalidPassword))
		assert.NoError(t, err)

		// Check that we only have ErrorResponse, no Ready message
		reader := buffer.NewReader(slogt.New(t), sink, buffer.DefaultBufferSize)

		// First message should be ErrorResponse
		msgType, _, err := reader.ReadTypedMsg()
		assert.NoError(t, err)
		assert.Equal(t, types.ServerMessage(msgType), types.ServerErrorResponse)

		// There should be no more messages
		_, _, err = reader.ReadTypedMsg()
		assert.Error(t, err) // Should get EOF or similar
	})
}
