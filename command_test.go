package wire

import (
	"context"
	"fmt"
	"net"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jeroenrinzema/psql-wire/pkg/buffer"
	"github.com/jeroenrinzema/psql-wire/pkg/mock"
	"github.com/jeroenrinzema/psql-wire/pkg/types"
	"github.com/neilotoole/slogt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMessageSizeExceeded(t *testing.T) {
	server, err := NewServer(nil, Logger(slogt.New(t)))
	require.NoError(t, err)

	address := TListenAndServe(t, server)
	conn, err := net.Dial("tcp", address.String())
	require.NoError(t, err)

	client := mock.NewClient(t, conn)
	client.Handshake(t)
	client.Authenticate(t)
	client.ReadyForQuery(t)

	// NOTE: attempt to send a message twice the max buffer size
	size := uint32(buffer.DefaultBufferSize * 2)
	t.Logf("writing message of size: %d", size)

	client.Start(types.ClientSimpleQuery)
	client.AddBytes(make([]byte, size))
	err = client.End()
	require.NoError(t, err)

	client.Error(t)
	client.Close(t)
}

func TestBindMessageParameters(t *testing.T) {
	t.Parallel()

	columns := Columns{
		{
			Table: 0,
			Name:  "full_name",
			Oid:   pgtype.TextOID,
			Width: 256,
		},
		{
			Table: 0,
			Name:  "answer_to_life_the_universe_and_everything",
			Oid:   pgtype.TextOID,
			Width: 256,
		},
	}

	handler := func(ctx context.Context, query string) (PreparedStatements, error) {
		handle := func(ctx context.Context, writer DataWriter, parameters []Parameter) error {
			t.Log("serving query")

			if len(parameters) != 2 {
				return fmt.Errorf("unexpected amount of parameters %d, expected 2", len(parameters))
			}

			first := string(parameters[0].value)
			second := string(parameters[1].value)

			writer.Row([]any{first, second}) //nolint:errcheck
			return writer.Complete("SELECT 1")
		}

		return Prepared(NewStatement(handle, WithColumns(columns), WithParameters(ParseParameters(query)))), nil
	}

	server, err := NewServer(handler, Logger(slogt.New(t)))
	require.NoError(t, err)

	address := TListenAndServe(t, server)

	ctx := context.Background()
	connstr := fmt.Sprintf("postgres://%s:%d", address.IP, address.Port)

	t.Run("pgx", func(t *testing.T) {
		conn, err := pgx.Connect(ctx, connstr)
		require.NoError(t, err)

		defer conn.Close(ctx) //nolint:errcheck

		rows, err := conn.Query(ctx, "SELECT $1 $2;", "John Doe", 42)
		require.NoError(t, err)

		assert.True(t, rows.Next())

		var name string
		var answer string

		err = rows.Scan(&name, &answer)
		require.NoError(t, err)

		t.Logf("scan result: %s, %s", name, answer)

		assert.Equal(t, name, "John Doe")
		assert.Equal(t, answer, "42")

		assert.False(t, rows.Next())

		rows.Close()

		err = conn.Close(ctx)
		require.NoError(t, err)
	})

	t.Run("pgx nil parameter", func(t *testing.T) {
		conn, err := pgx.Connect(ctx, connstr)
		require.NoError(t, err)

		defer conn.Close(ctx) //nolint:errcheck

		// changed to nil
		rows, err := conn.Query(ctx, "SELECT $1 $2;", "John Doe", nil)
		require.NoError(t, err)

		assert.True(t, rows.Next())

		var name string
		var answer string

		err = rows.Scan(&name, &answer)
		require.NoError(t, err)

		t.Logf("scan result: %s, %s", name, answer)

		assert.Equal(t, name, "John Doe")
		assert.Equal(t, answer, "")

		assert.False(t, rows.Next())

		rows.Close()

		err = conn.Close(ctx)
		require.NoError(t, err)
	})
}

func TestServerLimit(t *testing.T) {
	server, err := NewServer(nil, Logger(slogt.New(t)))
	require.NoError(t, err)

	address := TListenAndServe(t, server)
	conn, err := net.Dial("tcp", address.String())
	require.NoError(t, err)

	client := mock.NewClient(t, conn)
	client.Handshake(t)
	client.Authenticate(t)
	client.ReadyForQuery(t)

	// client.Start(types.ClientExecute)
	// client.AddString("limited")
	// client.AddInt32(1)
	// err = client.End()
	// require.NoError(t, err)

	client.Close(t)
}
