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

func TestPortalSuspended(t *testing.T) {
	t.Parallel()

	totalRows := 5
	columns := Columns{
		{
			Table: 0,
			Name:  "id",
			Oid:   pgtype.Int4OID,
			Width: 4,
		},
	}

	handler := func(ctx context.Context, query string) (PreparedStatements, error) {
		handle := func(ctx context.Context, writer DataWriter, parameters []Parameter) error {
			for i := 0; i < totalRows; i++ {
				if err := writer.Row([]any{int32(i)}); err != nil {
					return err
				}
			}
			return writer.Complete("SELECT 5")
		}

		return Prepared(NewStatement(handle, WithColumns(columns))), nil
	}

	server, err := NewServer(handler, Logger(slogt.New(t)))
	require.NoError(t, err)

	address := TListenAndServe(t, server)
	conn, err := net.Dial("tcp", address.String())
	require.NoError(t, err)

	client := mock.NewClient(t, conn)
	client.Handshake(t)
	client.Authenticate(t)
	client.ReadyForQuery(t)

	for cycle := 0; cycle < 2; cycle++ {
		t.Logf("cycle %d", cycle)

		client.Parse(t, "stmt1", "SELECT id")
		client.ExpectMsg(t, types.ServerParseComplete)

		client.Bind(t, "portal1", "stmt1")
		client.ExpectMsg(t, types.ServerBindComplete)

		// Execute with limit=2 — should get rows 0 and 1
		client.Execute(t, "portal1", 2)
		rows := client.ExpectDataRows(t, 2)
		assert.Equal(t, "0", string(rows[0][0]))
		assert.Equal(t, "1", string(rows[1][0]))
		client.ExpectMsg(t, types.ServerPortalSuspended)

		// Execute with limit=10 (more than remaining 3 rows) — should get rows 2, 3, 4
		client.Execute(t, "portal1", 10)
		rows = client.ExpectDataRows(t, 3)
		assert.Equal(t, "2", string(rows[0][0]))
		assert.Equal(t, "3", string(rows[1][0]))
		assert.Equal(t, "4", string(rows[2][0]))
		client.ExpectMsg(t, types.ServerCommandComplete)

		client.Sync(t)
		client.ExpectMsg(t, types.ServerReady)
	}

	client.Close(t)
}

func TestReExecuteCompletedPortal(t *testing.T) {
	t.Parallel()

	columns := Columns{
		{
			Table: 0,
			Name:  "id",
			Oid:   pgtype.Int4OID,
			Width: 4,
		},
	}

	handler := func(ctx context.Context, query string) (PreparedStatements, error) {
		handle := func(ctx context.Context, writer DataWriter, parameters []Parameter) error {
			if err := writer.Row([]any{int32(1)}); err != nil {
				return err
			}
			return writer.Complete("SELECT 1")
		}

		return Prepared(NewStatement(handle, WithColumns(columns))), nil
	}

	server, err := NewServer(handler, Logger(slogt.New(t)))
	require.NoError(t, err)

	address := TListenAndServe(t, server)
	conn, err := net.Dial("tcp", address.String())
	require.NoError(t, err)

	client := mock.NewClient(t, conn)
	client.Handshake(t)
	client.Authenticate(t)
	client.ReadyForQuery(t)

	client.Parse(t, "stmt1", "SELECT id")
	client.ExpectMsg(t, types.ServerParseComplete)

	client.Bind(t, "portal1", "stmt1")
	client.ExpectMsg(t, types.ServerBindComplete)

	// Execute with limit=2 — query only produces 1 row
	client.Execute(t, "portal1", 2)
	rows := client.ExpectDataRows(t, 1)
	assert.Equal(t, "1", string(rows[0][0]))
	tag := client.ExpectCommandComplete(t)
	assert.Equal(t, "SELECT 1", tag)

	// Re-execute the same portal — should get CommandComplete with the same tag
	client.Execute(t, "portal1", 0)
	reTag := client.ExpectCommandComplete(t)
	assert.Equal(t, "SELECT 1", reTag)

	client.Sync(t)
	client.ExpectMsg(t, types.ServerReady)

	client.Close(t)
}
