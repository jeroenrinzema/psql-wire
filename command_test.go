package wire

import (
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"sync"
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

	handler := func(ctx context.Context, query Query) (PreparedStatements, error) {
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

		return Prepared(NewStatement(handle, WithColumns(columns), WithParameters(ParseParameters(query.Query)))), nil
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

	handler := func(ctx context.Context, query Query) (PreparedStatements, error) {
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

	handler := func(ctx context.Context, query Query) (PreparedStatements, error) {
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

// TestClientParameterTypeMismatch demonstrates that when a client sends binary
// parameters using a smaller integer type (e.g. int2) than what the server
// would otherwise declare (e.g. int8), the ParseFn can use the parameterOIDs
// argument to match the client's types and decode correctly.
//
// This happens in practice with clients like psycopg3 that encode small Python
// ints as int2 (2 bytes binary), while the server-side resolved type may be
// int8 (bigint). PostgreSQL handles this via implicit casts at the planning
// level. psql-wire passes the client-specified parameter OIDs from the Parse
// message to ParseFn so handlers can do the same.
func TestClientParameterTypeMismatch(t *testing.T) {
	t.Parallel()

	columns := Columns{
		{
			Table: 0,
			Name:  "val",
			Oid:   pgtype.Int8OID,
			Width: 8,
		},
	}

	handler := func(ctx context.Context, query Query) (PreparedStatements, error) {
		// The ParameterOIDs slice tells us what types the client will
		// send binary data as. We can use this to set WithParameters to
		// match the client's types, so Scan decodes correctly.
		parameterOIDs := query.ParameterOIDs
		if len(parameterOIDs) == 0 {
			parameterOIDs = ParseParameters(query.Query)
		}

		handle := func(ctx context.Context, writer DataWriter, parameters []Parameter) error {
			val, err := parameters[0].Scan(parameterOIDs[0])
			if err != nil {
				return err
			}

			writer.Row([]any{val}) //nolint:errcheck
			return writer.Complete("SELECT 1")
		}

		return Prepared(NewStatement(handle,
			WithColumns(columns),
			WithParameters(parameterOIDs),
		)), nil
	}

	server, err := NewServer(handler, Logger(slogt.New(t)))
	require.NoError(t, err)

	address := TListenAndServe(t, server)

	ctx := context.Background()
	connstr := fmt.Sprintf("postgres://%s:%d", address.IP, address.Port)

	conn, err := pgx.Connect(ctx, connstr)
	require.NoError(t, err)
	defer conn.Close(ctx) //nolint:errcheck

	t.Run("int2 binary for int8 parameter", func(t *testing.T) {
		int2Bytes := make([]byte, 2)
		binary.BigEndian.PutUint16(int2Bytes, 42)

		result := conn.PgConn().ExecParams(ctx,
			"SELECT $1",
			[][]byte{int2Bytes},
			[]uint32{pgtype.Int2OID},
			[]int16{pgx.BinaryFormatCode},
			nil,
		)
		_, err := result.Close()
		assert.NoError(t, err, "handler should decode binary int2 via ClientOID()")
	})
}

// TestParseFnSimpleQueryFlag verifies that the Query.SimpleQuery flag reflects
// which protocol a query arrived on: true for the simple query protocol and
// false for the extended query (Parse/Bind/Execute) protocol.
func TestParseFnSimpleQueryFlag(t *testing.T) {
	t.Parallel()

	var mu sync.Mutex
	captured := map[string]Query{}

	handler := func(ctx context.Context, query Query) (PreparedStatements, error) {
		mu.Lock()
		captured[query.Query] = query
		mu.Unlock()

		handle := func(ctx context.Context, writer DataWriter, parameters []Parameter) error {
			return writer.Complete("SELECT 0")
		}
		return Prepared(NewStatement(handle)), nil
	}

	server, err := NewServer(handler, Logger(slogt.New(t)))
	require.NoError(t, err)

	address := TListenAndServe(t, server)

	ctx := context.Background()
	connstr := fmt.Sprintf("postgres://%s:%d", address.IP, address.Port)

	conn, err := pgx.Connect(ctx, connstr)
	require.NoError(t, err)
	defer conn.Close(ctx) //nolint:errcheck

	// The simple query protocol carries the query in a single Query message.
	_, err = conn.Exec(ctx, "SELECT 'simple'", pgx.QueryExecModeSimpleProtocol)
	require.NoError(t, err)

	// ExecParams always uses the extended query protocol, sending a Parse
	// message before Bind and Execute.
	result := conn.PgConn().ExecParams(ctx, "SELECT 'extended'", nil, nil, nil, nil)
	_, err = result.Close()
	require.NoError(t, err)

	mu.Lock()
	defer mu.Unlock()

	simple, ok := captured["SELECT 'simple'"]
	require.True(t, ok, "simple query should have reached the handler")
	assert.True(t, simple.SimpleQuery, "query received over the simple protocol should have SimpleQuery=true")
	assert.Nil(t, simple.ParameterOIDs, "simple queries cannot specify parameter OIDs")

	extended, ok := captured["SELECT 'extended'"]
	require.True(t, ok, "extended query should have reached the handler")
	assert.False(t, extended.SimpleQuery, "query received over the extended protocol should have SimpleQuery=false")
}
