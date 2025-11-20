package wire

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/jeroenrinzema/psql-wire/pkg/mock"
	"github.com/jeroenrinzema/psql-wire/pkg/types"
	"github.com/lib/pq"
	"github.com/lib/pq/oid"
	"github.com/neilotoole/slogt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TListenAndServe will open a new TCP listener on a unallocated port inside
// the local network. The newly created listener is passed to the given server to
// start serving PostgreSQL connections. The full listener address is returned
// for clients to interact with the newly created server.
func TListenAndServe(t *testing.T, server *Server) *net.TCPAddr {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		err := server.Close()
		if err != nil {
			t.Fatal(err)
		}
	})

	go server.Serve(listener) //nolint:errcheck
	return listener.Addr().(*net.TCPAddr)
}

func TestClientConnect(t *testing.T) {
	t.Parallel()

	handler := func(ctx context.Context, query string) (PreparedStatements, error) {
		statement := NewStatement(func(ctx context.Context, writer DataWriter, parameters []Parameter) error {
			t.Log("serving query")
			return writer.Complete("OK")
		})

		return Prepared(statement), nil
	}

	server, err := NewServer(handler, Logger(slogt.New(t)))
	if err != nil {
		t.Fatal(err)
	}

	address := TListenAndServe(t, server)

	t.Run("mock", func(t *testing.T) {
		conn, err := net.Dial("tcp", address.String())
		if err != nil {
			t.Fatal(err)
		}

		client := mock.NewClient(t, conn)
		client.Handshake(t)
		client.Authenticate(t)
		client.ReadyForQuery(t)
		client.Close(t)
	})

	t.Run("lib/pq", func(t *testing.T) {
		connstr := fmt.Sprintf("host=%s port=%d sslmode=disable", address.IP, address.Port)
		conn, err := sql.Open("postgres", connstr)
		if err != nil {
			t.Fatal(err)
		}

		err = conn.Ping()
		if err != nil {
			t.Fatal(err)
		}

		err = conn.Close()
		if err != nil {
			t.Fatal(err)
		}
	})

	t.Run("jackc/pgx", func(t *testing.T) {
		ctx := context.Background()
		connstr := fmt.Sprintf("postgres://%s:%d", address.IP, address.Port)
		conn, err := pgx.Connect(ctx, connstr)
		if err != nil {
			t.Fatal(err)
		}

		err = conn.Ping(ctx)
		if err != nil {
			t.Fatal(err)
		}

		err = conn.Close(ctx)
		if err != nil {
			t.Fatal(err)
		}
	})
}

func TestClientParameters(t *testing.T) {
	t.Parallel()

	handler := func(ctx context.Context, query string) (PreparedStatements, error) {
		handle := func(ctx context.Context, writer DataWriter, parameters []Parameter) error {
			writer.Row([]any{"John Doe"}) //nolint:errcheck
			return writer.Complete("SELECT 1")
		}

		columns := Columns{
			{
				Table: 0,
				Name:  "full_name",
				Oid:   oid.T_text,
				Width: 256,
			},
		}

		return Prepared(NewStatement(handle, WithColumns(columns), WithParameters(ParseParameters(query)))), nil
	}

	server, err := NewServer(handler, Logger(slogt.New(t)))
	if err != nil {
		t.Fatal(err)
	}

	address := TListenAndServe(t, server)

	t.Run("lib/pq", func(t *testing.T) {
		connstr := fmt.Sprintf("host=%s port=%d sslmode=disable", address.IP, address.Port)
		conn, err := sql.Open("postgres", connstr)
		if err != nil {
			t.Fatal(err)
		}

		rows, err := conn.Query("SELECT * FROM users WHERE age > ?", 50)
		if err != nil {
			t.Fatal(err)
		}

		err = rows.Close()
		if err != nil {
			t.Fatal(err)
		}

		err = conn.Close()
		if err != nil {
			t.Fatal(err)
		}
	})

	t.Run("jackc/pgx", func(t *testing.T) {
		ctx := context.Background()
		connstr := fmt.Sprintf("postgres://%s:%d", address.IP, address.Port)
		conn, err := pgx.Connect(ctx, connstr)
		if err != nil {
			t.Fatal(err)
		}

		rows, err := conn.Query(ctx, "SELECT * FROM users WHERE age > ?", 50)
		if err != nil {
			t.Fatal(err)
		}

		rows.Close()

		err = conn.Close(ctx)
		if err != nil {
			t.Fatal(err)
		}
	})
}

func TestServerWritingResult(t *testing.T) {
	t.Parallel()

	handler := func(ctx context.Context, query string) (PreparedStatements, error) {
		handle := func(ctx context.Context, writer DataWriter, parameters []Parameter) error {
			t.Log("serving query")
			writer.Row([]any{"John", true, 28})   //nolint:errcheck
			writer.Row([]any{"Marry", false, 21}) //nolint:errcheck
			return writer.Complete("SELECT 2")
		}

		columns := Columns{ //nolint:errcheck
			{
				Table: 0,
				Name:  "name",
				Oid:   oid.T_text,
				Width: 256,
			},
			{
				Table: 0,
				Name:  "member",
				Oid:   oid.T_bool,
				Width: 1,
			},
			{
				Table: 0,
				Name:  "age",
				Oid:   oid.T_int4,
				Width: 1,
			},
		}

		return Prepared(NewStatement(handle, WithColumns(columns))), nil
	}

	server, err := NewServer(handler, Logger(slogt.New(t)))
	if err != nil {
		t.Fatal(err)
	}

	address := TListenAndServe(t, server)

	t.Run("lib/pq", func(t *testing.T) {
		connstr := fmt.Sprintf("host=%s port=%d sslmode=disable", address.IP, address.Port)
		conn, err := sql.Open("postgres", connstr)
		if err != nil {
			t.Fatal(err)
		}

		rows, err := conn.Query("SELECT *;")
		if err != nil {
			t.Fatal(err)
		}

		for rows.Next() {
			var name string
			var member bool
			var age int

			err := rows.Scan(&name, &member, &age)
			if err != nil {
				t.Fatal(err)
			}

			t.Logf("scan result: %s, %d, %t", name, age, member)
		}
		err = conn.Close()
		if err != nil {
			t.Fatal(err)
		}
	})

	t.Run("jackc/pgx", func(t *testing.T) {
		ctx := context.Background()
		connstr := fmt.Sprintf("postgres://%s:%d", address.IP, address.Port)
		conn, err := pgx.Connect(ctx, connstr)
		if err != nil {
			t.Fatal(err)
		}

		rows, err := conn.Query(ctx, "SELECT *;")
		if err != nil {
			t.Fatal(err)
		}

		for rows.Next() {
			var name string
			var member bool
			var age int

			err := rows.Scan(&name, &member, &age)
			if err != nil {
				t.Fatal(err)
			}

			t.Logf("scan result: %s, %d, %t", name, age, member)
		}

		err = conn.Close(ctx)
		if err != nil {
			t.Fatal(err)
		}
	})
}

func TestServerHandlingMultipleConnections(t *testing.T) {
	address := TOpenMockServer(t)
	connstr := fmt.Sprintf("postgres://%s:%d", address.IP, address.Port)
	conn, err := sql.Open("pgx", connstr)
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = conn.Close()
	})

	err = conn.Ping()
	require.NoError(t, err)

	t.Run("simple query", func(t *testing.T) {
		rows, err := conn.Query("select age from person")
		require.NoError(t, err)
		t.Cleanup(func() {
			rows.Close() //nolint:errcheck
		})
		assert.True(t, rows.Next())
		require.NoError(t, rows.Err())
	})

	t.Run("prepared statements", func(t *testing.T) {
		tests := []string{
			"select age from person where age > $1",
			"select age from person where age > ?",
		}

		for _, query := range tests {
			t.Run(query, func(t *testing.T) {
				stmt, err := conn.Prepare(query)
				require.NoError(t, err)
				t.Cleanup(func() {
					stmt.Close() //nolint:errcheck
				})
				rows, err := stmt.Query(1)
				require.NoError(t, err)
				t.Cleanup(func() {
					rows.Close() //nolint:errcheck
				})
				require.True(t, rows.Next())
				require.NoError(t, rows.Err())
			})
		}
	})
}

func TOpenMockServer(t *testing.T) *net.TCPAddr {
	t.Helper()
	handler := func(ctx context.Context, query string) (PreparedStatements, error) {
		handle := func(ctx context.Context, writer DataWriter, parameters []Parameter) error {
			t.Log("serving query")
			writer.Row([]any{20}) //nolint:errcheck
			return writer.Complete("SELECT 1")
		}

		columns := Columns{
			{
				Table: 0,
				Name:  "age",
				Oid:   oid.T_int4,
				Width: 1,
			},
		}

		return Prepared(NewStatement(handle, WithColumns(columns), WithParameters(ParseParameters(query)))), nil
	}

	server, err := NewServer(handler, Logger(slogt.New(t)))
	require.NoError(t, err)
	address := TListenAndServe(t, server)
	return address
}

func TestServerNULLValues(t *testing.T) {
	t.Parallel()

	name := "John"
	expected := []*string{
		&name,
		nil,
	}

	handler := func(ctx context.Context, query string) (PreparedStatements, error) {
		handle := func(ctx context.Context, writer DataWriter, parameters []Parameter) error {
			t.Log("serving query")
			writer.Row([]any{"John"}) //nolint:errcheck
			writer.Row([]any{nil})    //nolint:errcheck
			return writer.Complete("SELECT 2")
		}

		columns := Columns{
			{
				Table: 0,
				Name:  "name",
				Oid:   oid.T_text,
				Width: 256,
			},
		}

		return Prepared(NewStatement(handle, WithColumns(columns))), nil
	}

	server, err := NewServer(handler, Logger(slogt.New(t)))
	if err != nil {
		t.Fatal(err)
	}

	address := TListenAndServe(t, server)

	t.Run("lib/pq", func(t *testing.T) {
		connstr := fmt.Sprintf("host=%s port=%d sslmode=disable", address.IP, address.Port)
		conn, err := sql.Open("postgres", connstr)
		if err != nil {
			t.Fatal(err)
		}

		rows, err := conn.Query("SELECT *;")
		if err != nil {
			t.Fatal(err)
		}

		result := []*string{}
		for rows.Next() {
			var name *string
			err := rows.Scan(&name)
			if err != nil {
				t.Fatal(err)
			}

			t.Logf("scan result: %+v", name)
			result = append(result, name)
		}

		if len(result) != len(expected) {
			t.Fatal("an unexpected amount of records was returned")
		}

		for index := range expected {
			switch {
			case expected[index] == nil:
				if result[index] != nil {
					t.Errorf("unexpected value %+v, expected nil", result[index])
				}
			case expected[index] != nil:
				left := *expected[index]
				right := *result[index]

				if left != right {
					t.Errorf("unexpected value %+v, expected %+v", left, right)
				}
			}
		}

		err = conn.Close()
		if err != nil {
			t.Fatal(err)
		}
	})

	t.Run("jackc/pgx", func(t *testing.T) {
		ctx := context.Background()
		connstr := fmt.Sprintf("postgres://%s:%d", address.IP, address.Port)
		conn, err := pgx.Connect(ctx, connstr)
		if err != nil {
			t.Fatal(err)
		}

		rows, err := conn.Query(ctx, "SELECT *;")
		if err != nil {
			t.Fatal(err)
		}

		result := []*string{}
		for rows.Next() {
			var name *string
			err := rows.Scan(&name)
			if err != nil {
				t.Fatal(err)
			}

			t.Logf("scan result: %+v", name)
			result = append(result, name)
		}

		for index := range expected {
			switch {
			case expected[index] == nil:
				if result[index] != nil {
					t.Errorf("unexpected value %+v, expected nil", result[index])
				}
			case expected[index] != nil:
				left := *expected[index]
				right := *result[index]

				if left != right {
					t.Errorf("unexpected value %+v, expected %+v", left, right)
				}
			}
		}

		err = conn.Close(ctx)
		if err != nil {
			t.Fatal(err)
		}
	})
}

func TestSessionAttributes(t *testing.T) {
	t.Parallel()

	var sessionAttributes map[string]interface{}
	handler := func(ctx context.Context, query string) (PreparedStatements, error) {
		SetAttribute(ctx, "test_key", "test_value")
		SetAttribute(ctx, "numeric_key", 42)

		sess, ok := GetSession(ctx)
		if ok {
			sessionAttributes = sess.Attributes
		}

		statement := NewStatement(func(ctx context.Context, writer DataWriter, parameters []Parameter) error {
			return writer.Complete("OK")
		})

		return Prepared(statement), nil
	}

	server, err := NewServer(handler, Logger(slogt.New(t)))
	if err != nil {
		t.Fatal(err)
	}

	address := TListenAndServe(t, server)

	connstr := fmt.Sprintf("host=%s port=%d sslmode=disable", address.IP, address.Port)
	conn, err := sql.Open("postgres", connstr)
	if err != nil {
		t.Fatal(err)
	}

	_, err = conn.Exec("SELECT 1")
	if err != nil {
		t.Fatal(err)
	}

	err = conn.Close()
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, "test_value", sessionAttributes["test_key"])
	assert.Equal(t, 42, sessionAttributes["numeric_key"])

	ctx := context.Background()
	sess := &Session{Attributes: map[string]interface{}{"foo": "bar"}}
	ctx = context.WithValue(ctx, sessionKey, sess)

	val, ok := GetAttribute(ctx, "foo")
	assert.True(t, ok)
	assert.Equal(t, "bar", val)

	val, ok = GetAttribute(ctx, "non_existent")
	assert.False(t, ok)
	assert.Nil(t, val)

	ok = SetAttribute(ctx, "new_key", "new_value")
	assert.True(t, ok)
	assert.Equal(t, "new_value", sess.Attributes["new_key"])
}

func TestServerCopyIn(t *testing.T) {
	t.Parallel()

	handler := func(ctx context.Context, query string) (PreparedStatements, error) {
		t.Log("preparing query", query)

		handle := func(ctx context.Context, writer DataWriter, parameters []Parameter) error {
			t.Log("copying data")

			c, err := writer.CopyIn(BinaryFormat)
			if err != nil {
				return err
			}

			b, err := NewBinaryColumnReader(ctx, c)
			if err != nil {
				return err
			}

			for {
				rows, err := b.Read(ctx)
				if err == io.EOF {
					break
				}

				if err != nil {
					return err
				}

				t.Logf("received columns: %+v", rows)
			}

			return writer.Complete("COPY 2")
		}

		columns := Columns{
			{
				Table: 0,
				Name:  "id",
				Oid:   oid.T_int4,
				Width: 1,
			},
			{
				Table: 0,
				Name:  "name",
				Oid:   oid.T_text,
				Width: 256,
			},
			{
				Table: 0,
				Name:  "spotify_id",
				Oid:   oid.T_text,
				Width: 256,
			},
		}

		return Prepared(NewStatement(handle, WithColumns(columns))), nil
	}

	server, err := NewServer(handler, Logger(slogt.New(t)))
	if err != nil {
		t.Fatal(err)
	}

	address := TListenAndServe(t, server)

	rows := [][]any{
		{196, "My Posse In Effect", nil},
		{181, "Almost KISS", "10"},
	}

	t.Run("lib/pq", func(t *testing.T) {
		t.Skip()
		connstr := fmt.Sprintf("host=%s port=%d sslmode=disable", address.IP, address.Port)
		conn, err := sql.Open("postgres", connstr)
		if err != nil {
			t.Fatal(err)
		}

		txn, err := conn.Begin()
		if err != nil {
			t.Fatal(err)
		}

		stmt, err := txn.Prepare(pq.CopyIn("id", "name", "spotify_id"))
		if err != nil {
			t.Fatal(err)
		}

		for _, row := range rows {
			_, err := stmt.Exec(row...)
			if err != nil {
				t.Fatal(err)
			}
		}
		if err != nil {
			t.Fatal(err)
		}

		if err := stmt.Close(); err != nil {
			t.Fatal(err)
		}
		if err := txn.Commit(); err != nil {
			t.Fatal(err)
		}

		if err := conn.Close(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("jackc/pgx", func(t *testing.T) {
		ctx := context.Background()
		connstr := fmt.Sprintf("postgres://%s:%d", address.IP, address.Port)
		conn, err := pgx.Connect(ctx, connstr)
		if err != nil {
			t.Fatal(err)
		}

		n, err := conn.CopyFrom(ctx, pgx.Identifier{"foo"}, []string{"id", "name", "spotify_id"}, pgx.CopyFromRows(rows))
		if err != nil {
			t.Fatal(err)
		}
		if n != 2 {
			t.Fatalf("unexpected number of rows copied: %d", n)
		}

		err = conn.Close(ctx)
		if err != nil {
			t.Fatal(err)
		}
	})
}

func TestServerShutdownWithContextDeadline(t *testing.T) {
	t.Parallel()

	handler := func(ctx context.Context, query string) (PreparedStatements, error) {
		statement := NewStatement(func(ctx context.Context, writer DataWriter, parameters []Parameter) error {
			return writer.Complete("OK")
		})
		return Prepared(statement), nil
	}

	// Create server with a longer shutdown timeout
	server, err := NewServer(handler,
		WithShutdownTimeout(5*time.Second),
		Logger(slogt.New(t)))
	require.NoError(t, err)

	_ = TListenAndServeWithoutCleanup(t, server)

	// Test shutdown with shorter context deadline (should be used)
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	start := time.Now()
	err = server.Shutdown(ctx)
	duration := time.Since(start)

	// Should complete quickly since no active connections
	assert.NoError(t, err)
	assert.Less(t, duration, 300*time.Millisecond)
}

func TestServerShutdownWithServerTimeout(t *testing.T) {
	t.Parallel()

	handler := func(ctx context.Context, query string) (PreparedStatements, error) {
		statement := NewStatement(func(ctx context.Context, writer DataWriter, parameters []Parameter) error {
			return writer.Complete("OK")
		})
		return Prepared(statement), nil
	}

	// Create server with custom shutdown timeout
	server, err := NewServer(handler,
		WithShutdownTimeout(100*time.Millisecond),
		Logger(slogt.New(t)))
	require.NoError(t, err)

	_ = TListenAndServeWithoutCleanup(t, server)

	// Test shutdown uses server timeout regardless of context
	ctx := context.Background()
	start := time.Now()
	err = server.Shutdown(ctx)
	duration := time.Since(start)

	assert.NoError(t, err)
	assert.Less(t, duration, 200*time.Millisecond)
}

func TestServerShutdownUsesShortestTimeout(t *testing.T) {
	t.Parallel()

	handler := func(ctx context.Context, query string) (PreparedStatements, error) {
		statement := NewStatement(func(ctx context.Context, writer DataWriter, parameters []Parameter) error {
			return writer.Complete("OK")
		})
		return Prepared(statement), nil
	}

	// Create server with longer timeout
	server, err := NewServer(handler,
		WithShutdownTimeout(1*time.Second),
		Logger(slogt.New(t)))
	require.NoError(t, err)

	_ = TListenAndServeWithoutCleanup(t, server)

	// Test with shorter context deadline - should be used instead of server timeout
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	start := time.Now()
	err = server.Shutdown(ctx)
	duration := time.Since(start)

	// Should complete quickly since no active connections
	// (would take longer if server timeout was used)
	assert.NoError(t, err)
	assert.Less(t, duration, 200*time.Millisecond)
}

func TestServerShutdownShorterTimeoutWins(t *testing.T) {
	t.Parallel()

	var queryStarted sync.WaitGroup
	var queryCanFinish sync.WaitGroup
	queryStarted.Add(1)
	queryCanFinish.Add(1)

	handler := func(ctx context.Context, query string) (PreparedStatements, error) {
		statement := NewStatement(func(ctx context.Context, writer DataWriter, parameters []Parameter) error {
			queryStarted.Done()
			queryCanFinish.Wait() // Block until test releases it
			return writer.Complete("OK")
		})
		return Prepared(statement), nil
	}

	// Create server with longer timeout (1 second)
	server, err := NewServer(handler,
		WithShutdownTimeout(1*time.Second),
		Logger(slogt.New(t)))
	require.NoError(t, err)

	addr := TListenAndServeWithoutCleanup(t, server)

	// Start a long-running query
	go func() {
		conn, err := sql.Open("postgres", fmt.Sprintf("postgres://username:password@%s/database?sslmode=disable", addr))
		if err != nil {
			return
		}
		defer conn.Close() //nolint:errcheck
		_, _ = conn.Exec("SELECT 1")
	}()

	// Wait for query to start
	queryStarted.Wait()

	// Test with shorter context deadline - should win over server timeout
	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()

	start := time.Now()
	err = server.Shutdown(ctx)
	duration := time.Since(start)

	// Should timeout after ~80ms (context deadline), not 1s (server timeout)
	assert.Error(t, err)
	assert.Equal(t, context.DeadlineExceeded, err)
	assert.GreaterOrEqual(t, duration, 80*time.Millisecond)
	assert.Less(t, duration, 200*time.Millisecond)

	// Cleanup
	queryCanFinish.Done()
	time.Sleep(10 * time.Millisecond)
	server.Close() //nolint:errcheck
}

func TestServerShutdownServerTimeoutWins(t *testing.T) {
	t.Parallel()

	var queryStarted sync.WaitGroup
	var queryCanFinish sync.WaitGroup
	queryStarted.Add(1)
	queryCanFinish.Add(1)

	handler := func(ctx context.Context, query string) (PreparedStatements, error) {
		statement := NewStatement(func(ctx context.Context, writer DataWriter, parameters []Parameter) error {
			queryStarted.Done()
			queryCanFinish.Wait() // Block until test releases it
			return writer.Complete("OK")
		})
		return Prepared(statement), nil
	}

	// Create server with shorter timeout (80ms)
	server, err := NewServer(handler,
		WithShutdownTimeout(80*time.Millisecond),
		Logger(slogt.New(t)))
	require.NoError(t, err)

	addr := TListenAndServeWithoutCleanup(t, server)

	// Start a long-running query
	go func() {
		conn, err := sql.Open("postgres", fmt.Sprintf("postgres://username:password@%s/database?sslmode=disable", addr))
		if err != nil {
			return
		}
		defer conn.Close() //nolint:errcheck
		_, _ = conn.Exec("SELECT 1")
	}()

	// Wait for query to start
	queryStarted.Wait()

	// Test with longer context deadline - server timeout should win
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	start := time.Now()
	err = server.Shutdown(ctx)
	duration := time.Since(start)

	// Should timeout after ~80ms (server timeout), not 1s (context deadline)
	assert.Error(t, err)
	assert.Equal(t, context.DeadlineExceeded, err)
	assert.GreaterOrEqual(t, duration, 80*time.Millisecond)
	assert.Less(t, duration, 200*time.Millisecond)

	// Cleanup
	queryCanFinish.Done()
	time.Sleep(10 * time.Millisecond)
	server.Close() //nolint:errcheck
}

func TestServerShutdownTimeout(t *testing.T) {
	t.Parallel()

	var queryStarted sync.WaitGroup
	var queryCanFinish sync.WaitGroup
	queryStarted.Add(1)
	queryCanFinish.Add(1)

	handler := func(ctx context.Context, query string) (PreparedStatements, error) {
		statement := NewStatement(func(ctx context.Context, writer DataWriter, parameters []Parameter) error {
			queryStarted.Done()
			queryCanFinish.Wait() // Block until test releases it
			return writer.Complete("OK")
		})
		return Prepared(statement), nil
	}

	// Create server with very short shutdown timeout
	server, err := NewServer(handler,
		WithShutdownTimeout(50*time.Millisecond),
		Logger(slogt.New(t)))
	require.NoError(t, err)

	addr := TListenAndServeWithoutCleanup(t, server)

	// Start a long-running query in background
	go func() {
		conn, err := sql.Open("postgres", fmt.Sprintf("postgres://username:password@%s/database?sslmode=disable", addr))
		if err != nil {
			return
		}
		defer conn.Close() //nolint:errcheck
		_, _ = conn.Exec("SELECT 1")
	}()

	// Wait for query to start
	queryStarted.Wait()

	// Test shutdown will timeout because query is still running
	ctx := context.Background()
	start := time.Now()
	err = server.Shutdown(ctx)
	duration := time.Since(start)

	// Should timeout because query is still running
	assert.Error(t, err)
	assert.Equal(t, context.DeadlineExceeded, err)
	assert.GreaterOrEqual(t, duration, 50*time.Millisecond)
	assert.Less(t, duration, 150*time.Millisecond)

	// Allow query to finish and cleanup
	queryCanFinish.Done()
	time.Sleep(10 * time.Millisecond)
	server.Close() //nolint:errcheck
}

func TestServerShutdownConcurrent(t *testing.T) {
	t.Parallel()

	handler := func(ctx context.Context, query string) (PreparedStatements, error) {
		statement := NewStatement(func(ctx context.Context, writer DataWriter, parameters []Parameter) error {
			return writer.Complete("OK")
		})
		return Prepared(statement), nil
	}

	server, err := NewServer(handler, Logger(slogt.New(t)))
	require.NoError(t, err)

	TListenAndServeWithoutCleanup(t, server)

	// Test concurrent shutdown calls
	var wg sync.WaitGroup
	errors := make([]error, 3)

	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()
			errors[index] = server.Shutdown(ctx)
		}(i)
	}

	wg.Wait()

	// All shutdown calls should succeed (first one does the work, others are no-ops)
	for i, err := range errors {
		assert.NoError(t, err, "Shutdown call %d should not error", i)
	}
}

func TestWithShutdownTimeoutOption(t *testing.T) {
	t.Parallel()

	handler := func(ctx context.Context, query string) (PreparedStatements, error) {
		statement := NewStatement(func(ctx context.Context, writer DataWriter, parameters []Parameter) error {
			return writer.Complete("OK")
		})
		return Prepared(statement), nil
	}

	// Test custom timeout
	customTimeout := 5 * time.Second
	server, err := NewServer(handler, WithShutdownTimeout(customTimeout))
	require.NoError(t, err)
	assert.Equal(t, customTimeout, server.ShutdownTimeout)

	// Test default timeout (1 second)
	serverDefault, err := NewServer(handler)
	require.NoError(t, err)
	assert.Equal(t, 1*time.Second, serverDefault.ShutdownTimeout)
}

func TestServerShutdownInfiniteTimeout(t *testing.T) {
	t.Parallel()

	handler := func(ctx context.Context, query string) (PreparedStatements, error) {
		statement := NewStatement(func(ctx context.Context, writer DataWriter, parameters []Parameter) error {
			return writer.Complete("OK")
		})
		return Prepared(statement), nil
	}

	// Create server and set ShutdownTimeout to 0 to test infinite timeout
	server, err := NewServer(handler, Logger(slogt.New(t)))
	require.NoError(t, err)
	server.ShutdownTimeout = 0 // Zero timeout means wait indefinitely

	TListenAndServeWithoutCleanup(t, server)

	// Test shutdown without context deadline (should wait indefinitely but complete quickly with no active connections)
	ctx := context.Background()
	start := time.Now()
	err = server.Shutdown(ctx)
	duration := time.Since(start)

	assert.NoError(t, err)
	assert.Less(t, duration, 100*time.Millisecond) // Should complete quickly with no active connections
}

func TestServerShutdownInfiniteTimeoutWithActiveConnection(t *testing.T) {
	t.Parallel()

	var queryStarted sync.WaitGroup
	var queryCanFinish sync.WaitGroup
	queryStarted.Add(1)
	queryCanFinish.Add(1)

	handler := func(ctx context.Context, query string) (PreparedStatements, error) {
		statement := NewStatement(func(ctx context.Context, writer DataWriter, parameters []Parameter) error {
			queryStarted.Done()
			queryCanFinish.Wait() // Block until test releases it
			return writer.Complete("OK")
		})
		return Prepared(statement), nil
	}

	// Create server with zero timeout (infinite wait)
	server, err := NewServer(handler, Logger(slogt.New(t)))
	require.NoError(t, err)
	server.ShutdownTimeout = 0 // Zero timeout means wait indefinitely

	addr := TListenAndServeWithoutCleanup(t, server)

	// Start a long-running query in background
	go func() {
		conn, err := sql.Open("postgres", fmt.Sprintf("postgres://username:password@%s/database?sslmode=disable", addr))
		if err != nil {
			return
		}
		defer conn.Close() //nolint:errcheck
		_, _ = conn.Exec("SELECT 1")
	}()

	// Wait for query to start
	queryStarted.Wait()

	// Start shutdown in background (should wait indefinitely)
	shutdownDone := make(chan error, 1)
	go func() {
		ctx := context.Background() // No deadline, uses server timeout (0 = infinite)
		shutdownDone <- server.Shutdown(ctx)
	}()

	// Verify shutdown doesn't complete immediately
	select {
	case <-shutdownDone:
		t.Error("Shutdown completed too quickly - should wait for active connection")
	case <-time.After(100 * time.Millisecond):
		// Good - shutdown is waiting
	}

	// Allow query to finish
	queryCanFinish.Done()

	// Now shutdown should complete
	select {
	case err := <-shutdownDone:
		assert.NoError(t, err)
	case <-time.After(200 * time.Millisecond):
		t.Error("Shutdown took too long after query finished")
	}
}

// testLogHandler captures log messages for testing
type testLogHandler struct {
	errorLogs *[]string
	debugLogs *[]string
	mu        *sync.Mutex
}

func (h *testLogHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return true
}

func (h *testLogHandler) Handle(ctx context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	if r.Level >= slog.LevelError {
		*h.errorLogs = append(*h.errorLogs, r.Message)
	} else if r.Level == slog.LevelDebug {
		*h.debugLogs = append(*h.debugLogs, r.Message)
	}
	return nil
}

func (h *testLogHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return h
}

func (h *testLogHandler) WithGroup(name string) slog.Handler {
	return h
}

// TestClientDisconnectDuringWrite tests that the server handles client disconnections gracefully
// without logging them as unexpected errors when they occur during result writing.
func TestClientDisconnectDuringWrite(t *testing.T) {
	t.Parallel()

	// Capture logs
	var errorLogs []string
	var debugLogs []string
	var mu sync.Mutex

	// WaitGroup to synchronize on broken pipe error
	var brokenPipeWG sync.WaitGroup
	brokenPipeWG.Add(1)

	handler := func(ctx context.Context, query string) (PreparedStatements, error) {
		// Handler that writes multiple rows to trigger broken pipe when client disconnects
		statement := NewStatement(func(ctx context.Context, writer DataWriter, parameters []Parameter) error {
			// Write enough data to fill buffers and ensure we hit the broken pipe
			for i := 0; i < 1000; i++ {
				if err := writer.Row([]any{fmt.Sprintf("Row %d with some data to fill buffers", i)}); err != nil {
					// Signal that we've hit the broken pipe error
					brokenPipeWG.Done()
					return err // This will be a broken pipe error after client disconnects
				}
			}
			return writer.Complete("SELECT 1000")
		}, WithColumns(Columns{{Name: "data", Oid: oid.T_text}}))
		return Prepared(statement), nil
	}

	// Create custom logger that captures ERROR and DEBUG logs
	logHandler := &testLogHandler{
		errorLogs: &errorLogs,
		debugLogs: &debugLogs,
		mu:        &mu,
	}

	server, err := NewServer(handler, Logger(slog.New(logHandler)))
	require.NoError(t, err)
	address := TListenAndServe(t, server)

	// Connect and send query
	conn, err := net.Dial("tcp", address.String())
	require.NoError(t, err)

	client := mock.NewClient(t, conn)
	client.Handshake(t)
	client.Authenticate(t)
	client.ReadyForQuery(t)

	// Send query
	client.Start(types.ClientSimpleQuery)
	client.AddString("SELECT 1")
	client.AddNullTerminate()
	err = client.End()
	require.NoError(t, err)

	// Close connection immediately to cause broken pipe
	err = conn.Close()
	require.NoError(t, err)

	// Wait for the handler to encounter the broken pipe error
	done := make(chan struct{})
	go func() {
		brokenPipeWG.Wait()
		close(done)
	}()

	select {
	case <-done:
		// Handler encountered the broken pipe error as expected
	case <-time.After(2 * time.Second):
		t.Error("Timeout waiting for broken pipe error in handler")
		return
	}

	// Check logs
	mu.Lock()
	defer mu.Unlock()

	// The test fails if we see the error log about unexpected connection error
	for _, log := range errorLogs {
		if log == "an unexpected error got returned while serving a client connection" {
			t.Errorf("Server logged broken pipe as unexpected error - the fix is not working")
		}
	}
}

// TListenAndServeWithoutCleanup is like TListenAndServe but doesn't register cleanup.
// Used for tests that need manual control over server shutdown.
func TListenAndServeWithoutCleanup(t *testing.T, server *Server) *net.TCPAddr {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	go server.Serve(listener) //nolint:errcheck
	return listener.Addr().(*net.TCPAddr)
}
