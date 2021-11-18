package wire

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"testing"

	"github.com/jackc/pgx/v4"
	"github.com/jeroenrinzema/psql-wire/internal/mock"
	_ "github.com/lib/pq"
	"github.com/lib/pq/oid"
)

// TListenAndServe will open a new TCP listener on a unallocated port inside
// the local network. The newly created listner is passed to the given server to
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

	pong := func(ctx context.Context, query string, writer DataWriter) error {
		return writer.Complete("OK")
	}

	server, err := NewServer(SimpleQuery(pong))
	if err != nil {
		t.Fatal(err)
	}

	address := TListenAndServe(t, server)

	t.Run("mock", func(t *testing.T) {
		conn, err := net.Dial("tcp", address.String())
		if err != nil {
			t.Fatal(err)
		}

		client := mock.NewClient(conn)
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

func TestServerWritingResult(t *testing.T) {
	t.Parallel()

	handler := func(ctx context.Context, query string, writer DataWriter) error {
		t.Log("serving query")

		writer.Define(Columns{
			{
				Table:  0,
				Name:   "name",
				Oid:    oid.T_text,
				Width:  256,
				Format: TextFormat,
			},
			{
				Table:  0,
				Name:   "member",
				Oid:    oid.T_bool,
				Width:  1,
				Format: TextFormat,
			},
			{
				Table:  0,
				Name:   "age",
				Oid:    oid.T_int4,
				Width:  1,
				Format: TextFormat,
			},
		})

		writer.Row([]interface{}{"John", true, 28})
		writer.Row([]interface{}{"Marry", false, 21})
		return writer.Complete("OK")
	}

	server, err := NewServer(SimpleQuery(handler))
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

	// NOTE(Jeroen): the jackc/pgx test has been disabled due to a lock of
	// support for parse, describe, and execute messages. This test could be
	// enabled again once these message types are supported.
	// t.Run("jackc/pgx", func(t *testing.T) {
	// 	ctx := context.Background()
	// 	connstr := fmt.Sprintf("postgres://%s:%d", address.IP, address.Port)
	// 	conn, err := pgx.Connect(ctx, connstr)
	// 	if err != nil {
	// 		t.Fatal(err)
	// 	}

	// 	rows, err := conn.Query(ctx, "SELECT *;")
	// 	if err != nil {
	// 		t.Fatal(err)
	// 	}

	// 	for rows.Next() {
	// 		var name string
	// 		var member bool
	// 		var age int

	// 		err := rows.Scan(&name, &member, &age)
	// 		if err != nil {
	// 			t.Fatal(err)
	// 		}

	// 		t.Logf("scan result: %s, %d, %t", name, age, member)
	// 	}

	// 	err = conn.Close(ctx)
	// 	if err != nil {
	// 		t.Fatal(err)
	// 	}
	// })
}
