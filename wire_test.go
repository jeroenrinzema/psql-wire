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
