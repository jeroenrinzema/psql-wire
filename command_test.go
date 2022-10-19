package wire

import (
	"context"
	"fmt"
	"net"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jeroenrinzema/psql-wire/internal/buffer"
	"github.com/jeroenrinzema/psql-wire/internal/mock"
	"github.com/jeroenrinzema/psql-wire/internal/types"
	"github.com/lib/pq/oid"
	"go.uber.org/zap"
)

func TestMessageSizeExceeded(t *testing.T) {
	server, err := NewServer()
	if err != nil {
		t.Fatal(err)
	}

	address := TListenAndServe(t, server)
	conn, err := net.Dial("tcp", address.String())
	if err != nil {
		t.Fatal(err)
	}

	client := mock.NewClient(conn)
	client.Handshake(t)
	client.Authenticate(t)
	client.ReadyForQuery(t)

	// NOTE(Jeroen): attempt to send a message twice the max buffer size
	size := uint32(buffer.DefaultBufferSize * 2)
	t.Logf("writing message of size: %d", size)

	client.Start(types.ClientSimpleQuery)
	client.AddBytes(make([]byte, size))
	err = client.End()
	if err != nil {
		t.Fatal(err)
	}

	client.Error(t)
	client.ReadyForQuery(t)
	client.Close(t)
}

func TestBindMessageParameters(t *testing.T) {
	t.Parallel()

	handler := func(ctx context.Context, query string, writer DataWriter) error {
		t.Log("serving query")

		writer.Define(Columns{ //nolint:errcheck
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

		writer.Row([]any{"John", true, 28})   //nolint:errcheck
		writer.Row([]any{"Marry", false, 21}) //nolint:errcheck
		return writer.Complete("OK")
	}

	d, _ := zap.NewDevelopment()
	server, err := NewServer(SimpleQuery(handler), Logger(d))
	if err != nil {
		t.Fatal(err)
	}

	address := TListenAndServe(t, server)

	ctx := context.Background()
	connstr := fmt.Sprintf("postgres://%s:%d", address.IP, address.Port)

	t.Run("pgx", func(t *testing.T) {
		conn, err := pgx.Connect(ctx, connstr)
		if err != nil {
			t.Fatal(err)
		}

		defer conn.Close(ctx)

		_, err = conn.Query(ctx, "SELECT $1 $2;", 10, "Jeroen")
		if err != nil {
			t.Fatal(err)
		}
	})

	// for rows.Next() {
	// 	var name string
	// 	var member bool
	// 	var age int

	// 	err := rows.Scan(&name, &member, &age)
	// 	if err != nil {
	// 		t.Fatal(err)
	// 	}

	// 	t.Logf("scan result: %s, %d, %t", name, age, member)
	// }

	// rows.Close()

	// err = conn.Close(ctx)
	// if err != nil {
	// 	t.Fatal(err)
	// }
}
