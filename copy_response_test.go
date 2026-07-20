package wire

import (
	"context"
	"fmt"
	"io"
	"net"
	"testing"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/neilotoole/slogt"
)

// TestCopyInSimpleQuerySuppressesRowDescription asserts the exact response
// sequence for a simple-query COPY ... FROM STDIN on a statement marked
// WithCopyIn: PostgreSQL answers such a query with a CopyInResponse and never
// sends a RowDescription for it. The assertion runs at the raw protocol level
// because tolerant clients (pgx) skip an unexpected RowDescription, while
// strict ones (lib/pq's prepareCopyIn) abandon the connection on it ("unknown
// response for copy query: 'T'").
func TestCopyInSimpleQuerySuppressesRowDescription(t *testing.T) {
	table := Columns{
		{
			Table: 0,
			Name:  "id",
			Oid:   pgtype.Int4OID,
			Width: 4,
		},
	}

	handler := func(ctx context.Context, query Query) (PreparedStatements, error) {
		handle := func(ctx context.Context, writer DataWriter, parameters []Parameter) error {
			reader, err := writer.CopyIn(TextFormat)
			if err != nil {
				return err
			}

			// Drain the (empty) COPY stream until the client's CopyDone.
			for {
				if err := reader.Read(ctx); err == io.EOF {
					break
				} else if err != nil {
					return err
				}
			}

			return writer.Complete("COPY 0")
		}

		return Prepared(NewStatement(handle, WithColumns(table), WithCopyIn())), nil
	}

	server, err := NewServer(handler, Logger(slogt.New(t)))
	if err != nil {
		t.Fatal(err)
	}

	address := TListenAndServe(t, server)

	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", address.IP, address.Port))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close() //nolint:errcheck

	frontend := pgproto3.NewFrontend(conn, conn)
	frontend.Send(&pgproto3.StartupMessage{
		ProtocolVersion: pgproto3.ProtocolVersionNumber,
		Parameters:      map[string]string{"user": "copy-tester"},
	})
	if err := frontend.Flush(); err != nil {
		t.Fatal(err)
	}

	// Consume the handshake up to the first ReadyForQuery.
	for {
		msg, err := frontend.Receive()
		if err != nil {
			t.Fatal(err)
		}
		if _, ok := msg.(*pgproto3.ReadyForQuery); ok {
			break
		}
	}

	frontend.Send(&pgproto3.Query{String: "COPY jedis FROM STDIN"})
	if err := frontend.Flush(); err != nil {
		t.Fatal(err)
	}

	// The first data-carrying response to the COPY query must be the
	// CopyInResponse itself.
	for {
		msg, err := frontend.Receive()
		if err != nil {
			t.Fatal(err)
		}

		switch typed := msg.(type) {
		case *pgproto3.CopyInResponse:
			if len(typed.ColumnFormatCodes) != len(table) {
				t.Fatalf("CopyInResponse advertises %d columns, expected %d", len(typed.ColumnFormatCodes), len(table))
			}
		case *pgproto3.RowDescription:
			t.Fatal("received a RowDescription in response to COPY ... FROM STDIN; PostgreSQL sends a CopyInResponse alone, and lib/pq aborts the connection on the stray message")
		case *pgproto3.ErrorResponse:
			t.Fatalf("unexpected error response: %s", typed.Message)
		default:
			t.Fatalf("unexpected message %T before the CopyInResponse", msg)
		}
		break
	}

	// Complete the (empty) copy operation and drain to ReadyForQuery so the
	// full exchange round-trips.
	frontend.Send(&pgproto3.CopyDone{})
	if err := frontend.Flush(); err != nil {
		t.Fatal(err)
	}

	sawComplete := false
	for {
		msg, err := frontend.Receive()
		if err != nil {
			t.Fatal(err)
		}

		switch typed := msg.(type) {
		case *pgproto3.CommandComplete:
			sawComplete = true
		case *pgproto3.ReadyForQuery:
			if !sawComplete {
				t.Fatal("ReadyForQuery arrived without a CommandComplete for the COPY")
			}
			return
		case *pgproto3.ErrorResponse:
			t.Fatalf("unexpected error response: %s", typed.Message)
		}
	}
}
