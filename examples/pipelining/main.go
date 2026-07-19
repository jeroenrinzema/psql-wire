package main

// This example demonstrates parallel pipelining with a test client.
//
// Run the server:    go run main.go
// Run the client:    go run ./client
//
// The client sends 3 batched queries. With parallel pipelining enabled,
// the server executes them concurrently (~100ms total instead of ~300ms).

import (
	"context"
	"log"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	wire "github.com/jeroenrinzema/psql-wire"
)

func main() {
	server, err := wire.NewServer(handler,
		wire.ParallelPipeline(wire.ParallelPipelineConfig{Enabled: true}),
	)
	if err != nil {
		log.Fatal(err)
	}

	log.Println("PostgreSQL server is up and running at [127.0.0.1:5432]")
	log.Println("Parallel pipelining enabled")

	server.ListenAndServe("127.0.0.1:5432") //nolint:errcheck
}

var table = wire.Columns{
	{
		Table: 0,
		Name:  "result",
		Oid:   pgtype.Int4OID,
		Width: 1,
	},
}

func handler(ctx context.Context, query wire.Query) (wire.PreparedStatements, error) {
	log.Println("incoming SQL query:", query.Query)

	handle := func(ctx context.Context, writer wire.DataWriter, parameters []wire.Parameter) error {
		// Simulate work (database lookup, network call, etc.)
		time.Sleep(100 * time.Millisecond)

		writer.Row([]any{1}) //nolint:errcheck
		return writer.Complete("SELECT 1")
	}

	return wire.Prepared(wire.NewStatement(handle, wire.WithColumns(table))), nil
}
