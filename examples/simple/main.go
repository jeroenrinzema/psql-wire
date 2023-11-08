package main

import (
	"context"
	"log"

	wire "github.com/jeroenrinzema/psql-wire"
	"github.com/lib/pq/oid"
)

func main() {
	log.Println("PostgreSQL server is up and running at [127.0.0.1:5432]")
	wire.ListenAndServe("127.0.0.1:5432", handler)
}

var table = wire.Columns{
	{
		Table:  0,
		Name:   "name",
		Oid:    oid.T_text,
		Width:  256,
		Format: wire.TextFormat,
	},
	{
		Table:  0,
		Name:   "member",
		Oid:    oid.T_bool,
		Width:  1,
		Format: wire.TextFormat,
	},
	{
		Table:  0,
		Name:   "age",
		Oid:    oid.T_int4,
		Width:  1,
		Format: wire.TextFormat,
	},
}

func handler(ctx context.Context, query string) (*wire.PreparedStatement, error) {
	log.Println("incoming SQL query:", query)

	statement := wire.NewPreparedStatement(func(ctx context.Context, writer wire.DataWriter, parameters []wire.Parameter) error {
		writer.Row([]any{"John", true, 29})
		writer.Row([]any{"Marry", false, 21})
		return writer.Complete("SELECT 2")
	})

	statement.WithColumns(table)
	return statement, nil
}
