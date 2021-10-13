package main

import (
	"context"
	"log"

	wire "github.com/jeroenrinzema/psql-wire"
	"github.com/lib/pq/oid"
)

func main() {
	log.Println("PostgreSQL server is up and running at [127.0.0.1:5432]")
	wire.ListenAndServe("127.0.0.1:5432", handle)
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

func handle(ctx context.Context, query string, writer wire.DataWriter) error {
	log.Println("incoming SQL query:", query)

	writer.Define(table)
	writer.Row([]interface{}{"John", true, 29})
	writer.Row([]interface{}{"Marry", false, 21})
	return writer.Complete("OK")
}
