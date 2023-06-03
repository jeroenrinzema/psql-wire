package main

import (
	"context"
	"log"

	"github.com/jackc/pgtype"
	shopspring "github.com/jackc/pgtype/ext/shopspring-numeric"
	wire "github.com/jeroenrinzema/psql-wire"
	"github.com/lib/pq/oid"
	"github.com/shopspring/decimal"
)

func main() {
	types := wire.ExtendTypes(func(info *pgtype.ConnInfo) {
		info.RegisterDataType(pgtype.DataType{
			Value: &shopspring.Numeric{},
			Name:  "numeric",
			OID:   pgtype.NumericOID,
		})
	})

	srv, err := wire.NewServer(handler, types)
	if err != nil {
		panic(err)
	}

	log.Println("PostgreSQL server is up and running at [127.0.0.1:5432]")
	srv.ListenAndServe("127.0.0.1:5432")
}

var table = wire.Columns{
	{
		Table:  0,
		Name:   "account_balance",
		Oid:    oid.T_numeric,
		Width:  1,
		Format: wire.TextFormat,
	},
}

func handler(ctx context.Context, query string) (wire.PreparedStatementFn, []oid.Oid, wire.Columns, error) {
	log.Println("incoming SQL query:", query)

	statement := func(ctx context.Context, writer wire.DataWriter, parameters []string) error {
		balance, err := decimal.NewFromString("256.23")
		if err != nil {
			return err
		}

		writer.Row([]any{balance})
		return writer.Complete("SELECT 1")
	}

	return statement, wire.ParseParameters(query), table, nil
}
