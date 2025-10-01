package main

import (
	"context"
	"fmt"
	"io"
	"log"

	wire "github.com/jeroenrinzema/psql-wire"
	"github.com/lib/pq/oid"
)

func main() {
	log.Println("PostgreSQL server is up and running at [127.0.0.1:5432]")
	wire.ListenAndServe("127.0.0.1:5432", handler) //nolint:errcheck
}

var table = wire.Columns{
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

func handler(ctx context.Context, query string) (wire.PreparedStatements, error) {
	log.Println("incoming SQL query:", query)

	handle := func(ctx context.Context, writer wire.DataWriter, parameters []wire.Parameter) error {
		log.Println("copying data")

		copy, err := writer.CopyIn(wire.BinaryFormat)
		if err != nil {
			return err
		}

		var length int
		reader, err := wire.NewBinaryColumnReader(ctx, copy)
		if err != nil {
			return err
		}

		for {
			columns, err := reader.Read(ctx)
			if err == io.EOF {
				break
			}

			if err != nil {
				return err
			}

			log.Printf("received columns: %+v", columns)
			length++
		}

		return writer.Complete(fmt.Sprintf("COPY %d", length))
	}

	return wire.Prepared(wire.NewStatement(handle, wire.WithColumns(table))), nil
}
