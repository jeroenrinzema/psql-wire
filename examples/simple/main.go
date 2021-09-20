package main

import (
	"context"
	"log"

	wire "github.com/jeroenrinzema/psql-wire"
)

func main() {
	log.Println("PostgreSQL server is up and running at [127.0.0.1:5432]")
	wire.ListenAndServe("127.0.0.1:5432", handle)
}

func handle(ctx context.Context, query string, writer wire.DataWriter) error {
	log.Println(query)
	return writer.Complete("OK")
}
