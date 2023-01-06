package main

import (
	"context"
	"errors"
	"log"

	wire "github.com/jeroenrinzema/psql-wire"
	"github.com/jeroenrinzema/psql-wire/codes"
	psqlerr "github.com/jeroenrinzema/psql-wire/errors"
)

func main() {
	log.Println("PostgreSQL server is up and running at [127.0.0.1:5432]")
	wire.ListenAndServe("127.0.0.1:5432", handle)
}

func handle(ctx context.Context, query string, writer wire.DataWriter, parameters []string) error {
	log.Println("incoming SQL query:", query)

	err := errors.New("unimplemented feature")
	err = psqlerr.WithCode(err, codes.FeatureNotSupported)
	err = psqlerr.WithSeverity(err, psqlerr.LevelFatal)
	return err
}
