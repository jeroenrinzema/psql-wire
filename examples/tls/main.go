package main

import (
	"context"
	"crypto/tls"
	"log"

	wire "github.com/jeroenrinzema/psql-wire"
	"github.com/lib/pq/oid"
	"go.uber.org/zap"
)

func main() {
	err := run()
	if err != nil {
		panic(err)
	}
}

func run() error {
	logger, err := zap.NewDevelopment()
	if err != nil {
		return err
	}

	cert, err := tls.LoadX509KeyPair("./psql.crt", "./psql.key")
	if err != nil {
		return err
	}

	certs := []tls.Certificate{cert}
	server, err := wire.NewServer(handler, wire.Certificates(certs), wire.Logger(logger), wire.MessageBufferSize(100))
	if err != nil {
		return err
	}

	logger.Info("PostgreSQL server is up and running at [127.0.0.1:5432]")
	return server.ListenAndServe("127.0.0.1:5432")
}

func handler(ctx context.Context, query string) (wire.PreparedStatementFn, []oid.Oid, wire.Columns, error) {
	log.Println("incoming SQL query:", query)

	statement := func(ctx context.Context, writer wire.DataWriter, parameters []string) error {
		return writer.Complete("OK")
	}

	return statement, wire.ParseParameters(query), nil, nil
}
