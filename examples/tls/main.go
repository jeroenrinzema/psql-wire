package main

import (
	"context"
	"crypto/tls"

	wire "github.com/jeroenrinzema/psql-wire"
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

	server, err := wire.NewServer("127.0.0.1:5432", wire.SimpleQuery(handle), wire.Logger(logger))
	if err != nil {
		return err
	}

	logger.Info("PostgreSQL server is up and running at [127.0.0.1:5432]")
	server.Certificates = []tls.Certificate{cert}
	return server.ListenAndServe()
}

func handle(ctx context.Context, query string, writer wire.DataWriter) error {
	return writer.Complete("OK")
}
