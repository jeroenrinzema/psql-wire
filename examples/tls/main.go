package main

import (
	"context"
	"crypto/tls"

	"log/slog"

	wire "github.com/jeroenrinzema/psql-wire"
)

func main() {
	err := run()
	if err != nil {
		panic(err)
	}
}

func run() error {
	logger := slog.Default()
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

func handler(ctx context.Context, query string) (*wire.PreparedStatement, error) {
	slog.Info("incoming SQL query", slog.String("query", query))

	statement := wire.NewPreparedStatement(func(ctx context.Context, writer wire.DataWriter, parameters []wire.Parameter) error {
		return writer.Complete("OK")
	})

	return statement, nil
}
