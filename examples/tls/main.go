package main

import (
	"context"
	"crypto/tls"
	"log"

	wire "github.com/jeroenrinzema/psql-wire"
)

func main() {
	err := run()
	if err != nil {
		log.Fatal(err)
	}
}

func run() error {
	cert, err := tls.LoadX509KeyPair("./psql.crt", "./psql.key")
	if err != nil {
		return err
	}

	server, err := wire.NewServer("127.0.0.1:5432", wire.SimpleQuery(handle))
	if err != nil {
		return err
	}

	log.Println("PostgreSQL server is up and running at [127.0.0.1:5432]")
	server.Certificates = []tls.Certificate{cert}
	return server.ListenAndServe()
}

func handle(ctx context.Context, query string, writer wire.DataWriter) error {
	return writer.Complete("OK")
}
