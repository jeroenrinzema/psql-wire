package main

import (
	"context"
	"fmt"
	"log"
	"sync"

	wire "github.com/jeroenrinzema/psql-wire"
)

func main() {
	srv, err := wire.NewServer(wire.Session(session), wire.SimpleQuery(handle))
	if err != nil {
		panic(err)
	}

	log.Println("PostgreSQL server is up and running at [127.0.0.1:5432]")
	srv.ListenAndServe("127.0.0.1:5432")
}

type key int

var mu sync.Mutex
var id = key(1)
var counter = 0

func session(ctx context.Context) (context.Context, error) {
	mu.Lock()
	counter++
	defer mu.Unlock()
	return context.WithValue(ctx, id, counter), nil
}

func handle(ctx context.Context, query string, writer wire.DataWriter, parameters []string) error {
	session := ctx.Value(id).(int)
	return writer.Complete(fmt.Sprintf("OK, session: %d", session))
}
