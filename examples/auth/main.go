package main

import (
	"context"
	"log"
	"os"

	"github.com/jackc/pgx/v5/pgtype"
	wire "github.com/jeroenrinzema/psql-wire"
)

// PostgreServer represents a PostgreSQL server with authentication
type PostgreServer struct {
	server *wire.Server
	logger *log.Logger
}

// Client credentials map for authentication
var clientCredentials = map[string]string{
	"postgres": "password",
	"admin":    "secret",
}

func main() {
	logger := log.New(os.Stdout, "[psql-wire] ", log.LstdFlags)
	server, err := NewPostgreServer(logger)
	if err != nil {
		logger.Fatalf("failed to create server: %s", err)
	}

	logger.Println("PostgreSQL server is running at [127.0.0.1:5432]")
	logger.Println("You can connect using: psql -h 127.0.0.1 -p 5432 -U postgres -W")
	logger.Println("Available users: postgres (password: password), admin (password: secret)")

	err = server.server.ListenAndServe("127.0.0.1:5432")
	if err != nil {
		logger.Fatalf("failed to start server: %s", err)
	}
}

// NewPostgreServer creates a new PostgreSQL server with authentication
func NewPostgreServer(logger *log.Logger) (*PostgreServer, error) {
	server := &PostgreServer{
		logger: logger,
	}

	wireServer, err := wire.NewServer(
		server.wireHandler,
		wire.SessionAuthStrategy(wire.ClearTextPassword(server.auth)),
		wire.SessionMiddleware(server.session),
		wire.TerminateConn(server.terminateConn),
		wire.Version("17.0"),
	)
	if err != nil {
		return nil, err
	}
	server.server = wireServer
	return server, nil
}

// auth handles authentication of incoming connections
func (s *PostgreServer) auth(ctx context.Context, database, username, password string) (context.Context, bool, error) {
	if expected, ok := clientCredentials[username]; !ok {
		s.logger.Printf("invalid username: %s", username)
		return ctx, false, nil
	} else if password != expected {
		s.logger.Printf("invalid password for user: %s", username)
		return ctx, false, nil
	}
	s.logger.Printf("successful authentication for user: %s", username)
	return ctx, true, nil
}

// session middleware for handling session context
func (s *PostgreServer) session(ctx context.Context) (context.Context, error) {
	s.logger.Printf("new session established: %s", wire.RemoteAddress(ctx))
	return ctx, nil
}

// terminateConn handles connection termination
func (s *PostgreServer) terminateConn(ctx context.Context) error {
	s.logger.Printf("session terminated: %s", wire.RemoteAddress(ctx))
	return nil
}

var table = wire.Columns{
	{
		Table: 0,
		Name:  "name",
		Oid:   pgtype.TextOID,
		Width: 256,
	},
	{
		Table: 0,
		Name:  "member",
		Oid:   pgtype.BoolOID,
		Width: 1,
	},
	{
		Table: 0,
		Name:  "age",
		Oid:   pgtype.Int4OID,
		Width: 1,
	},
}

// wireHandler processes incoming SQL queries
func (s *PostgreServer) wireHandler(ctx context.Context, query string) (wire.PreparedStatements, error) {
	s.logger.Printf("incoming SQL query: %s", query)

	handle := func(ctx context.Context, writer wire.DataWriter, parameters []wire.Parameter) error {
		writer.Row([]any{"John", true, 29})   //nolint:errcheck
		writer.Row([]any{"Marry", false, 21}) //nolint:errcheck
		return writer.Complete("SELECT 2")
	}

	return wire.Prepared(wire.NewStatement(handle, wire.WithColumns(table))), nil
}
