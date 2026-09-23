package main

import (
	"context"
	"log"
	"os"

	"github.com/jackc/pgx/v5/pgtype"
	wire "github.com/jeroenrinzema/psql-wire"
)

// PostgreServer represents a PostgreSQL server with authentication.
type PostgreServer struct {
	server      *wire.Server
	logger      *log.Logger
	credentials map[string]string
}

// clientPasswords are converted to SCRAM verifiers when the example starts.
// Applications should persist verifiers instead of plaintext passwords.
var clientPasswords = map[string]string{
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

// NewPostgreServer creates a new PostgreSQL server with authentication.
func NewPostgreServer(logger *log.Logger) (*PostgreServer, error) {
	credentials := make(map[string]string, len(clientPasswords))
	for username, password := range clientPasswords {
		verifier, err := wire.NewSCRAMSHA256Verifier(password)
		if err != nil {
			return nil, err
		}
		credentials[username] = verifier
	}

	server := &PostgreServer{
		logger:      logger,
		credentials: credentials,
	}

	wireServer, err := wire.NewServer(
		server.wireHandler,
		wire.SessionAuthStrategy(wire.SCRAMSHA256(server.authenticate)),
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

// authenticate returns the user's stored SCRAM verifier.
func (s *PostgreServer) authenticate(ctx context.Context, database, username string) (context.Context, string, bool, error) {
	credential, found := s.credentials[username]
	return ctx, credential, found, nil
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
func (s *PostgreServer) wireHandler(ctx context.Context, query wire.Query) (wire.PreparedStatements, error) {
	s.logger.Printf("incoming SQL query: %s", query.Query)

	handle := func(ctx context.Context, writer wire.DataWriter, parameters []wire.Parameter) error {
		writer.Row([]any{"John", true, 29})   //nolint:errcheck
		writer.Row([]any{"Marry", false, 21}) //nolint:errcheck
		return writer.Complete("SELECT 2")
	}

	return wire.Prepared(wire.NewStatement(handle, wire.WithColumns(table))), nil
}
