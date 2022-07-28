package wire

import (
	"context"
	"crypto/tls"
	"crypto/x509"

	"github.com/jackc/pgtype"
	"go.uber.org/zap"
)

type SimpleQueryFn func(ctx context.Context, query string, writer DataWriter) error

// ParseFn parses the given query and returns a prepared statement which could
// be used to execute at a later point in time.
type ParseFn func(ctx context.Context, query string) (PreparedStatementFn, error)

// PreparedStatementFn represents a query of which a statement has been
// prepared. The statement could be executed at any point in time with the given
// arguments and data writer.
type PreparedStatementFn func(ctx context.Context, writer DataWriter) error

// StatementCache represents a cache which could be used to store and retrieve
// prepared statements bound to a name.
type StatementCache interface {
	// Set attempts to bind the given statement to the given name. Any
	// previously defined statement is overridden.
	Set(ctx context.Context, name string, fn PreparedStatementFn) error
	// Get attempts to get the prepared statement for the given name. An error
	// is returned when no statement has been found.
	Get(ctx context.Context, name string) (PreparedStatementFn, error)
}


type PortalCache interface {
	Set(ctx context.Context) error
	Remove(ctx context.Context)
	Get(ctx context.Context) error
}

type CloseFn func(ctx context.Context) error

// OptionFn options pattern used to define and set options for the given
// PostgreSQL server.
type OptionFn func(*Server)

// SimpleQuery sets the simple query handle inside the given server instance.
func SimpleQuery(fn SimpleQueryFn) OptionFn {
	return func(srv *Server) {
		if srv.Parse != nil {
			return
		}

		srv.Parse = func(ctx context.Context, query string) (PreparedStatementFn, error) {
			statement := func(ctx context.Context, writer DataWriter) error {
				return fn(ctx, query, writer)
			}

			return statement, nil
		}
	}
}

// Parse sets the given parse function used to parse queries into prepared statements.
func Parse(fn ParseFn) OptionFn {
	return func(srv *Server) {
		srv.Parse = fn
	}
}

// Cache sets the statement cache used to cache statements for later use. By
// default is the DefaultStatementCache used to cache prepared statements.
func Cache(fn StatementCache) OptionFn {
	return func(srv *Server) {
		srv.Statements = fn
	}
}

// CloseConn sets the close connection handle inside the given server instance.
func CloseConn(fn CloseFn) OptionFn {
	return func(srv *Server) {
		srv.CloseConn = fn
	}
}

// TerminateConn sets the terminate connection handle inside the given server instance.
func TerminateConn(fn CloseFn) OptionFn {
	return func(srv *Server) {
		srv.TerminateConn = fn
	}
}

// MessageBufferSize sets the message buffer size which is allocated once a new
// connection gets constructed. If a negative value or zero value is provided is
// the default message buffer size used.
func MessageBufferSize(size int) OptionFn {
	return func(srv *Server) {
		srv.BufferedMsgSize = size
	}
}

// Certificates sets the given TLS certificates to be used to initialize a
// secure connection between the front-end (client) and back-end (server).
func Certificates(certs []tls.Certificate) OptionFn {
	return func(srv *Server) {
		srv.Certificates = certs
	}
}

// ClientCAs sets the given Client CAs to be used, by the server, to verify a
// secure connection between the front-end (client) and back-end (server).
func ClientCAs(cas *x509.CertPool) OptionFn {
	return func(srv *Server) {
		srv.ClientCAs = cas
	}
}

// ClientAuth sets the given Client Auth to be used, by the server, to verify a
// secure connection between the front-end (client) and back-end (server).
func ClientAuth(authType tls.ClientAuthType) OptionFn {
	return func(srv *Server) {
		srv.ClientAuth = authType
	}
}

// GlobalParameters sets the server parameters which are send back to the
// front-end (client) once a handshake has been established.
func GlobalParameters(params Parameters) OptionFn {
	return func(srv *Server) {
		srv.Parameters = params
	}
}

// Logger sets the given zap logger as the default logger for the given server.
func Logger(logger *zap.Logger) OptionFn {
	return func(srv *Server) {
		srv.logger = logger
	}
}

// Version sets the PostgreSQL version for the server which is send back to the
// front-end (client) once a handshake has been established.
func Version(version string) OptionFn {
	return func(srv *Server) {
		srv.Version = version
	}
}

// ExtendTypes provides the ability to extend the underlying connection types.
// Types registered inside the given pgtype.ConnInfo are registered to all
// incoming connections.
func ExtendTypes(fn func(*pgtype.ConnInfo)) OptionFn {
	return func(srv *Server) {
		fn(srv.types)
	}
}
