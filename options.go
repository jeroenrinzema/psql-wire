package wire

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"regexp"
	"strconv"

	"github.com/jackc/pgtype"
	"github.com/lib/pq/oid"
	"go.uber.org/zap"
)

// QueryParameters represents a regex which could be used to identify and lookup
// parameters defined inside a given query. Parameters could be defined as
// positional parameters and un-positional parameters.
// https://www.postgresql.org/docs/8.1/sql-syntax.html#:~:text=A%20dollar%20sign%20(%24)%20followed,a%20dollar%2Dquoted%20string%20constant.
var QueryParameters = regexp.MustCompile(`\$(\d+)|\?`)

// SimpleQueryFn represents a callback function called whenever an incoming
// query is being executed.
type SimpleQueryFn func(ctx context.Context, query string, writer DataWriter, parameters []string) error

// ParseFn parses the given query and returns a prepared statement which could
// be used to execute at a later point in time.
type ParseFn func(ctx context.Context, query string) (PreparedStatementFn, []oid.Oid, error)

// PreparedStatementFn represents a query of which a statement has been
// prepared. The statement could be executed at any point in time with the given
// arguments and data writer.
type PreparedStatementFn func(ctx context.Context, writer DataWriter, parameters []string) error

// SessionHandler represents a wrapper function defining the state of a single
// session. This function allows the user to wrap additional metadata around the
// shared context.
type SessionHandler func(ctx context.Context) (context.Context, error)

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

// PortalCache represents a cache which could be used to bind and execute
// prepared statements with parameters.
type PortalCache interface {
	Bind(ctx context.Context, name string, statement PreparedStatementFn, parameters []string) error
	Execute(ctx context.Context, name string, writer DataWriter) error
}

type CloseFn func(ctx context.Context) error

// OptionFn options pattern used to define and set options for the given
// PostgreSQL server.
type OptionFn func(*Server) error

// SimpleQuery sets the simple query handle inside the given server instance.
func SimpleQuery(fn SimpleQueryFn) OptionFn {
	return func(srv *Server) error {
		if srv.Parse != nil {
			return errors.New("simple query handler could not set if a query parser is set")
		}

		srv.Parse = func(ctx context.Context, query string) (PreparedStatementFn, []oid.Oid, error) {
			statement := func(ctx context.Context, writer DataWriter, parameters []string) error {
				return fn(ctx, query, writer, parameters)
			}

			// NOTE: we have to lookup all parameters within the given query.
			// Parameters could represent positional parameters or anonymous
			// parameters. We return a zero parameter oid for each parameter
			// indicating that the given parameters could contain any type. We
			// could safely ignore the err check while converting given
			// parameters since ony matches are returned by the positional
			// parameter regex.
			matches := QueryParameters.FindAllStringSubmatch(query, -1)
			parameters := make([]oid.Oid, 0, len(matches))
			for _, match := range matches {
				// NOTE: we have to check whether the returned match is a
				// positional parameter or an un-positional parameter.
				// SELECT * FROM users WHERE id = ?
				if match[1] == "" {
					parameters = append(parameters, 0)
				}

				position, _ := strconv.Atoi(match[1]) //nolint:errcheck
				if position > len(parameters) {
					parameters = parameters[:position]
				}
			}

			return statement, parameters, nil
		}

		return nil
	}
}

// Parse sets the given parse function used to parse queries into prepared statements.
func Parse(fn ParseFn) OptionFn {
	return func(srv *Server) error {
		if srv.Parse != nil {
			return errors.New("parser could not set if a simple query handler is set")
		}

		srv.Parse = fn
		return nil
	}
}

// Statements sets the statement cache used to cache statements for later use. By
// default is the DefaultStatementCache used to cache prepared statements.
func Statements(cache StatementCache) OptionFn {
	return func(srv *Server) error {
		srv.Statements = cache
		return nil
	}
}

// Portals sets the portals cache used to cache statements for later use. By
// default is the DefaultPortalCache used to evaluate portals.
func Portals(cache PortalCache) OptionFn {
	return func(srv *Server) error {
		srv.Portals = cache
		return nil
	}
}

// CloseConn sets the close connection handle inside the given server instance.
func CloseConn(fn CloseFn) OptionFn {
	return func(srv *Server) error {
		srv.CloseConn = fn
		return nil
	}
}

// TerminateConn sets the terminate connection handle inside the given server instance.
func TerminateConn(fn CloseFn) OptionFn {
	return func(srv *Server) error {
		srv.TerminateConn = fn
		return nil
	}
}

// MessageBufferSize sets the message buffer size which is allocated once a new
// connection gets constructed. If a negative value or zero value is provided is
// the default message buffer size used.
func MessageBufferSize(size int) OptionFn {
	return func(srv *Server) error {
		srv.BufferedMsgSize = size
		return nil
	}
}

// Certificates sets the given TLS certificates to be used to initialize a
// secure connection between the front-end (client) and back-end (server).
func Certificates(certs []tls.Certificate) OptionFn {
	return func(srv *Server) error {
		srv.Certificates = certs
		return nil
	}
}

// ClientCAs sets the given Client CAs to be used, by the server, to verify a
// secure connection between the front-end (client) and back-end (server).
func ClientCAs(cas *x509.CertPool) OptionFn {
	return func(srv *Server) error {
		srv.ClientCAs = cas
		return nil
	}
}

// ClientAuth sets the given Client Auth to be used, by the server, to verify a
// secure connection between the front-end (client) and back-end (server).
func ClientAuth(authType tls.ClientAuthType) OptionFn {
	return func(srv *Server) error {
		srv.ClientAuth = authType
		return nil
	}
}

// GlobalParameters sets the server parameters which are send back to the
// front-end (client) once a handshake has been established.
func GlobalParameters(params Parameters) OptionFn {
	return func(srv *Server) error {
		srv.Parameters = params
		return nil
	}
}

// Logger sets the given zap logger as the default logger for the given server.
func Logger(logger *zap.Logger) OptionFn {
	return func(srv *Server) error {
		srv.logger = logger
		return nil
	}
}

// Version sets the PostgreSQL version for the server which is send back to the
// front-end (client) once a handshake has been established.
func Version(version string) OptionFn {
	return func(srv *Server) error {
		srv.Version = version
		return nil
	}
}

// ExtendTypes provides the ability to extend the underlying connection types.
// Types registered inside the given pgtype.ConnInfo are registered to all
// incoming connections.
func ExtendTypes(fn func(*pgtype.ConnInfo)) OptionFn {
	return func(srv *Server) error {
		fn(srv.types)
		return nil
	}
}

// Session sets the given session handler within the underlying server. The
// session handler is called when a new connection is opened and authenticated
// allowing for additional metadata to be wrapped around the connection context.
func Session(fn SessionHandler) OptionFn {
	return func(srv *Server) error {
		if srv.Session == nil {
			srv.Session = fn
			return nil
		}

		wrapper := func(parent SessionHandler) SessionHandler {
			return func(ctx context.Context) (context.Context, error) {
				ctx, err := parent(ctx)
				if err != nil {
					return ctx, err
				}

				return fn(ctx)
			}
		}

		srv.Session = wrapper(srv.Session)
		return nil
	}
}
