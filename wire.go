package wire

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jeroenrinzema/psql-wire/pkg/buffer"
	"github.com/jeroenrinzema/psql-wire/pkg/types"
)

type contextKey string

const sessionKey contextKey = "pgsession"

// GetSession retrieves the session from the context.
// The first return value is the session object, which can be used to access all session data.
// The second return value indicates whether the session was found in the context.
func GetSession(ctx context.Context) (*Session, bool) {
	session, ok := ctx.Value(sessionKey).(*Session)
	return session, ok
}

// GetAttribute retrieves a custom attribute from the session by key.
// The first return value is the attribute value, which will be nil if the attribute doesn't exist.
// The second return value indicates whether the attribute was found.
//
// Example:
//
//	tenantID, ok := wire.GetAttribute(ctx, "tenant_id")
//	if ok {
//	    // Use tenantID
//	}
func GetAttribute(ctx context.Context, key string) (interface{}, bool) {
	session, ok := GetSession(ctx)
	if !ok {
		return nil, false
	}

	value, exists := session.Attributes[key]
	return value, exists
}

// SetAttribute sets a custom attribute in the session.
// The key is the attribute name, and value can be any type.
// Returns true if the attribute was set successfully, false if the session wasn't found.
//
// Example:
//
//	wire.SetAttribute(ctx, "tenant_id", "tenant-123")
func SetAttribute(ctx context.Context, key string, value interface{}) bool {
	session, ok := GetSession(ctx)
	if !ok {
		return false
	}

	session.Attributes[key] = value
	return true
}

// ListenAndServe opens a new Postgres server using the given address and
// default configurations. The given handler function is used to handle simple
// queries. This method should be used to construct a simple Postgres server for
// testing purposes or simple use cases.
func ListenAndServe(address string, handler ParseFn) error {
	server, err := NewServer(handler)
	if err != nil {
		return err
	}

	return server.ListenAndServe(address)
}

// NewServer constructs a new Postgres server using the given address and server options.
func NewServer(parse ParseFn, options ...OptionFn) (*Server, error) {
	shutdownCtx, shutdownCancel := context.WithCancel(context.Background())

	srv := &Server{
		parse:                   parse,
		logger:                  slog.Default(),
		closer:                  make(chan struct{}),
		types:                   pgtype.NewMap(),
		Statements:              DefaultStatementCacheFn,
		Portals:                 DefaultPortalCacheFn,
		Session:                 func(ctx context.Context) (context.Context, error) { return ctx, nil },
		GracefulShutdownTimeout: 1 * time.Second,
		shutdownCtx:             shutdownCtx,
		shutdownCancel:          shutdownCancel,
	}

	for _, option := range options {
		err := option(srv)
		if err != nil {
			return nil, fmt.Errorf("unexpected error while attempting to configure a new server: %w", err)
		}
	}

	return srv, nil
}

// Server contains options for listening to an address.
type Server struct {
	closing                 atomic.Bool
	connectionWG            sync.WaitGroup  // Track active connections
	logger                  *slog.Logger
	types                   *pgtype.Map
	Auth                    AuthStrategy
	BufferedMsgSize         int
	Parameters              Parameters
	TLSConfig               *tls.Config
	parse                   ParseFn
	Session                 SessionHandler
	Statements              func() StatementCache
	Portals                 func() PortalCache
	CloseConn               CloseFn
	TerminateConn           CloseFn
	Version                 string
	GracefulShutdownTimeout time.Duration
	closer                  chan struct{}
	shutdownCtx             context.Context
	shutdownCancel          context.CancelFunc
}

// ListenAndServe opens a new Postgres server on the preconfigured address and
// starts accepting and serving incoming client connections.
func (srv *Server) ListenAndServe(address string) error {
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}

	return srv.Serve(listener)
}

// Serve accepts and serves incoming Postgres client connections using the
// preconfigured configurations. The given listener will be closed once the
// server is gracefully closed.
func (srv *Server) Serve(listener net.Listener) error {
	defer srv.logger.Info("closing server")

	srv.logger.Info("serving incoming connections", slog.String("addr", listener.Addr().String()))

	// NOTE: handle graceful shutdowns - this is infrastructure, not a client connection
	go func() {
		<-srv.closer

		err := listener.Close()
		if err != nil {
			srv.logger.Error("unexpected error while attempting to close the net listener", "err", err)
		}
	}()

	for {
		conn, err := listener.Accept()
		if errors.Is(err, net.ErrClosed) {
			return nil
		}

		if err != nil {
			return err
		}

		// Check if server is closing after accepting connection to avoid race
		if srv.closing.Load() {
			conn.Close()
			return nil
		}

		// Track this connection in connection waitgroup
		srv.connectionWG.Add(1)
		go func(conn net.Conn) {
			defer srv.connectionWG.Done()
			// Use shutdown context to allow forced termination
			ctx, cancel := context.WithCancel(srv.shutdownCtx)
			defer cancel()
			err = srv.serve(ctx, conn)
			if err != nil && err != io.EOF && !errors.Is(err, context.Canceled) {
				srv.logger.Error("an unexpected error got returned while serving a client connection", "err", err)
			}
		}(conn)
	}
}

func (srv *Server) serve(ctx context.Context, conn net.Conn) error {
	ctx = setTypeInfo(ctx, srv.types)
	ctx = setRemoteAddress(ctx, conn.RemoteAddr())
	defer conn.Close()

	srv.logger.Debug("serving a new client connection")

	conn, version, reader, err := srv.Handshake(conn)
	if err != nil {
		return err
	}

	if version == types.VersionCancel {
		return conn.Close()
	}

	srv.logger.Debug("handshake successfull, validating authentication")

	writer := buffer.NewWriter(srv.logger, conn)
	ctx, err = srv.readClientParameters(ctx, reader)
	if err != nil {
		return err
	}

	ctx, err = srv.handleAuth(ctx, reader, writer)
	if err != nil {
		return err
	}

	srv.logger.Debug("connection authenticated, writing server parameters")

	ctx, err = srv.writeParameters(ctx, writer, srv.Parameters)
	if err != nil {
		return err
	}

	ctx, err = srv.Session(ctx)
	if err != nil {
		return err
	}

	session := &Session{
		Server:     srv,
		Statements: srv.Statements(),
		Portals:    srv.Portals(),
		Attributes: make(map[string]interface{}),
	}

	ctx = context.WithValue(ctx, sessionKey, session)

	return session.consumeCommands(ctx, conn, reader, writer)
}

// Close gracefully closes the underlaying Postgres server.
func (srv *Server) Close() error {
	if srv.closing.Load() {
		return nil
	}

	srv.logger.Info("initiating server shutdown")
	srv.closing.Store(true)
	close(srv.closer)

	// Wait for connections to finish with configurable timeout
	done := make(chan struct{})
	go func() {
		defer close(done)
		srv.connectionWG.Wait()
	}()

	timeout := srv.GracefulShutdownTimeout
	if timeout <= 0 {
		timeout = 1 * time.Second // Default fallback
	}

	select {
	case <-done:
		srv.logger.Info("server shutdown completed gracefully")
	case <-time.After(timeout):
		srv.logger.Warn("graceful shutdown timeout reached, forcing termination", "timeout", timeout)
		// Cancel shutdown context to force all operations to stop
		srv.shutdownCancel()

		// Give a brief moment for context cancellation to propagate
		forceTimeout := 100 * time.Millisecond
		select {
		case <-done:
			srv.logger.Info("server shutdown completed after forced termination")
		case <-time.After(forceTimeout):
			srv.logger.Error("server shutdown incomplete after forced termination")
		}
	}

	return nil
}
