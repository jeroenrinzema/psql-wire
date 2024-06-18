package wire

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"

	"log/slog"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jeroenrinzema/psql-wire/pkg/buffer"
	"github.com/jeroenrinzema/psql-wire/pkg/types"
)

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
	srv := &Server{
		parse:      parse,
		logger:     slog.Default(),
		closer:     make(chan struct{}),
		types:      pgtype.NewMap(),
		Statements: &DefaultStatementCache{},
		Portals:    &DefaultPortalCache{},
		Session:    func(ctx context.Context) (context.Context, error) { return ctx, nil },
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
	closing         atomic.Bool
	wg              sync.WaitGroup
	logger          *slog.Logger
	types           *pgtype.Map
	Auth            AuthStrategy
	BufferedMsgSize int
	Parameters      Parameters
	TLSConfig       *tls.Config
	Certificates    []tls.Certificate
	ClientCAs       *x509.CertPool
	ClientAuth      tls.ClientAuthType
	parse           ParseFn
	Session         SessionHandler
	Statements      StatementCache
	Portals         PortalCache
	CloseConn       CloseFn
	TerminateConn   CloseFn
	Version         string
	closer          chan struct{}
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
	srv.wg.Add(1)

	// NOTE: handle graceful shutdowns
	go func() {
		defer srv.wg.Done()
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

		go func() {
			ctx := context.Background()
			err = srv.serve(ctx, conn)
			if err != nil {
				srv.logger.Error("an unexpected error got returned while serving a client connectio", "err", err)
			}
		}()
	}
}

func (srv *Server) serve(ctx context.Context, conn net.Conn) error {
	ctx = setTypeInfo(ctx, srv.types)
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

	return srv.consumeCommands(ctx, conn, reader, writer)
}

// Close gracefully closes the underlaying Postgres server.
func (srv *Server) Close() error {
	if srv.closing.Load() {
		return nil
	}

	srv.closing.Store(true)
	close(srv.closer)
	srv.wg.Wait()
	return nil
}
