package wire

import (
	"context"
	"crypto/tls"
	"errors"
	"net"

	"github.com/jeroenrinzema/psql-wire/buffer"
	"github.com/jeroenrinzema/psql-wire/types"
	"go.uber.org/zap"
)

// ErrServerClosed indicates that the given Postgres server has been closed
var ErrServerClosed = errors.New("server closed")

// ListenAndServe opens a new Postgres server using the given address and
// default configurations. The given handler function is used to handle simple
// queries. This method should be used to construct a simple Postgres server for
// testing purposes or simple use cases.
func ListenAndServe(address string, handler SimpleQueryFn) error {
	server, err := NewServer(SimpleQuery(handler))
	if err != nil {
		return err
	}

	return server.ListenAndServe(address)
}

// NewServer constructs a new Postgres server using the given address and server options.
func NewServer(options ...OptionFn) (*Server, error) {
	srv := &Server{
		logger: zap.NewNop(),
		closer: make(chan struct{}),
	}

	for _, option := range options {
		option(srv)
	}

	return srv, nil
}

// Server contains options for listening to an address.
type Server struct {
	logger          *zap.Logger
	Auth            AuthStrategy
	BufferedMsgSize int
	Parameters      Parameters
	Certificates    []tls.Certificate
	SimpleQuery     SimpleQueryFn
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
// preconfigured configurations.
func (srv *Server) Serve(listener net.Listener) error {
	defer listener.Close()
	defer srv.logger.Info("closing server")

	srv.logger.Info("serving incoming connections", zap.String("addr", listener.Addr().String()))

	for {
		select {
		case <-srv.closer:
			err := listener.Close()
			if err != nil {
				return err
			}

			return ErrServerClosed
		default:
		}

		conn, err := listener.Accept()
		if err != nil {
			return err
		}

		go func() {
			ctx := context.Background()
			err = srv.serve(ctx, conn)
			if err != nil {
				srv.logger.Error("an unexpected error got returned while serving a client connection", zap.Error(err))
			}
		}()
	}
}

func (srv *Server) serve(ctx context.Context, conn net.Conn) error {
	ctx = setTypeInfo(ctx)
	defer conn.Close()

	srv.logger.Debug("serving a new client connection")

	conn, version, reader, err := srv.Handshake(conn)
	if err != nil {
		return err
	}

	if version == VersionCancel {
		return conn.Close()
	}

	srv.logger.Debug("handshake successfull, validating authentication")

	writer := buffer.NewWriter(conn)
	ctx, err = srv.ReadParameters(ctx, reader)
	if err != nil {
		return err
	}

	err = srv.HandleAuth(ctx, reader, writer)
	if err != nil {
		return err
	}

	srv.logger.Debug("connection authenticated, writing server parameters")

	ctx, err = srv.WriteParameters(ctx, writer, srv.Parameters)
	if err != nil {
		return err
	}

	return srv.ConsumeCommands(ctx, srv.SimpleQuery, reader, writer)
}

// Close gracefully closes the underlaying Postgres server
func (srv *Server) Close() error {
	close(srv.closer)
	return nil
}

// CommandComplete announces that the requested command has successfully been executed.
// The given description is written back to the client and could be used to send
// additional meta data to the user.
func CommandComplete(writer *buffer.Writer, description string) error {
	writer.Start(types.ServerCommandComplete)
	writer.AddString(description)
	writer.AddNullTerminate()
	return writer.End()
}

// EmptyQuery indicates a empty query response by sending a EmptyQuery message.
func EmptyQuery(writer *buffer.Writer) error {
	writer.Start(types.ServerEmptyQuery)
	return writer.End()
}
