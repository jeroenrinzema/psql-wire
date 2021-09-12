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
// default configurations.
func ListenAndServe(addr string, handler Handler) error {
	server, err := NewServer(addr, handler)
	if err != nil {
		return err
	}

	return server.ListenAndServe()
}

// NewServer constructs a new Postgres server using the given context address and handler.
func NewServer(addr string, handler Handler) (*Server, error) {
	logger, err := zap.NewProduction()
	if err != nil {
		return nil, err
	}

	srv := &Server{
		logger:  logger,
		Addr:    addr,
		Handler: handler,
	}

	return srv, nil
}

// Server contains options for listening to an address.
type Server struct {
	logger       *zap.Logger
	Addr         string
	Auth         AuthStrategy
	Parameters   Parameters
	Certificates []tls.Certificate
	Handler      Handler
	closer       chan struct{}
}

// ListenAndServe opens a new Postgres server on the preconfigured address and
// starts accepting and serving incoming client connections.
func (srv *Server) ListenAndServe() error {
	listener, err := net.Listen("tcp", srv.Addr)
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

	conn, _, reader, err := srv.Handshake(conn)
	if err != nil {
		return err
	}

	writer := buffer.NewWriter(conn)
	ctx, err = srv.ReadParameters(ctx, reader)
	if err != nil {
		return err
	}

	err = srv.HandleAuth(ctx, reader, writer)
	if err != nil {
		return err
	}

	ctx, err = srv.WriteParameters(ctx, writer, srv.Parameters)
	if err != nil {
		return err
	}

	return srv.ConsumeCommands(ctx, srv.Handler, reader, writer)
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
