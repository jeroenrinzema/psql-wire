package wire

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"

	"github.com/jeroenrinzema/psql-wire/codes"
	psqlerr "github.com/jeroenrinzema/psql-wire/errors"
	"github.com/jeroenrinzema/psql-wire/internal/buffer"
	"github.com/jeroenrinzema/psql-wire/internal/types"
	"go.uber.org/zap"
)

// NewErrUnimplementedMessageType is called whenever a unimplemented message
// type is send. This error indicates to the client that the send message cannot
// be processed at this moment in time.
func NewErrUnimplementedMessageType(t types.ClientMessage) error {
	err := fmt.Errorf("unimplemented client message type: %d", t)
	return psqlerr.WithSeverity(psqlerr.WithCode(err, codes.ConnectionDoesNotExist), psqlerr.LevelFatal)
}

type SimpleQueryFn func(ctx context.Context, query string, writer DataWriter) error

type CloseFn func(ctx context.Context) error

// consumeCommands consumes incoming commands send over the Postgres wire connection.
// Commands consumed from the connection are returned through a go channel.
// Responses for the given message type are written back to the client.
// This method keeps consuming messages until the client issues a close message
// or the connection is terminated.
func (srv *Server) consumeCommands(ctx context.Context, conn net.Conn, reader *buffer.Reader, writer *buffer.Writer) (err error) {
	srv.logger.Debug("ready for query... starting to consume commands")

	// TODO(Jeroen): include a indentification value inside the context that
	// could be used to identify connections at a later stage.

	for {
		err = readyForQuery(writer, types.ServerIdle)
		if err != nil {
			return err
		}

		t, length, err := reader.ReadTypedMsg()
		if err == io.EOF {
			return nil
		}

		// NOTE(Jeroen): we could recover from this scenario
		if errors.Is(err, buffer.ErrMessageSizeExceeded) {
			err = srv.handleMessageSizeExceeded(reader, writer, err)
			if err != nil {
				return err
			}

			continue
		}

		srv.logger.Debug("incoming command", zap.Int("length", length), zap.String("type", string(t)))

		if err != nil {
			return err
		}

		err = srv.handleCommand(ctx, conn, t, reader, writer)
		if err != nil {
			return err
		}
	}
}

// handleMessageSizeExceeded attempts to unwrap the given error message as
// message size exceeded. The expected message size will be consumed and
// discarded from the given reader. An error message is written to the client
// once the expected message size is read.
//
// The given error is returned if it does not contain an message size exceeded
// type. A fatal error is returned when an unexpected error is returned while
// consuming the expected message size or when attempting to write the error
// message back to the client.
func (srv *Server) handleMessageSizeExceeded(reader *buffer.Reader, writer *buffer.Writer, exceeded error) (err error) {
	unwrapped, has := buffer.UnwrapMessageSizeExceeded(exceeded)
	if !has {
		return exceeded
	}

	err = reader.Slurp(unwrapped.Size)
	if err != nil {
		return err
	}

	return ErrorCode(writer, exceeded)
}

// handleCommand handles the given client message. A client message includes a
// message type and reader buffer containing the actual message. The type
// indecates a action executed by the client.
func (srv *Server) handleCommand(ctx context.Context, conn net.Conn, t types.ClientMessage, reader *buffer.Reader, writer *buffer.Writer) (err error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	switch t {
	case types.ClientSync:
		// TODO(Jeroen): client sync received
	case types.ClientSimpleQuery:
		return srv.handleSimpleQuery(ctx, reader, writer)
	case types.ClientExecute:
	case types.ClientParse:
	case types.ClientDescribe:
	case types.ClientBind:
	case types.ClientFlush:
	case types.ClientCopyData, types.ClientCopyDone, types.ClientCopyFail:
		// We're supposed to ignore these messages, per the protocol spec. This
		// state will happen when an error occurs on the server-side during a copy
		// operation: the server will send an error and a ready message back to
		// the client, and must then ignore further copy messages. See:
		// https://github.com/postgres/postgres/blob/6e1dd2773eb60a6ab87b27b8d9391b756e904ac3/src/backend/tcop/postgres.c#L4295
		break
	case types.ClientClose:
		err = srv.handleConnClose(ctx)
		if err != nil {
			return err
		}

		return conn.Close()
	case types.ClientTerminate:
		err = srv.handleConnTerminate(ctx)
		if err != nil {
			return err
		}

		return conn.Close()
	default:
		return ErrorCode(writer, NewErrUnimplementedMessageType(t))
	}

	return nil
}

func (srv *Server) handleSimpleQuery(ctx context.Context, reader *buffer.Reader, writer *buffer.Writer) error {
	if srv.SimpleQuery == nil {
		return ErrorCode(writer, NewErrUnimplementedMessageType(types.ClientSimpleQuery))
	}

	query, err := reader.GetString()
	if err != nil {
		return err
	}

	srv.logger.Debug("incoming query", zap.String("query", query))

	err = srv.SimpleQuery(ctx, query, &dataWriter{
		ctx:    ctx,
		client: writer,
	})

	if err != nil {
		return ErrorCode(writer, err)
	}

	return nil
}

func (srv *Server) handleConnClose(ctx context.Context) error {
	if srv.CloseConn == nil {
		return nil
	}

	return srv.CloseConn(ctx)
}

func (srv *Server) handleConnTerminate(ctx context.Context) error {
	if srv.TerminateConn == nil {
		return nil
	}

	return srv.TerminateConn(ctx)
}
