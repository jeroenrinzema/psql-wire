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

// NewErrUnkownStatement is returned whenever no executable has been found for
// the given name.
func NewErrUnkownStatement(name string) error {
	err := fmt.Errorf("unknown executeable: %s", name)
	return psqlerr.WithSeverity(psqlerr.WithCode(err, codes.InvalidPreparedStatementDefinition), psqlerr.LevelFatal)
}

// consumeCommands consumes incoming commands send over the Postgres wire connection.
// Commands consumed from the connection are returned through a go channel.
// Responses for the given message type are written back to the client.
// This method keeps consuming messages until the client issues a close message
// or the connection is terminated.
func (srv *Server) consumeCommands(ctx context.Context, conn net.Conn, reader *buffer.Reader, writer *buffer.Writer) (err error) {
	srv.logger.Debug("ready for query... starting to consume commands")

	// TODO(Jeroen): include a identification value inside the context that
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
		if err == io.EOF {
			return nil
		}

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
// https://www.postgresql.org/docs/14/protocol-message-formats.html
func (srv *Server) handleCommand(ctx context.Context, conn net.Conn, t types.ClientMessage, reader *buffer.Reader, writer *buffer.Writer) (err error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	switch t {
	case types.ClientSync:
		// TODO(Jeroen): client sync received
	case types.ClientSimpleQuery:
		return srv.handleSimpleQuery(ctx, reader, writer)
	case types.ClientExecute:
		return srv.handleExecute(ctx, reader, writer)
	case types.ClientParse:
		return srv.handleParse(ctx, reader, writer)
	case types.ClientDescribe:
		fmt.Println("Describe Message:")
		fmt.Println(reader.GetBytes(1)) // 'S' to describe a prepared statement; or 'P' to describe a portal.
		fmt.Println(reader.GetString()) // The name of the prepared statement or portal to describe (an empty string selects the unnamed prepared statement or portal).
	case types.ClientBind:
		return srv.handleBind(ctx, reader, writer)
	case types.ClientFlush:
		// TODO(Jeroen): flush received
		fmt.Println("Flush!")
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

		err = conn.Close()
		if err != nil {
			return err
		}

		return io.EOF
	default:
		return ErrorCode(writer, NewErrUnimplementedMessageType(t))
	}

	return nil
}

func (srv *Server) handleSimpleQuery(ctx context.Context, reader *buffer.Reader, writer *buffer.Writer) error {
	if srv.Parse == nil {
		return ErrorCode(writer, NewErrUnimplementedMessageType(types.ClientSimpleQuery))
	}

	query, err := reader.GetString()
	if err != nil {
		return err
	}

	srv.logger.Debug("incoming query", zap.String("query", query))

	statement, err := srv.Parse(ctx, query)
	if err != nil {
		return err
	}

	if err != nil {
		return ErrorCode(writer, err)
	}

	err = statement(ctx, NewDataWriter(ctx, writer))
	if err != nil {
		return ErrorCode(writer, err)
	}

	return nil
}

func (srv *Server) handleParse(ctx context.Context, reader *buffer.Reader, writer *buffer.Writer) error {
	if srv.Parse == nil || srv.Statements == nil {
		return ErrorCode(writer, NewErrUnimplementedMessageType(types.ClientParse))
	}

	name, err := reader.GetString()
	if err != nil {
		return err
	}

	query, err := reader.GetString()
	if err != nil {
		return err
	}

	// TODO(Jeroen): reader.GetUint16()
	// The number of parameter data types specified (can be zero). Note that
	// this is not an indication of the number of parameters that might appear
	// in the query string, only the number that the frontend wants to
	// prespecify types for.

	// TODO(Jeroen): reader.GetUint32()
	// Specifies the object ID of the parameter data type. Placing a zero here
	// is equivalent to leaving the type unspecified.

	srv.logger.Debug("incoming query", zap.String("query", query), zap.String("name", name))

	statement, err := srv.Parse(ctx, query)
	if err != nil {
		return ErrorCode(writer, err)
	}

	err = srv.Statements.Set(ctx, name, statement)
	if err != nil {
		return ErrorCode(writer, err)
	}

	writer.Start(types.ServerParseComplete)
	writer.End() //nolint:errcheck

	return nil
}

func (srv *Server) handleBind(ctx context.Context, reader *buffer.Reader, writer *buffer.Writer) error {
	name, err := reader.GetString()
	if err != nil {
		return err
	}

	statement, err := reader.GetString()
	if err != nil {
		return err
	}

	_, err = srv.readParameters(ctx, reader)
	if err != nil {
		return err
	}

	fn, err := srv.Statements.Get(ctx, statement)
	if err != nil {
		return err
	}

	err = srv.Portals.Bind(ctx, name, fn)
	if err != nil {
		return err
	}

	writer.Start(types.ServerBindComplete)
	return writer.End()
}

func (srv *Server) readParameters(ctx context.Context, reader *buffer.Reader) ([]interface{}, error) {
	length, err := reader.GetUint16()
	if err != nil {
		return nil, err
	}

	srv.logger.Debug("reading parameters", zap.Uint16("length", length))
	if length == 0 {
		return nil, nil
	}

	for i := uint16(0); i < length; i++ {
		fmt.Println(reader.GetUint16()) // parameter format code
		fmt.Println(reader.GetUint16()) // number of parameters that will follow
		fmt.Println(reader.GetUint32()) // length of the value
		fmt.Println(reader.GetBytes(0)) // value in bytes (based on length)
		fmt.Println(reader.GetUint16()) // The number of result-column format codes that follow
		fmt.Println(reader.GetUint16()) // The result-column format codes
	}

	return nil, nil
}

func (srv *Server) handleExecute(ctx context.Context, reader *buffer.Reader, writer *buffer.Writer) error {
	if srv.Statements == nil {
		return ErrorCode(writer, NewErrUnimplementedMessageType(types.ClientExecute))
	}

	name, err := reader.GetString()
	if err != nil {
		return err
	}

	// TODO(Jeroen): reader.GetUint32()
	// Maximum number of rows to return, if portal
	// contains a query that returns rows (ignored otherwise). Zero denotes “no
	// limit”.

	srv.logger.Debug("executing", zap.String("name", name))
	return srv.Portals.Execute(ctx, name, NewDataWriter(ctx, writer))
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
