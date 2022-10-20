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
	"github.com/lib/pq/oid"
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
		// TODO(Jeroen): include the ability to catch sync messages in order to
		// close the current transaction.
		//
		// At completion of each series of extended-query messages, the frontend
		// should issue a Sync message. This parameterless message causes the
		// backend to close the current transaction if it's not inside a
		// BEGIN/COMMIT transaction block (“close” meaning to commit if no
		// error, or roll back if error). Then a ReadyForQuery response is
		// issued. The purpose of Sync is to provide a resynchronization point
		// for error recovery. When an error is detected while processing any
		// extended-query message, the backend issues ErrorResponse, then reads
		// and discards messages until a Sync is reached, then issues
		// ReadyForQuery and returns to normal message processing. (But note
		// that no skipping occurs if an error is detected while processing Sync
		// — this ensures that there is one and only one ReadyForQuery sent for
		// each Sync.)
		// https://www.postgresql.org/docs/current/protocol-flow.html#PROTOCOL-FLOW-EXT-QUERY
	case types.ClientSimpleQuery:
		return srv.handleSimpleQuery(ctx, reader, writer)
	case types.ClientExecute:
		return srv.handleExecute(ctx, reader, writer)
	case types.ClientParse:
		return srv.handleParse(ctx, reader, writer)
	case types.ClientDescribe:
		// TODO(Jeroen): the server should return the column types that will be
		// returned for the given portal or statement.
		//
		// The Describe message (portal variant) specifies the name of an
		// existing portal (or an empty string for the unnamed portal). The
		// response is a RowDescription message describing the rows that will be
		// returned by executing the portal; or a NoData message if the portal
		// does not contain a query that will return rows; or ErrorResponse if
		// there is no such portal.
		//
		// The Describe message (statement variant) specifies the name of an
		// existing prepared statement (or an empty string for the unnamed
		// prepared statement). The response is a ParameterDescription message
		// describing the parameters needed by the statement, followed by a
		// RowDescription message describing the rows that will be returned when
		// the statement is eventually executed (or a NoData message if the
		// statement will not return rows). ErrorResponse is issued if there is
		// no such prepared statement. Note that since Bind has not yet been
		// issued, the formats to be used for returned columns are not yet known
		// to the backend; the format code fields in the RowDescription message
		// will be zeroes in this case.
		// https://www.postgresql.org/docs/current/protocol-flow.html#PROTOCOL-FLOW-EXT-QUERY
	case types.ClientBind:
		return srv.handleBind(ctx, reader, writer)
	case types.ClientFlush:
		// TODO(Jeroen): flush all remaining rows inside connection buffer if
		// any are remaining. The Flush message does not cause any specific
		// output to be generated, but forces the backend to deliver any data
		// pending in its output buffers. A Flush must be sent after any
		// extended-query command except Sync, if the frontend wishes to examine
		// the results of that command before issuing more commands. Without
		// Flush, messages returned by the backend will be combined into the
		// minimum possible number of packets to minimize network overhead.
		// https://www.postgresql.org/docs/current/protocol-flow.html#PROTOCOL-FLOW-EXT-QUERY
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

	srv.logger.Debug("incoming simple query", zap.String("query", query))

	statement, _, err := srv.Parse(ctx, query)
	if err != nil {
		return err
	}

	if err != nil {
		return ErrorCode(writer, err)
	}

	err = statement(ctx, NewDataWriter(ctx, writer), nil)
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

	// NOTE(Jeroen): the number of parameter data types specified (can be
	// zero). Note that this is not an indication of the number of parameters
	// that might appear in the query string, only the number that the frontend
	// wants to prespecify types for.
	parameters, err := reader.GetUint16()
	if err != nil {
		return err
	}

	for i := uint16(0); i < parameters; i++ {
		// TODO(Jeroen): reader.GetUint32()
		// Specifies the object ID of the parameter data type. Placing a zero here
		// is equivalent to leaving the type unspecified.
	}

	statement, descriptions, err := srv.Parse(ctx, query)
	if err != nil {
		return ErrorCode(writer, err)
	}

	srv.logger.Debug("incoming extended query", zap.String("query", query), zap.String("name", name), zap.Int("parameters", len(descriptions)))

	err = srv.writeParameterDescriptions(writer, descriptions)
	if err != nil {
		return err
	}

	err = srv.Statements.Set(ctx, name, statement)
	if err != nil {
		return ErrorCode(writer, err)
	}

	writer.Start(types.ServerParseComplete)
	return writer.End()
}

func (srv *Server) writeParameterDescriptions(writer *buffer.Writer, parameters []oid.Oid) error {
	writer.Start(types.ServerParameterDescription)
	writer.AddInt16(int16(len(parameters)))

	for _, parameter := range parameters {
		writer.AddInt32(int32(parameter))
	}

	return writer.End()
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

	parameters, err := srv.readParameters(ctx, reader)
	if err != nil {
		return err
	}

	fn, err := srv.Statements.Get(ctx, statement)
	if err != nil {
		return err
	}

	err = srv.Portals.Bind(ctx, name, fn, parameters)
	if err != nil {
		return err
	}

	writer.Start(types.ServerBindComplete)
	return writer.End()
}

// readParameters attempts to read all incoming parameters from the given
// reader. The parameters are parsed and returned.
// https://www.postgresql.org/docs/14/protocol-message-formats.html
func (srv *Server) readParameters(ctx context.Context, reader *buffer.Reader) ([]string, error) {
	// NOTE(Jeroen): read the total amount of parameter format codes that will
	// be send by the client.
	length, err := reader.GetUint16()
	if err != nil {
		return nil, err
	}

	srv.logger.Debug("reading parameters format codes", zap.Uint16("length", length))

	for i := uint16(0); i < length; i++ {
		format, err := reader.GetUint16()
		if err != nil {
			return nil, err
		}

		// NOTE(Jeroen): the parameter format codes. Each must presently be zero (text) or one (binary).
		// https://www.postgresql.org/docs/14/protocol-message-formats.html
		if format != 0 {
			return nil, errors.New("unsupported binary parameter format, only text formatted parameter types are currently supported")
		}

		// TODO(Jeroen): handle multiple parameter format codes.
	}

	// NOTE(Jeroen): read the total amount of parameter values that will be send
	// by the client.
	length, err = reader.GetUint16()
	if err != nil {
		return nil, err
	}

	srv.logger.Debug("reading parameters values", zap.Uint16("length", length))

	parameters := make([]string, length)
	for i := uint16(0); i < length; i++ {
		length, err := reader.GetUint32()
		if err != nil {
			return nil, err
		}

		value, err := reader.GetBytes(int(length))
		if err != nil {
			return nil, err
		}

		srv.logger.Debug("incoming parameter", zap.String("value", string(value)))
		parameters[i] = string(value)
	}

	// NOTE(Jeroen): read the total amount of result-column format that will be
	// send by the client.
	length, err = reader.GetUint16()
	if err != nil {
		return nil, err
	}

	srv.logger.Debug("reading result-column format codes", zap.Uint16("length", length))

	for i := uint16(0); i < length; i++ {
		// TODO(Jeroen): handle incoming result-column format codes
		_, err := reader.GetUint16()
		if err != nil {
			return nil, err
		}
	}

	return parameters, nil
}

func (srv *Server) handleExecute(ctx context.Context, reader *buffer.Reader, writer *buffer.Writer) error {
	if srv.Statements == nil {
		return ErrorCode(writer, NewErrUnimplementedMessageType(types.ClientExecute))
	}

	name, err := reader.GetString()
	if err != nil {
		return err
	}

	// TODO(Jeroen): Maximum number of limit to return, if portal contains a
	// query that returns limit (ignored otherwise). Zero denotes “no limit”.
	limit, err := reader.GetUint32()
	if err != nil {
		return err
	}

	srv.logger.Debug("executing", zap.String("name", name), zap.Uint32("limit", limit))
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
