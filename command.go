package wire

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/jeroenrinzema/psql-wire/buffer"
	"github.com/jeroenrinzema/psql-wire/types"
	"go.uber.org/zap"
)

type SimpleQueryFn func(ctx context.Context, query string, writer DataWriter) error

// ConsumeCommands consumes incoming commands send over the Postgres wire connection.
// Commands consumed from the connection are returned through a go channel.
// Responses for the given message type are written back to the client.
// This method keeps consuming messages until the client issues a close message
// or the connection is terminated.
func (srv *Server) ConsumeCommands(ctx context.Context, handle SimpleQueryFn, reader *buffer.Reader, writer *buffer.Writer) (err error) {
	srv.logger.Debug("ready for query... starting to consume commands")

	for {
		err = ReadyForQuery(writer, ServerIdle)
		if err != nil {
			return err
		}

		t, length, err := reader.ReadTypedMsg()
		if err == io.EOF {
			return nil
		}

		srv.logger.Debug("incoming command", zap.Int("length", length), zap.String("type", string(t)))

		if err != nil {
			return err
		}

		err = srv.commandHandle(ctx, t, handle, reader, writer)
		if err != nil {
			return err
		}
	}
}

func (srv *Server) commandHandle(ctx context.Context, t types.ClientMessage, handle SimpleQueryFn, reader *buffer.Reader, writer *buffer.Writer) (err error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	switch t {
	case types.ClientSync:
		// TODO(Jeroen): client sync received
	case types.ClientSimpleQuery:
		return srv.handleSimpleQuery(ctx, handle, reader, writer)
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
		// TODO(Jeroen): handle graceful close
		return nil
	case types.ClientTerminate:
		return nil
	default:
		err = ErrorCode(writer, fmt.Errorf("unrecognized client message type %d", t))
		if err != nil {
			return err
		}
	}

	return nil
}

// DataWriter represents a writer interface for writing columns and data rows
// using the Postgres wire to the connected client.
type DataWriter interface {
	// Define writes the column headers containing their type definitions, width
	// type oid, etc. to the underlaying Postgres client.
	Define(Columns) error
	// Row writes a single data row containing the values inside the given slice to
	// the underlaying Postgres client. The column headers have to be written before
	// sending rows. Each item inside the slice represents a single column value.
	// The slice length needs to be the same length as the defined columns. Nil
	// values are encoded as NULL values.
	Row([]interface{}) error

	// Empty announces to the client a empty response and that no data rows should
	// be expected.
	Empty() error

	// Complete announces to the client that the command has been completed and
	// no further data should be expected.
	Complete(description string) error
}

// ErrColumnsDefined is thrown when columns already have been defined inside the
// given data writer.
var ErrColumnsDefined = errors.New("columns have already been defined")

// ErrUndefinedColumns is thrown when the columns inside the data writer have not
// yet been defined.
var ErrUndefinedColumns = errors.New("columns have not been defined")

// ErrClosedWriter is thrown when the data writer has been closed
var ErrClosedWriter = errors.New("closed writer")

type dataWriter struct {
	columns Columns
	ctx     context.Context
	client  *buffer.Writer
	closed  bool
}

func (writer *dataWriter) Define(columns Columns) error {
	if writer.closed {
		return ErrClosedWriter
	}

	if writer.columns != nil {
		return ErrColumnsDefined
	}

	writer.columns = columns
	return writer.columns.Define(writer.ctx, writer.client)
}

func (writer *dataWriter) Row(values []interface{}) error {
	if writer.closed {
		return ErrClosedWriter
	}

	if writer.columns == nil {
		return ErrUndefinedColumns
	}

	return writer.columns.Write(writer.ctx, writer.client, values)
}

func (writer *dataWriter) Empty() error {
	if writer.closed {
		return ErrClosedWriter
	}

	if writer.columns == nil {
		return ErrUndefinedColumns
	}

	defer writer.close()
	return EmptyQuery(writer.client)
}

func (writer *dataWriter) Complete(description string) error {
	if writer.closed {
		return ErrClosedWriter
	}

	defer writer.close()
	return CommandComplete(writer.client, description)
}

func (writer *dataWriter) close() {
	writer.closed = true
}

func (srv *Server) handleSimpleQuery(ctx context.Context, handle SimpleQueryFn, reader *buffer.Reader, writer *buffer.Writer) error {
	query, err := reader.GetString()
	if err != nil {
		return err
	}

	srv.logger.Debug("incoming query", zap.String("query", query))

	err = handle(ctx, query, &dataWriter{
		ctx:    ctx,
		client: writer,
	})

	if err != nil {
		return ErrorCode(writer, err)
	}

	return nil
}
