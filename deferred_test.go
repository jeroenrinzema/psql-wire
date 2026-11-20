package wire

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync/atomic"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jeroenrinzema/psql-wire/codes"
	psqlerr "github.com/jeroenrinzema/psql-wire/errors"
	"github.com/jeroenrinzema/psql-wire/pkg/mock"
	"github.com/jeroenrinzema/psql-wire/pkg/types"
	"github.com/neilotoole/slogt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var echoColumns = []mock.Column{{Name: "v", Oid: pgtype.TextOID}}

// deferredTestServer serves "echo" with deferred columns that are only known
// once the statement executes, "fail" with deferred columns whose execution
// fails before defining them, "static" with columns that are known up front
// but are defined anyway, and "insert" as a statement with side effects that
// are counted. "deferred insert" is like "insert" but with deferred columns,
// "undefined" is a buggy version of it that never defines its columns, and
// "copy" starts a COPY without defining its columns.
func deferredTestServer(t *testing.T, options ...OptionFn) (*mock.Client, *atomic.Int32) {
	t.Helper()

	var sideEffects atomic.Int32
	handler := func(ctx context.Context, query Query) (PreparedStatements, error) {
		switch query.Query {
		case "echo":
			return Prepared(NewStatement(func(ctx context.Context, writer DataWriter, parameters []Parameter) error {
				if err := writer.Define(Columns{{Name: "v", Oid: pgtype.TextOID}}); err != nil {
					return err
				}
				value := "none"
				if len(parameters) > 0 {
					value = string(parameters[0].Value())
				}
				if err := writer.Row([]any{value}); err != nil {
					return err
				}
				return writer.Complete("SELECT 1")
			}, WithParameters([]uint32{pgtype.TextOID}), WithDeferredColumns())), nil
		case "fail":
			return Prepared(NewStatement(func(ctx context.Context, writer DataWriter, parameters []Parameter) error {
				return errors.New("execution failed")
			}, WithDeferredColumns())), nil
		case "static":
			// The client might already have received the columns of a
			// statement that doesn't defer them, so they can't change.
			columns := Columns{{Name: "v", Oid: pgtype.TextOID}}
			return Prepared(NewStatement(func(ctx context.Context, writer DataWriter, parameters []Parameter) error {
				if err := writer.Define(Columns{{Name: "other", Oid: pgtype.TextOID}}); !errors.Is(err, errResultTypeChanged) {
					return fmt.Errorf("unexpected error when changing the columns: %v", err)
				}
				if err := writer.Define(columns); err != nil {
					return err
				}
				if err := writer.Row([]any{"static"}); err != nil {
					return err
				}
				return writer.Complete("SELECT 1")
			}, WithColumns(columns))), nil
		case "insert":
			return Prepared(NewStatement(func(ctx context.Context, writer DataWriter, parameters []Parameter) error {
				sideEffects.Add(1)
				return writer.Complete("INSERT 0 1")
			})), nil
		case "deferred insert":
			return Prepared(NewStatement(func(ctx context.Context, writer DataWriter, parameters []Parameter) error {
				sideEffects.Add(1)
				if err := writer.Define(nil); err != nil {
					return err
				}
				return writer.Complete("INSERT 0 1")
			}, WithDeferredColumns())), nil
		case "undefined":
			// A buggy handler that ignores the error of completing before
			// defining its columns.
			return Prepared(NewStatement(func(ctx context.Context, writer DataWriter, parameters []Parameter) error {
				sideEffects.Add(1)
				_ = writer.Complete("INSERT 0 1")
				return nil
			}, WithDeferredColumns())), nil
		case "dynamic":
			// The name of the column is the value of the parameter, so a
			// later execution with another value changes the columns.
			return Prepared(NewStatement(func(ctx context.Context, writer DataWriter, parameters []Parameter) error {
				name := string(parameters[0].Value())
				if err := writer.Define(Columns{{Name: name, Oid: pgtype.TextOID}}); err != nil {
					return err
				}
				if err := writer.Row([]any{name}); err != nil {
					return err
				}
				return writer.Complete("SELECT 1")
			}, WithParameters([]uint32{pgtype.TextOID}), WithDeferredColumns())), nil
		case "reuse":
			return Prepared(NewStatement(func(ctx context.Context, writer DataWriter, parameters []Parameter) error {
				columns := Columns{{Name: "v", Oid: pgtype.TextOID}}
				if err := writer.Define(columns); err != nil {
					return err
				}
				columns[0].Name = "reused"
				if err := writer.Row([]any{"reused"}); err != nil {
					return err
				}
				return writer.Complete("SELECT 1")
			}, WithDeferredColumns())), nil
		case "copy":
			return Prepared(NewStatement(func(ctx context.Context, writer DataWriter, parameters []Parameter) error {
				_, err := writer.CopyIn(TextFormat)
				return err
			}, WithDeferredColumns())), nil
		default:
			return nil, fmt.Errorf("unexpected query: %s", query.Query)
		}
	}

	server, err := NewServer(handler, append([]OptionFn{Logger(slogt.New(t))}, options...)...)
	require.NoError(t, err)
	address := TListenAndServe(t, server)

	conn, err := net.Dial("tcp", address.String())
	require.NoError(t, err)

	client := mock.NewClient(t, conn)
	client.Handshake(t)
	client.Authenticate(t)
	client.ReadyForQuery(t, types.ServerIdle)
	t.Cleanup(func() { client.Close(t) })

	return client, &sideEffects
}

// expectEcho expects the result of executing the "echo" statement.
func expectEcho(t *testing.T, client *mock.Client, value string) {
	t.Helper()
	assert.Equal(t, [][]byte{[]byte(value)}, client.ExpectDataRow(t))
	assert.Equal(t, "SELECT 1", client.ExpectCommandComplete(t))
}

// expectDeferredError expects the error for a Describe that can't wait for
// the statement to execute.
func expectDeferredError(t *testing.T, client *mock.Client) {
	t.Helper()
	client.ErrorCode(t, "42P18", "^the result columns of the statement are only known once it executes$")
}

func TestDeferredColumnsDescribePortal(t *testing.T) {
	t.Parallel()
	client, _ := deferredTestServer(t)

	client.Parse(t, "", "echo")
	client.Bind(t, "", "", "hello")
	client.DescribePortal(t, "")
	client.Execute(t, "", 0)
	client.Sync(t)

	client.ExpectMsg(t, types.ServerParseComplete)
	client.ExpectMsg(t, types.ServerBindComplete)
	assert.Equal(t, echoColumns, client.ExpectRowDescription(t))
	expectEcho(t, client, "hello")
	client.ReadyForQuery(t, types.ServerIdle)
}

func TestDeferredColumnsDescribeStatementBeforeBind(t *testing.T) {
	t.Parallel()
	client, _ := deferredTestServer(t)

	client.Parse(t, "", "echo")
	client.DescribeStatement(t, "")
	client.Bind(t, "", "", "hello")
	client.Execute(t, "", 0)
	client.Sync(t)

	client.ExpectMsg(t, types.ServerParseComplete)
	client.ExpectMsg(t, types.ServerParameterDescription)
	assert.Equal(t, echoColumns, client.ExpectRowDescription(t))
	client.ExpectMsg(t, types.ServerBindComplete)
	expectEcho(t, client, "hello")
	client.ReadyForQuery(t, types.ServerIdle)
}

func TestDeferredColumnsSyncBeforeExecute(t *testing.T) {
	t.Parallel()
	client, _ := deferredTestServer(t)

	client.Parse(t, "s", "echo")
	client.DescribeStatement(t, "s")
	client.Sync(t)

	client.ExpectMsg(t, types.ServerParseComplete)
	expectDeferredError(t, client)
	client.ReadyForQuery(t, types.ServerIdle)

	client.Bind(t, "", "s", "hello")
	client.Execute(t, "", 0)
	client.Sync(t)

	client.ExpectMsg(t, types.ServerBindComplete)
	expectEcho(t, client, "hello")
	client.ReadyForQuery(t, types.ServerIdle)
}

func TestDeferredColumnsDescribeStatementAndPortal(t *testing.T) {
	t.Parallel()
	client, _ := deferredTestServer(t)

	client.Parse(t, "", "echo")
	client.DescribeStatement(t, "")
	client.Bind(t, "", "", "hello")
	client.DescribePortal(t, "")
	client.Execute(t, "", 0)
	client.Sync(t)

	client.ExpectMsg(t, types.ServerParseComplete)
	client.ExpectMsg(t, types.ServerParameterDescription)
	assert.Equal(t, echoColumns, client.ExpectRowDescription(t))
	client.ExpectMsg(t, types.ServerBindComplete)
	assert.Equal(t, echoColumns, client.ExpectRowDescription(t))
	expectEcho(t, client, "hello")
	client.ReadyForQuery(t, types.ServerIdle)
}

func TestDeferredColumnsDescribeTwoPortals(t *testing.T) {
	t.Parallel()
	client, _ := deferredTestServer(t)

	// Executing portal a can't answer the Describe of portal b, so it can't
	// run either.
	client.Parse(t, "s", "echo")
	client.Bind(t, "a", "s", "a")
	client.Bind(t, "b", "s", "b")
	client.DescribePortal(t, "a")
	client.DescribePortal(t, "b")
	client.Execute(t, "a", 0)
	client.Sync(t)

	client.ExpectMsg(t, types.ServerParseComplete)
	client.ExpectMsg(t, types.ServerBindComplete)
	client.ExpectMsg(t, types.ServerBindComplete)
	expectDeferredError(t, client)
	client.ReadyForQuery(t, types.ServerIdle)
}

func TestDeferredColumnsFlushBeforeExecute(t *testing.T) {
	t.Parallel()
	client, _ := deferredTestServer(t)

	client.Parse(t, "", "echo")
	client.Bind(t, "", "", "hello")
	client.DescribePortal(t, "")
	client.Flush(t)
	client.Execute(t, "", 0)
	client.Sync(t)

	client.ExpectMsg(t, types.ServerParseComplete)
	client.ExpectMsg(t, types.ServerBindComplete)
	expectDeferredError(t, client)
	client.ReadyForQuery(t, types.ServerIdle)
}

func TestDeferredColumnsSkipLaterSideEffects(t *testing.T) {
	t.Parallel()
	client, sideEffects := deferredTestServer(t)

	client.Parse(t, "s", "echo")
	client.DescribeStatement(t, "s")
	client.Parse(t, "", "insert")
	client.Bind(t, "", "")
	client.Execute(t, "", 0)
	client.Sync(t)

	client.ExpectMsg(t, types.ServerParseComplete)
	expectDeferredError(t, client)
	client.ReadyForQuery(t, types.ServerIdle)
	assert.Equal(t, int32(0), sideEffects.Load())

	// An Execute of another portal can't run before the Describe is answered
	// either, even when the statement was parsed before.
	client.Parse(t, "i", "insert")
	client.Parse(t, "s2", "echo")
	client.Bind(t, "pi", "i")
	client.DescribeStatement(t, "s2")
	client.Execute(t, "pi", 0)
	client.Sync(t)

	client.ExpectMsg(t, types.ServerParseComplete)
	client.ExpectMsg(t, types.ServerParseComplete)
	client.ExpectMsg(t, types.ServerBindComplete)
	expectDeferredError(t, client)
	client.ReadyForQuery(t, types.ServerIdle)
	assert.Equal(t, int32(0), sideEffects.Load())
}

func TestDeferredColumnsExecutionError(t *testing.T) {
	t.Parallel()
	client, _ := deferredTestServer(t)

	client.Parse(t, "", "fail")
	client.Bind(t, "", "")
	client.DescribePortal(t, "")
	client.Execute(t, "", 0)
	client.Sync(t)

	client.ExpectMsg(t, types.ServerParseComplete)
	client.ExpectMsg(t, types.ServerBindComplete)
	client.ErrorCode(t, "XXUUU", "^execution failed$")
	client.ReadyForQuery(t, types.ServerIdle)
}

func TestDeferredColumnsExecuteUnknownPortal(t *testing.T) {
	t.Parallel()
	client, _ := deferredTestServer(t)

	client.Parse(t, "s", "echo")
	client.DescribeStatement(t, "s")
	client.Execute(t, "missing", 0)
	client.Sync(t)

	client.ExpectMsg(t, types.ServerParseComplete)
	client.ErrorCode(t, "34000", `^portal "missing" does not exist$`)
	client.ReadyForQuery(t, types.ServerIdle)
}

func TestDeferredColumnsMultiplePortals(t *testing.T) {
	t.Parallel()
	client, _ := deferredTestServer(t)

	client.Parse(t, "s", "echo")
	client.Bind(t, "a", "s", "a")
	client.DescribePortal(t, "a")
	client.Execute(t, "a", 0)
	client.Bind(t, "b", "s", "b")
	client.DescribePortal(t, "b")
	client.Execute(t, "b", 0)
	client.Sync(t)

	client.ExpectMsg(t, types.ServerParseComplete)
	client.ExpectMsg(t, types.ServerBindComplete)
	assert.Equal(t, echoColumns, client.ExpectRowDescription(t))
	expectEcho(t, client, "a")
	client.ExpectMsg(t, types.ServerBindComplete)
	assert.Equal(t, echoColumns, client.ExpectRowDescription(t))
	expectEcho(t, client, "b")
	client.ReadyForQuery(t, types.ServerIdle)
}

func TestDeferredColumnsSimpleQuery(t *testing.T) {
	t.Parallel()
	client, _ := deferredTestServer(t)

	client.SimpleQuery(t, "echo")

	assert.Equal(t, echoColumns, client.ExpectRowDescription(t))
	expectEcho(t, client, "none")
	client.ReadyForQuery(t, types.ServerIdle)

	client.SimpleQuery(t, "static")

	assert.Equal(t, echoColumns, client.ExpectRowDescription(t))
	expectEcho(t, client, "static")
	client.ReadyForQuery(t, types.ServerIdle)
}

func TestDeferredColumnsParallelPipeline(t *testing.T) {
	t.Parallel()
	client, _ := deferredTestServer(t, ParallelPipeline(ParallelPipelineConfig{Enabled: true}))

	client.Parse(t, "", "echo")
	client.Bind(t, "", "", "hello")
	client.DescribePortal(t, "")
	client.Execute(t, "", 0)
	client.Sync(t)

	client.ErrorCode(t, "0A000", "^statements whose result columns are only known once they execute are not supported with the parallel pipeline$")
	client.ReadyForQuery(t, types.ServerIdle)
}

func TestDeferredColumnsCustomError(t *testing.T) {
	t.Parallel()
	client, _ := deferredTestServer(t, DeferredColumnsError(errors.New("custom deferred error")))

	client.Parse(t, "s", "echo")
	client.DescribeStatement(t, "s")
	client.Sync(t)

	client.ExpectMsg(t, types.ServerParseComplete)
	client.ErrorCode(t, "42P18", "^custom deferred error$")
	client.ReadyForQuery(t, types.ServerIdle)
}

func TestDeferredColumnsCustomErrorWithCode(t *testing.T) {
	t.Parallel()
	client, _ := deferredTestServer(t, DeferredColumnsError(psqlerr.WithCode(errors.New("custom deferred error"), codes.FeatureNotSupported)))

	client.Parse(t, "s", "echo")
	client.DescribeStatement(t, "s")
	client.Sync(t)

	client.ExpectMsg(t, types.ServerParseComplete)
	client.ErrorCode(t, "0A000", "^custom deferred error$")
	client.ReadyForQuery(t, types.ServerIdle)
}

// The statement keeps its own copy of the columns, so a handler can reuse the
// slice it passed to Define.
func TestDeferredColumnsHandlerReusesColumns(t *testing.T) {
	t.Parallel()
	client, _ := deferredTestServer(t)

	client.Parse(t, "s", "reuse")
	client.Bind(t, "", "s")
	client.Execute(t, "", 0)
	client.DescribeStatement(t, "s")
	client.Sync(t)

	client.ExpectMsg(t, types.ServerParseComplete)
	client.ExpectMsg(t, types.ServerBindComplete)
	expectEcho(t, client, "reused")
	client.ExpectMsg(t, types.ServerParameterDescription)
	assert.Equal(t, echoColumns, client.ExpectRowDescription(t))
	client.ReadyForQuery(t, types.ServerIdle)
}

func TestDeferredColumnsNoData(t *testing.T) {
	t.Parallel()
	client, sideEffects := deferredTestServer(t)

	client.Parse(t, "", "deferred insert")
	client.Bind(t, "", "")
	client.DescribePortal(t, "")
	client.Execute(t, "", 0)
	client.Sync(t)

	client.ExpectMsg(t, types.ServerParseComplete)
	client.ExpectMsg(t, types.ServerBindComplete)
	client.ExpectMsg(t, types.ServerNoData)
	assert.Equal(t, "INSERT 0 1", client.ExpectCommandComplete(t))
	client.ReadyForQuery(t, types.ServerIdle)
	assert.Equal(t, int32(1), sideEffects.Load())

	client.SimpleQuery(t, "deferred insert")

	assert.Equal(t, "INSERT 0 1", client.ExpectCommandComplete(t))
	client.ReadyForQuery(t, types.ServerIdle)
}

// A handler that finishes without defining its columns already had its side
// effects, so failing the Describe would make the client believe that the
// statement was skipped. Instead the connection is closed.
func TestDeferredColumnsNotDefined(t *testing.T) {
	t.Parallel()
	client, sideEffects := deferredTestServer(t)

	client.Parse(t, "", "undefined")
	client.Bind(t, "", "")
	client.DescribePortal(t, "")
	client.Execute(t, "", 0)
	client.Sync(t)

	client.ExpectMsg(t, types.ServerParseComplete)
	client.ExpectMsg(t, types.ServerBindComplete)
	client.ErrorCode(t, "XX000", "^the handler of a statement with deferred result columns finished without calling DataWriter.Define$")
	client.ExpectClosed(t)
	assert.Equal(t, int32(1), sideEffects.Load())
}

func TestDeferredColumnsCopyBeforeDefine(t *testing.T) {
	t.Parallel()
	client, _ := deferredTestServer(t)

	client.Parse(t, "", "copy")
	client.Bind(t, "", "")
	client.DescribePortal(t, "")
	client.Execute(t, "", 0)
	client.Sync(t)

	client.ExpectMsg(t, types.ServerParseComplete)
	client.ExpectMsg(t, types.ServerBindComplete)
	client.Error(t, "^DataWriter.Define has to be called before writing the result of a statement with deferred result columns$")
	client.ReadyForQuery(t, types.ServerIdle)
}

// A simple Query after a held Describe is skipped until the Sync, just like
// any other message after an extended query message that fails.
func TestDeferredColumnsSimpleQueryWhileHeld(t *testing.T) {
	t.Parallel()
	client, sideEffects := deferredTestServer(t)

	client.Parse(t, "", "echo")
	client.DescribeStatement(t, "")
	client.SimpleQuery(t, "insert")
	client.Sync(t)

	client.ExpectMsg(t, types.ServerParseComplete)
	expectDeferredError(t, client)
	client.ReadyForQuery(t, types.ServerIdle)
	assert.Equal(t, int32(0), sideEffects.Load())
}

func TestDeferredColumnsDescribeAfterExecute(t *testing.T) {
	t.Parallel()
	client, _ := deferredTestServer(t)

	client.Parse(t, "s", "echo")
	client.Bind(t, "p", "s", "hello")
	client.Execute(t, "p", 0)
	client.DescribePortal(t, "p")
	client.DescribeStatement(t, "s")
	client.Execute(t, "p", 0)
	client.Sync(t)

	client.ExpectMsg(t, types.ServerParseComplete)
	client.ExpectMsg(t, types.ServerBindComplete)
	expectEcho(t, client, "hello")
	// The statement already defined its columns, so both can be described
	// right away.
	assert.Equal(t, echoColumns, client.ExpectRowDescription(t))
	client.ExpectMsg(t, types.ServerParameterDescription)
	assert.Equal(t, echoColumns, client.ExpectRowDescription(t))
	assert.Equal(t, "SELECT 1", client.ExpectCommandComplete(t))
	client.ReadyForQuery(t, types.ServerIdle)
}

func TestDeferredColumnsCannotChange(t *testing.T) {
	t.Parallel()
	client, _ := deferredTestServer(t)

	client.Parse(t, "s", "dynamic")
	client.Bind(t, "", "s", "a")
	client.DescribePortal(t, "")
	client.Execute(t, "", 0)
	client.Bind(t, "", "s", "a")
	client.Execute(t, "", 0)
	client.Bind(t, "", "s", "b")
	client.Execute(t, "", 0)
	client.Sync(t)

	columnsA := []mock.Column{{Name: "a", Oid: pgtype.TextOID}}
	client.ExpectMsg(t, types.ServerParseComplete)
	client.ExpectMsg(t, types.ServerBindComplete)
	assert.Equal(t, columnsA, client.ExpectRowDescription(t))
	expectEcho(t, client, "a")
	client.ExpectMsg(t, types.ServerBindComplete)
	expectEcho(t, client, "a")
	client.ExpectMsg(t, types.ServerBindComplete)
	// Clients like pgjdbc detect this error by its code and routine, to
	// prepare the statement again.
	fields := client.ErrorFields(t)
	assert.Equal(t, "0A000", fields['C'])
	assert.Equal(t, "cached plan must not change result type", fields['M'])
	assert.Equal(t, "RevalidateCachedQuery", fields['R'])
	assert.Equal(t, "plancache.c", fields['F'])
	assert.Equal(t, "886", fields['L'])
	client.ReadyForQuery(t, types.ServerIdle)

	// Preparing the statement again allows the columns to change.
	client.Parse(t, "s", "dynamic")
	client.Bind(t, "", "s", "b")
	client.DescribePortal(t, "")
	client.Execute(t, "", 0)
	client.Sync(t)

	client.ExpectMsg(t, types.ServerParseComplete)
	client.ExpectMsg(t, types.ServerBindComplete)
	assert.Equal(t, []mock.Column{{Name: "b", Oid: pgtype.TextOID}}, client.ExpectRowDescription(t))
	expectEcho(t, client, "b")
	client.ReadyForQuery(t, types.ServerIdle)
}
