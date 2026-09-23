package wire

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jeroenrinzema/psql-wire/pkg/buffer"
	"github.com/jeroenrinzema/psql-wire/pkg/types"
	"github.com/stretchr/testify/require"
)

type rowPackets struct {
	bytes.Buffer
	packets [][]byte
	err     error
}

func (out *rowPackets) Write(p []byte) (int, error) {
	out.packets = append(out.packets, bytes.Clone(p))
	if out.err != nil {
		return 0, out.err
	}
	return out.Buffer.Write(p)
}

func newRowsWriter(columns Columns, formats []FormatCode, output io.Writer) *dataWriter {
	return &dataWriter{
		ctx:     setTypeInfo(context.Background(), pgtype.NewMap()),
		columns: columns, formats: formats,
		client: buffer.NewWriter(slog.New(slog.NewTextHandler(io.Discard, nil)), output),
		yield:  func(struct{}) bool { return true }, tag: new(string), batchable: true,
	}
}

func rowFrames(t *testing.T, data []byte) [][]byte {
	t.Helper()
	var frames [][]byte
	for len(data) > 0 {
		require.GreaterOrEqual(t, len(data), 5)
		n := int(binary.BigEndian.Uint32(data[1:5])) + 1
		require.GreaterOrEqual(t, n, 5)
		require.LessOrEqual(t, n, len(data))
		frames = append(frames, data[:n])
		data = data[n:]
	}
	return frames
}

func TestRowsMatchRowAndColumnsWrite(t *testing.T) {
	stamp := time.Date(2026, 9, 16, 1, 2, 3, 456000000, time.UTC)
	columns := Columns{
		{Oid: pgtype.TextOID}, {Oid: pgtype.ByteaOID}, {Oid: pgtype.Int8OID}, {Oid: pgtype.BoolOID},
		{Oid: pgtype.Float8OID}, {Oid: pgtype.JSONBOID}, {Oid: pgtype.UUIDOID},
		{Oid: pgtype.TimestamptzOID}, {Oid: pgtype.NumericOID}, {Oid: pgtype.TextArrayOID},
		{Oid: pgtype.Int2OID}, {Oid: pgtype.Int4OID}, {Oid: pgtype.Float4OID},
		{Oid: pgtype.TimestampOID}, {Oid: pgtype.DateOID}, {Oid: pgtype.TimeOID},
		{Oid: pgtype.IntervalOID}, {Oid: pgtype.JSONOID}, {Oid: pgtype.VarcharOID},
	}
	for _, formats := range [][]FormatCode{nil, {TextFormat}, {BinaryFormat}, {TextFormat, BinaryFormat}, {BinaryFormat, TextFormat, BinaryFormat, TextFormat, BinaryFormat, TextFormat, BinaryFormat, TextFormat, BinaryFormat, TextFormat, BinaryFormat, TextFormat, BinaryFormat, TextFormat, BinaryFormat, TextFormat, BinaryFormat, TextFormat, BinaryFormat, 7}} {
		t.Run(fmt.Sprint(formats), func(t *testing.T) {
			var rows [][]any
			for _, size := range []int{0, 1, 32, 4096, 17, 64 << 10, (64 << 10) + 1, 5, 0} {
				text := strings.Repeat("x", size)
				rows = append(rows, []any{
					text, bytes.Repeat([]byte{0, 255}, size/2), int64(-size), size%2 == 0,
					float64(size) / 3, map[string]any{"text": text}, pgtype.UUID{Bytes: [16]byte{1, 2, 3, 255}, Valid: true},
					stamp, pgtype.Numeric{Int: big.NewInt(int64(size)), Exp: -2, Valid: true}, []string{text, "", "tail"},
					int16(-123), int32(987654), float32(0.5), stamp, stamp, pgtype.Time{Microseconds: 123456, Valid: true},
					pgtype.Interval{Microseconds: 123456, Days: 3, Months: 1, Valid: true}, map[string]any{"n": size}, text,
				}, make([]any, len(columns)))
			}
			rows = append(rows, []any{
				pgtype.Text{}, []byte(nil), pgtype.Int8{}, pgtype.Bool{}, pgtype.Float8{}, []byte(nil), pgtype.UUID{},
				pgtype.Timestamptz{}, pgtype.Numeric{}, []string(nil), pgtype.Int2{}, pgtype.Int4{}, pgtype.Float4{},
				pgtype.Timestamp{}, pgtype.Date{}, pgtype.Time{}, pgtype.Interval{}, []byte(nil), sql.NullString{},
			})
			var sequential, batch, allocating bytes.Buffer
			ref := newRowsWriter(columns, formats, &sequential)
			writer := newRowsWriter(columns, formats, &batch)
			public := newRowsWriter(columns, formats, &allocating)
			for _, row := range rows {
				require.NoError(t, ref.Row(row))
				require.NoError(t, columns.Write(public.ctx, formats, public.client, row))
			}
			require.NoError(t, WriteRows(writer, rows))
			require.Equal(t, allocating.Bytes(), sequential.Bytes(), "scratch preserves the public encoding path")
			require.Equal(t, sequential.Bytes(), batch.Bytes(), "batching preserves every byte")
			require.Equal(t, ref.Written(), writer.Written())
			frames := rowFrames(t, batch.Bytes())
			require.Len(t, frames, len(rows))
			for i, frame := range frames {
				require.Equal(t, byte(types.ServerDataRow), frame[0])
				var decoded pgproto3.DataRow
				require.NoError(t, decoded.Decode(frame[5:]))
				require.Len(t, decoded.Values, len(columns))
				if i%2 == 1 {
					for _, value := range decoded.Values {
						require.Nil(t, value)
					}
				}
			}
		})
	}
}

func TestRowsMatchColumnsWriteWhenValueTypesChange(t *testing.T) {
	columns := Columns{{Oid: pgtype.Int8OID}}
	value := int64(4)
	rows := [][]any{
		{int64(1)},
		{int64(2)},
		{int32(3)},
		{"4"},
		{&value},
		{(*int64)(nil)},
		{nil},
		{int64(5)},
		{int64(6)},
		{int64(7)},
	}

	var sequential, batch bytes.Buffer
	reference := newRowsWriter(columns, nil, &sequential)
	writer := newRowsWriter(columns, nil, &batch)
	for _, row := range rows {
		require.NoError(t, columns.Write(reference.ctx, nil, reference.client, row))
	}
	require.NoError(t, writer.Rows(rows))
	require.Equal(t, sequential.Bytes(), batch.Bytes())
}

type rowErrorText struct{ err error }

func (value rowErrorText) TextValue() (pgtype.Text, error) { return pgtype.Text{}, value.err }

func TestRowsErrorFlushesCompletedRowsAndDisablesBatch(t *testing.T) {
	encodeErr := errors.New("encode failed")
	for _, test := range []struct {
		name string
		bad  []any
	}{
		{"column_count", []any{"one"}},
		{"encode", []any{"one", rowErrorText{encodeErr}}},
		{"unsupported", []any{"one", make(chan int)}},
	} {
		t.Run(test.name, func(t *testing.T) {
			columns := Columns{{Oid: pgtype.TextOID}, {Oid: pgtype.TextOID}}
			out, refOut := &rowPackets{}, &rowPackets{}
			writer, ref := newRowsWriter(columns, nil, out), newRowsWriter(columns, nil, refOut)
			first := []any{"good", "row"}
			require.NoError(t, ref.Row(first))
			refErr := ref.Row(test.bad)
			err := writer.Rows([][]any{first, test.bad, {"not", "written"}})
			require.EqualError(t, err, refErr.Error())
			require.Equal(t, refOut.Bytes(), out.Bytes(), "only completed rows flush")
			require.Equal(t, ref.Written(), writer.Written())
			require.Equal(t, ref.client.Bytes(), writer.client.Bytes(), "partial frame is not flushed")
			// Without deferred EndBatch, this CommandComplete would stay in the
			// batch forever, leaving the client waiting for query completion.
			require.NoError(t, writer.Complete("SELECT 1"))
			require.NoError(t, ref.Complete("SELECT 1"))
			require.Equal(t, refOut.Bytes(), out.Bytes())
			require.Len(t, out.packets, 2)
			require.Equal(t, byte(types.ServerCommandComplete), out.packets[1][0])
		})
	}
}

func TestRowsFlushChunksAndYield(t *testing.T) {
	columns, rows := benchmarkRows()
	out := &rowPackets{}
	writer := newRowsWriter(columns, nil, out)
	yields := 0
	writer.yield = func(struct{}) bool {
		yields++
		require.Len(t, out.packets, yields, "one checkpoint per flushed chunk")
		return true
	}
	require.NoError(t, writer.Rows(rows))
	require.Equal(t, uint32(len(rows)), writer.Written())
	require.LessOrEqual(t, len(out.packets), 30)
	require.Equal(t, len(out.packets), yields)
	for i, packet := range out.packets {
		frames := rowFrames(t, packet)
		// Flush at the first complete frame reaching the threshold.
		require.Less(t, len(packet)-len(frames[len(frames)-1]), 32<<10)
		if i < len(out.packets)-1 {
			require.GreaterOrEqual(t, len(packet), 32<<10)
		}
	}
}

func TestRowsCancellationAndFlushErrors(t *testing.T) {
	columns, rows := benchmarkRows()
	for _, count := range []int{1, len(rows)} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			out := &rowPackets{}
			writer := newRowsWriter(columns, nil, out)
			writer.yield = func(struct{}) bool { return false }
			require.ErrorIs(t, writer.Rows(rows[:count]), ErrSuspendedHandlerClosed)
			require.Len(t, out.packets, 1)
			require.NoError(t, commandComplete(writer.client, "SELECT"))
			require.Len(t, out.packets, 2, "cancellation disables batching")
		})
		t.Run(fmt.Sprintf("output_%d", count), func(t *testing.T) {
			outputErr := errors.New("output failed")
			out := &rowPackets{err: outputErr}
			writer := newRowsWriter(columns, nil, out)
			writer.yield = func(struct{}) bool { t.Fatal("failed flush must not yield"); return false }
			require.ErrorIs(t, writer.Rows(rows[:count]), outputErr)
			require.Len(t, out.packets, 1, "failed flush must not be retried by EndBatch")
			out.err = nil
			require.NoError(t, commandComplete(writer.client, "SELECT"))
			require.Len(t, out.packets, 2, "flush failure disables batching")
		})
	}
	t.Run("context", func(t *testing.T) {
		out := &rowPackets{}
		writer := newRowsWriter(Columns{{Oid: pgtype.TextOID}, {Oid: pgtype.TextOID}}, nil, out)
		ctx, cancel := context.WithCancel(writer.ctx)
		defer cancel()
		writer.ctx = ctx
		require.ErrorIs(t, writer.Rows([][]any{{cancelText{cancel}, "finish this row"}, {"after", "cancellation"}}), context.Canceled)
		require.Equal(t, uint32(1), writer.Written())
		require.Len(t, rowFrames(t, out.Bytes()), 1)
	})
}

type cancelText struct{ cancel context.CancelFunc }

func (v cancelText) TextValue() (pgtype.Text, error) {
	v.cancel()
	return pgtype.Text{String: "cancelled during encoding", Valid: true}, nil
}

func TestRowsScratchBoundAndEmpty(t *testing.T) {
	writer := newRowsWriter(Columns{{Oid: pgtype.TextOID}}, nil, io.Discard)
	require.NoError(t, writer.Rows([][]any{{strings.Repeat("x", 4096)}, {nil}}))
	require.GreaterOrEqual(t, cap(writer.encodeScratch), 4096)
	require.NoError(t, writer.Rows([][]any{{strings.Repeat("x", maxEncodeScratchCapacity+1)}}))
	require.Nil(t, writer.encodeScratch)
	require.NoError(t, writer.Rows([][]any{{"small"}}))
	require.LessOrEqual(t, cap(writer.encodeScratch), maxEncodeScratchCapacity)
	require.NoError(t, writer.Rows(nil))
	require.NoError(t, writer.Complete("SELECT 4"))
	require.Nil(t, writer.encodeScratch)
	require.ErrorIs(t, writer.Rows(nil), ErrClosedWriter)

	writer = newRowsWriter(nil, nil, io.Discard)
	require.NoError(t, writer.Rows([][]any{nil, {}}))
	require.Equal(t, uint32(2), writer.Written())
}

// Embedding only DataWriter hides Rows, modeling an existing external writer.
type rowOnlyWriter struct{ DataWriter }

func TestWriteRowsFallbackStopsAtError(t *testing.T) {
	for _, optionalInterface := range []bool{false, true} {
		t.Run(fmt.Sprint(optionalInterface), func(t *testing.T) {
			out := &rowPackets{}
			writer := newRowsWriter(Columns{{Oid: pgtype.TextOID}}, nil, out)
			writer.batchable = false
			var w DataWriter = writer
			if !optionalInterface {
				w = rowOnlyWriter{writer}
			}
			err := WriteRows(w, [][]any{{"first"}, {}, {"never written"}})
			require.ErrorContains(t, err, "unexpected columns")
			require.Len(t, out.packets, 1)
			require.Equal(t, uint32(1), writer.Written())
		})
	}
}

func TestRowsPortalExecuteLimits(t *testing.T) {
	for _, limits := range [][]Limit{{1, 1, NoLimit}, {2, 1, 1}, {NoLimit}} {
		t.Run(fmt.Sprint(limits), func(t *testing.T) {
			out := &rowPackets{}
			writer := newRowsWriter(Columns{{Oid: pgtype.TextOID}}, nil, out)
			calls := 0
			var dw *dataWriter
			portal := &Portal{statement: &Statement{
				columns: writer.columns,
				fn: func(_ context.Context, w DataWriter, _ []Parameter) error {
					calls++
					dw = w.(*dataWriter)
					require.Equal(t, limits[0] == NoLimit, dw.batchable)
					if err := WriteRows(w, [][]any{{"one"}, {"two"}, {"three"}}); err != nil {
						return err
					}
					return w.Complete("SELECT 3")
				},
			}}
			defer portal.Close()
			written := 0
			for _, limit := range limits {
				before := len(out.packets)
				require.NoError(t, portal.execute(writer.ctx, limit, nil, writer.client))
				frames := rowFrames(t, bytes.Join(out.packets[before:], nil))
				n := 3 - written
				if limit != NoLimit && n >= int(limit) {
					n = int(limit)
					require.False(t, portal.done)
					require.Equal(t, byte(types.ServerPortalSuspended), frames[len(frames)-1][0])
				} else {
					require.True(t, portal.done)
					require.Equal(t, byte(types.ServerCommandComplete), frames[len(frames)-1][0])
				}
				require.Len(t, frames, n+1)
				for _, frame := range frames[:n] {
					require.Equal(t, byte(types.ServerDataRow), frame[0])
				}
				if limits[0] != NoLimit {
					require.Equal(t, n+1, len(out.packets)-before, "limited portals write one frame per call")
				}
				written += n
				require.Equal(t, uint32(written), dw.Written())
			}
			require.True(t, portal.done)
			require.Equal(t, 1, calls, "the handler survives re-execution")
			before := len(out.packets)
			require.NoError(t, portal.execute(writer.ctx, 1, nil, writer.client))
			require.Len(t, out.packets, before+1)
			require.Equal(t, byte(types.ServerCommandComplete), out.packets[before][0], "completed portals cannot emit more rows")
		})
	}
}

type countingContext struct {
	context.Context
	values int
}

func (ctx *countingContext) Value(key any) any {
	ctx.values++
	return ctx.Context.Value(key)
}

func TestRowsResolvesTypeMapOnce(t *testing.T) {
	columns, rows := benchmarkRows()
	writer := newRowsWriter(columns, nil, io.Discard)
	ctx := &countingContext{Context: writer.ctx}
	writer.ctx = ctx
	require.NoError(t, writer.Rows(rows))
	require.Equal(t, 1, ctx.values)
}

func TestRowsMissingContextState(t *testing.T) {
	for _, name := range []string{"type_map", "cancelled", "deadline"} {
		t.Run(name, func(t *testing.T) {
			out := &rowPackets{}
			writer := newRowsWriter(Columns{{Oid: pgtype.TextOID}}, nil, out)
			ctx := context.Background()
			var want error
			switch name {
			case "cancelled":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
				want = context.Canceled
			case "deadline":
				var cancel context.CancelFunc
				ctx, cancel = context.WithDeadline(ctx, time.Unix(0, 0))
				defer cancel()
				want = context.DeadlineExceeded
			}
			writer.ctx = ctx
			err := writer.Rows([][]any{{"value"}})
			if want != nil {
				require.ErrorIs(t, err, want)
			} else {
				require.ErrorContains(t, err, "postgres connection info")
			}
			require.Empty(t, out.packets)
			require.Zero(t, writer.Written())
			require.NoError(t, writer.Complete("SELECT 0"))
			require.Len(t, out.packets, 1)
		})
	}
}
