package wire

import (
	"bytes"
	"context"
	"fmt"
	"strconv"
	"testing"

	"github.com/jeroenrinzema/psql-wire/pkg/buffer"
	"github.com/jeroenrinzema/psql-wire/pkg/types"
	"github.com/neilotoole/slogt"
	"github.com/stretchr/testify/require"
)

func TestDefaultHandleAuth(t *testing.T) {
	input := bytes.NewBuffer([]byte{})
	sink := bytes.NewBuffer([]byte{})

	ctx := context.Background()
	reader := buffer.NewReader(slogt.New(t), input, buffer.DefaultBufferSize)
	writer := buffer.NewWriter(slogt.New(t), sink)

	server := &Server{logger: slogt.New(t)}
	_, err := server.handleAuth(ctx, reader, writer)
	require.NoError(t, err)

	result := buffer.NewReader(slogt.New(t), sink, buffer.DefaultBufferSize)
	ty, ln, err := result.ReadTypedMsg()
	require.NoError(t, err)

	if ln == 0 {
		t.Error("unexpected length, expected typed message length to be greater then 0")
	}

	if ty != 'R' {
		t.Errorf("unexpected message type %s, expected 'R'", strconv.QuoteRune(rune(ty)))
	}

	status, err := result.GetUint32()
	require.NoError(t, err)

	if authType(status) != authOK {
		t.Errorf("unexpected auth status %d, expected OK", status)
	}
}

func TestClearTextPassword(t *testing.T) {
	expected := "password"

	input := bytes.NewBuffer([]byte{})
	incoming := buffer.NewWriter(slogt.New(t), input)

	// NOTE: we could reuse the server buffered writer to write client messages
	incoming.Start(types.ServerMessage(types.ClientPassword))
	incoming.AddString(expected)
	incoming.AddNullTerminate()
	incoming.End() //nolint:errcheck

	validate := func(ctx context.Context, database, username, password string) (context.Context, bool, error) {
		if password != expected {
			return ctx, false, fmt.Errorf("unexpected password: %s", password)
		}

		return ctx, true, nil
	}

	sink := bytes.NewBuffer([]byte{})

	ctx := context.Background()
	reader := buffer.NewReader(slogt.New(t), input, buffer.DefaultBufferSize)
	writer := buffer.NewWriter(slogt.New(t), sink)

	server := &Server{logger: slogt.New(t), Auth: ClearTextPassword(validate)}
	out, err := server.handleAuth(ctx, reader, writer)
	require.NoError(t, err)
	require.Equal(t, ctx, out)
}
