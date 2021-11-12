package wire

import (
	"bytes"
	"context"
	"fmt"
	"strconv"
	"testing"

	"github.com/jeroenrinzema/psql-wire/internal/buffer"
	"github.com/jeroenrinzema/psql-wire/internal/types"
	"go.uber.org/zap"
)

func TestDefaultHandleAuth(t *testing.T) {
	input := bytes.NewBuffer([]byte{})
	sink := bytes.NewBuffer([]byte{})

	ctx := context.Background()
	reader := buffer.NewReader(input, buffer.DefaultBufferSize)
	writer := buffer.NewWriter(sink)

	server := &Server{logger: zap.NewNop()}
	err := server.handleAuth(ctx, reader, writer)
	if err != nil {
		t.Fatal(err)
	}

	result := buffer.NewReader(sink, buffer.DefaultBufferSize)
	ty, ln, err := result.ReadTypedMsg()
	if err != nil {
		t.Fatal(err)
	}

	if ln == 0 {
		t.Error("unexpected length, expected typed message length to be greater then 0")
	}

	if ty != 'R' {
		t.Errorf("unexpected message type %s, expected 'R'", strconv.QuoteRune(rune(ty)))
	}

	status, err := result.GetUint32()
	if err != nil {
		t.Fatal(err)
	}

	if authType(status) != authOK {
		t.Errorf("unexpected auth status %d, expected OK", status)
	}
}

func TestClearTextPassword(t *testing.T) {
	expected := "password"

	input := bytes.NewBuffer([]byte{})
	incoming := buffer.NewWriter(input)

	// NOTE(Jeroen): we could reuse the server buffered writer to write client messages
	incoming.Start(types.ServerMessage(types.ClientPassword))
	incoming.AddString(expected)
	incoming.AddNullTerminate()
	incoming.End() //nolint:errcheck

	validate := func(username, password string) (bool, error) {
		if password != expected {
			return false, fmt.Errorf("unexpected password: %s", password)
		}

		return true, nil
	}

	sink := bytes.NewBuffer([]byte{})

	ctx := context.Background()
	reader := buffer.NewReader(input, buffer.DefaultBufferSize)
	writer := buffer.NewWriter(sink)

	server := &Server{logger: zap.NewNop(), Auth: ClearTextPassword(validate)}
	err := server.handleAuth(ctx, reader, writer)
	if err != nil {
		t.Error("unexpected error:", err)
	}
}
