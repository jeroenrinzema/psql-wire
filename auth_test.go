package wire

import (
	"bytes"
	"context"
	"strconv"
	"testing"

	"github.com/jeroenrinzema/psql-wire/buffer"
	"go.uber.org/zap"
)

func TestDefaultHandleAuth(t *testing.T) {
	input := bytes.NewBuffer([]byte{})
	sink := bytes.NewBuffer([]byte{})

	ctx := context.Background()
	reader := buffer.NewReader(input)
	writer := buffer.NewWriter(sink)

	server := &Server{logger: zap.NewNop()}
	err := server.HandleAuth(ctx, reader, writer)
	if err != nil {
		t.Fatal(err)
	}

	result := buffer.NewReader(sink)
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
	validate := func(username, password string) (bool, error) {
		return true, nil
	}

	input := bytes.NewBuffer([]byte{})
	sink := bytes.NewBuffer([]byte{})

	ctx := context.Background()
	reader := buffer.NewReader(input)
	writer := buffer.NewWriter(sink)

	server := &Server{logger: zap.NewNop(), Auth: ClearTextPassword(validate)}
	server.HandleAuth(ctx, reader, writer)
}
