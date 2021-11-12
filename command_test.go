package wire

import (
	"net"
	"testing"

	"github.com/jeroenrinzema/psql-wire/internal/buffer"
	"github.com/jeroenrinzema/psql-wire/internal/mock"
	"github.com/jeroenrinzema/psql-wire/internal/types"
)

func TestMessageSizeExceeded(t *testing.T) {
	server, err := NewServer()
	if err != nil {
		t.Fatal(err)
	}

	address := TListenAndServe(t, server)
	conn, err := net.Dial("tcp", address.String())
	if err != nil {
		t.Fatal(err)
	}

	client := mock.NewClient(conn)
	client.Handshake(t)
	client.Authenticate(t)
	client.ReadyForQuery(t)

	// NOTE(Jeroen): attempt to send a message twice the max buffer size
	size := uint32(buffer.DefaultBufferSize * 2)
	t.Logf("writing message of size: %d", size)

	client.Start(types.ClientSimpleQuery)
	client.AddBytes(make([]byte, size))
	err = client.End()
	if err != nil {
		t.Fatal(err)
	}

	client.Error(t)
	client.ReadyForQuery(t)
	client.Close(t)
}
