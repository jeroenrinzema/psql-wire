package mock

import (
	"encoding/binary"
	"net"
	"testing"

	"github.com/jeroenrinzema/psql-wire/internal/types"
)

func NewClient(conn net.Conn) *Client {
	return &Client{
		conn:   conn,
		Writer: NewWriter(conn),
		Reader: NewReader(conn),
	}
}

type Client struct {
	conn net.Conn
	*Writer
	*Reader
}

// Handshake performs a simple handshake over the underlaying connection. A
// handshake consists out of introducing/publishing the client version and
// connection preferences and the writing of (metadata) parameters identifying
// the given client.
func (client *Client) Handshake(t *testing.T) {
	t.Log("performing simple handshake")
	defer t.Log("simple handshake completed")

	version := make([]byte, 4)
	binary.BigEndian.PutUint32(version, uint32(types.Version30))

	// NOTE: the parameters consist out of keys and values. Each key and
	// value is terminated using a nul byte and the end of all parameters is
	// identified using a empty key value.
	nul := byte(0)
	key := append([]byte("client"), nul)
	value := append([]byte("mock"), nul)
	end := append([]byte(""), nul)
	parameters := append(append(key, value...), end...)

	// NOTE: we have to define the total message length inside the
	// header by prefixing a unsigned 32 big-endian int.
	header := make([]byte, 4)
	binary.BigEndian.PutUint32(header, uint32(len(version)+len(parameters)+len(header)))

	_, err := client.conn.Write(append(header, append(version, parameters...)...))
	if err != nil {
		t.Fatal(err)
	}
}

// Authenticate performs a simple authentication using the PostgreSQL wire
// protocol. The method fails whenever an unexpected message server message
// type/state has been returned of the connection has not been authenticated.
func (client *Client) Authenticate(t *testing.T) {
	t.Log("performing simple authentication")
	defer t.Log("simple authentication completed")

	typed, _, err := client.ReadTypedMsg()
	if err != nil {
		t.Fatal(err)
	}

	if typed != types.ServerAuth {
		t.Fatalf("unexpected message type %d, expected %d", typed, types.ServerAuth)
	}

	status, err := client.GetUint32()
	if err != nil {
		t.Fatal(err)
	}

	// NOTE: a status of 0 indicates that the connection has been authenticated
	if status != 0 {
		t.Fatalf("unexpected auth status: %d, expected auth ok", status)
	}
}

// ReadyForQuery awaits till the underlaying network connection returns a ready
// for query message. This message indicates that the server is ready to accept
// a new typed message to execute a action.
func (client *Client) ReadyForQuery(t *testing.T) {
	var err error
	var typed types.ServerMessage

	t.Log("awaiting ready for query")
	defer t.Log("ready for query received")

	for {
		typed, _, err = client.ReadTypedMsg()
		if err != nil {
			t.Fatal(err)
		}

		if typed != types.ServerParameterStatus {
			break
		}
	}

	if typed != types.ServerReady {
		t.Fatalf("unexpected message type %d, expected %d", typed, types.ServerReady)
	}

	bb, err := client.GetBytes(1)
	if err != nil {
		t.Fatal(err)
	}

	if types.ServerStatus(bb[0]) != types.ServerIdle {
		t.Fatalf("unexpected ready for query status: %d, expected server idle", bb)
	}
}

func (client *Client) Error(t *testing.T) {
	t.Log("awaiting error message")
	defer t.Log("error message received")

	ct, _, err := client.ReadTypedMsg()
	if err != nil {
		t.Fatal(err)
	}

	st := types.ServerMessage(ct)
	if st != types.ServerErrorResponse {
		t.Fatalf("unexpected response message type %d, expected %d", st, types.ServerErrorResponse)
	}
}

func (client *Client) Close(t *testing.T) {
	t.Log("closing the client!")
	defer t.Log("client closed")

	client.Start(types.ClientTerminate)
	err := client.End()
	if err != nil {
		t.Fatal(err)
	}
}
