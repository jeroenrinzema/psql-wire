package mock

import (
	"encoding/binary"
	"net"
	"testing"

	"github.com/jeroenrinzema/psql-wire/pkg/types"
	"github.com/stretchr/testify/require"
)

func NewClient(t *testing.T, conn net.Conn) *Client {
	return &Client{
		conn:   conn,
		Writer: NewWriter(t, conn),
		Reader: NewReader(t, conn),
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

	err = client.conn.Close()
	if err != nil {
		t.Fatal(err)
	}
}

// Parse sends a Parse message with the given statement name and query.
func (client *Client) Parse(t *testing.T, name, query string) {
	t.Helper()
	client.Start(types.ClientParse)
	client.AddString(name)
	client.AddNullTerminate()
	client.AddString(query)
	client.AddNullTerminate()
	client.AddInt16(0) // no parameter types
	require.NoError(t, client.End())
}

// Bind sends a Bind message binding the given statement to the given portal.
func (client *Client) Bind(t *testing.T, portal, statement string) {
	t.Helper()
	client.Start(types.ClientBind)
	client.AddString(portal)
	client.AddNullTerminate()
	client.AddString(statement)
	client.AddNullTerminate()
	client.AddInt16(0) // no parameter formats
	client.AddInt16(0) // no parameter values
	client.AddInt16(0) // no result format codes
	require.NoError(t, client.End())
}

// Execute sends an Execute message for the given portal with the given row limit.
// A limit of 0 means no limit.
func (client *Client) Execute(t *testing.T, portal string, limit int32) {
	t.Helper()
	client.Start(types.ClientExecute)
	client.AddString(portal)
	client.AddNullTerminate()
	client.AddInt32(limit)
	require.NoError(t, client.End())
}

// Sync sends a Sync message.
func (client *Client) Sync(t *testing.T) {
	t.Helper()
	client.Start(types.ClientSync)
	require.NoError(t, client.End())
}

// ExpectMsg reads one message and asserts its type matches expected.
func (client *Client) ExpectMsg(t *testing.T, expected types.ServerMessage) {
	t.Helper()
	ct, _, err := client.ReadTypedMsg()
	require.NoError(t, err)
	require.Equal(t, expected, ct, "expected %s but got %s", expected, ct)
}

// ExpectDataRow reads a single DataRow message and returns the column values
// as raw byte slices (nil for SQL NULL).
func (client *Client) ExpectDataRow(t *testing.T) [][]byte {
	t.Helper()
	client.ExpectMsg(t, types.ServerDataRow)

	numCols, err := client.GetUint16()
	require.NoError(t, err)

	row := make([][]byte, numCols)
	for i := 0; i < int(numCols); i++ {
		length, err := client.GetInt32()
		require.NoError(t, err)
		if length == -1 {
			row[i] = nil
			continue
		}
		val, err := client.GetBytes(int(length))
		require.NoError(t, err)
		row[i] = val
	}
	return row
}

// ExpectCommandComplete reads a CommandComplete message and returns the tag string.
func (client *Client) ExpectCommandComplete(t *testing.T) string {
	t.Helper()
	client.ExpectMsg(t, types.ServerCommandComplete)
	tag, err := client.GetString()
	require.NoError(t, err)
	return tag
}

// ExpectDataRows reads exactly n DataRow messages and returns all rows.
func (client *Client) ExpectDataRows(t *testing.T, n int) [][][]byte {
	t.Helper()
	rows := make([][][]byte, n)
	for i := 0; i < n; i++ {
		rows[i] = client.ExpectDataRow(t)
	}
	return rows
}
