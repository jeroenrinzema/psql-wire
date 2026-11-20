package mock

import (
	"encoding/binary"
	"errors"
	"io"
	"net"
	"regexp"
	"syscall"
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
	// serverClosed is set once the server closed the connection, so Close
	// doesn't try to send a Terminate anymore.
	serverClosed bool
}

// ExpectClosed asserts that the server closed the connection without sending
// anything else. A connection reset counts as closed too, because the server
// resets the connection when it closes it with unread messages from the
// client.
func (client *Client) ExpectClosed(t *testing.T) {
	t.Helper()
	_, _, err := client.ReadTypedMsg()
	require.Error(t, err)
	require.True(t, errors.Is(err, io.EOF) || errors.Is(err, syscall.ECONNRESET), "expected the connection to be closed, got: %v", err)
	client.serverClosed = true
}

// Handshake performs a simple handshake over the underlaying connection. A
// handshake consists out of introducing/publishing the client version and
// connection preferences and the writing of (metadata) parameters identifying
// the given client.
func (client *Client) Handshake(t *testing.T) {
	client.HandshakeProtocol(t, types.Version30, "client", "mock")
}

// HandshakeProtocol performs a handshake announcing the given protocol version
// together with the provided startup parameters. The parameters are passed as
// alternating key/value pairs. Protocol extension options (keys prefixed with
// `_pq_.`) may be included among the parameters to exercise protocol version
// negotiation.
func (client *Client) HandshakeProtocol(t *testing.T, version types.Version, params ...string) {
	t.Log("performing simple handshake")
	defer t.Log("simple handshake completed")

	if len(params)%2 != 0 {
		t.Fatalf("expected an even number of key/value parameters, got %d", len(params))
	}

	versionBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(versionBytes, uint32(version))

	// NOTE: the parameters consist out of keys and values. Each key and
	// value is terminated using a nul byte and the end of all parameters is
	// identified using a empty key value.
	nul := byte(0)
	var parameters []byte
	for i := 0; i < len(params); i += 2 {
		parameters = append(parameters, append([]byte(params[i]), nul)...)
		parameters = append(parameters, append([]byte(params[i+1]), nul)...)
	}
	parameters = append(parameters, nul) // empty key terminates the parameter list

	// NOTE: we have to define the total message length inside the
	// header by prefixing a unsigned 32 big-endian int.
	header := make([]byte, 4)
	binary.BigEndian.PutUint32(header, uint32(len(versionBytes)+len(parameters)+len(header)))

	_, err := client.conn.Write(append(header, append(versionBytes, parameters...)...))
	if err != nil {
		t.Fatal(err)
	}
}

// NegotiateProtocolVersion reads a NegotiateProtocolVersion message from the
// server and returns the protocol version the server negotiated together with
// the list of protocol options the server did not recognize.
func (client *Client) NegotiateProtocolVersion(t *testing.T) (types.Version, []string) {
	t.Helper()
	t.Log("awaiting protocol version negotiation")
	defer t.Log("protocol version negotiation received")

	typed, _, err := client.ReadTypedMsg()
	require.NoError(t, err)
	require.Equal(t, types.ServerNegotiateVersion, typed, "unexpected message type %s, expected %s", typed, types.ServerNegotiateVersion)

	version, err := client.GetUint32()
	require.NoError(t, err)

	num, err := client.GetUint32()
	require.NoError(t, err)

	options := make([]string, 0, num)
	for i := uint32(0); i < num; i++ {
		option, err := client.GetString()
		require.NoError(t, err)
		options = append(options, option)
	}

	return types.Version(version), options
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
// for query message carrying the given transaction status byte. This message
// indicates that the server is ready to accept a new typed message to execute
// an action. ParameterStatus messages preceding the ReadyForQuery are skipped.
func (client *Client) ReadyForQuery(t *testing.T, expected types.ServerStatus) {
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

	if types.ServerStatus(bb[0]) != expected {
		t.Fatalf("unexpected ready for query status: %d, expected %d", bb[0], expected)
	}
}

// Error reads an ErrorResponse message and asserts that its primary message
// (the 'M' field) matches the given regex pattern.
func (client *Client) Error(t *testing.T, msgRegex string) {
	t.Helper()
	_, message := client.readError(t)
	require.Regexp(t, regexp.MustCompile(msgRegex), message)
}

// ErrorCode reads an ErrorResponse message and asserts that its SQLSTATE (the
// 'C' field) equals code, and that its primary message matches the given
// regex pattern.
func (client *Client) ErrorCode(t *testing.T, code string, msgRegex string) {
	t.Helper()
	actual, message := client.readError(t)
	require.Equal(t, code, actual)
	require.Regexp(t, regexp.MustCompile(msgRegex), message)
}

func (client *Client) readError(t *testing.T) (code string, message string) {
	t.Helper()
	fields := client.ErrorFields(t)
	return fields['C'], fields['M']
}

// ErrorFields reads an ErrorResponse message and returns all its fields by
// their field type, e.g. 'R' for the routine that reported the error.
func (client *Client) ErrorFields(t *testing.T) map[byte]string {
	t.Helper()
	t.Log("awaiting error message")
	defer t.Log("error message received")

	ct, _, err := client.ReadTypedMsg()
	require.NoError(t, err)
	require.Equal(t, types.ServerErrorResponse, types.ServerMessage(ct), "expected %s but got %s", types.ServerErrorResponse, ct)

	// ErrorResponse body is a sequence of (1-byte field type, null-terminated
	// string) records, terminated by a zero byte.
	fields := map[byte]string{}
	for {
		fieldType, err := client.GetBytes(1)
		require.NoError(t, err)
		if fieldType[0] == 0 {
			break
		}
		value, err := client.GetString()
		require.NoError(t, err)
		fields[fieldType[0]] = value
	}

	return fields
}

func (client *Client) Close(t *testing.T) {
	t.Log("closing the client!")
	defer t.Log("client closed")

	if !client.serverClosed {
		client.Start(types.ClientTerminate)
		err := client.End()
		if err != nil {
			t.Fatal(err)
		}
	}

	err := client.conn.Close()
	if err != nil {
		t.Fatal(err)
	}
}

// SimpleQuery sends a Query message of the simple query protocol.
func (client *Client) SimpleQuery(t *testing.T, query string) {
	t.Helper()
	client.Start(types.ClientSimpleQuery)
	client.AddString(query)
	client.AddNullTerminate()
	require.NoError(t, client.End())
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

// Bind sends a Bind message binding the given statement to the given portal,
// with the given parameter values in text format.
func (client *Client) Bind(t *testing.T, portal, statement string, parameters ...string) {
	t.Helper()
	client.Start(types.ClientBind)
	client.AddString(portal)
	client.AddNullTerminate()
	client.AddString(statement)
	client.AddNullTerminate()
	client.AddInt16(0) // all parameters use the text format
	client.AddInt16(int16(len(parameters)))
	for _, parameter := range parameters {
		client.AddInt32(int32(len(parameter)))
		client.AddString(parameter)
	}
	client.AddInt16(0) // no result format codes
	require.NoError(t, client.End())
}

// DescribeStatement sends a Describe message for the given prepared statement.
func (client *Client) DescribeStatement(t *testing.T, name string) {
	t.Helper()
	client.describe(t, types.DescribeStatement, name)
}

// DescribePortal sends a Describe message for the given portal.
func (client *Client) DescribePortal(t *testing.T, name string) {
	t.Helper()
	client.describe(t, types.DescribePortal, name)
}

func (client *Client) describe(t *testing.T, kind types.DescribeMessage, name string) {
	t.Helper()
	client.Start(types.ClientDescribe)
	client.AddByte(byte(kind))
	client.AddString(name)
	client.AddNullTerminate()
	require.NoError(t, client.End())
}

// Flush sends a Flush message.
func (client *Client) Flush(t *testing.T) {
	t.Helper()
	client.Start(types.ClientFlush)
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

// Column is a column of a RowDescription message.
type Column struct {
	Name string
	Oid  uint32
}

// ExpectRowDescription reads a RowDescription message and returns its columns.
func (client *Client) ExpectRowDescription(t *testing.T) []Column {
	t.Helper()
	client.ExpectMsg(t, types.ServerRowDescription)

	numCols, err := client.GetUint16()
	require.NoError(t, err)

	columns := make([]Column, numCols)
	for i := range columns {
		name, err := client.GetString()
		require.NoError(t, err)
		// The table OID (4 bytes) and attribute number (2 bytes) come
		// between the name and the type OID.
		_, err = client.GetBytes(6)
		require.NoError(t, err)
		oid, err := client.GetUint32()
		require.NoError(t, err)
		// The type size (2 bytes), type modifier (4 bytes) and format code
		// (2 bytes) are not checked by any test yet.
		_, err = client.GetBytes(8)
		require.NoError(t, err)
		columns[i] = Column{Name: name, Oid: oid}
	}
	return columns
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
