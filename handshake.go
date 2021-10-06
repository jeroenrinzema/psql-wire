package wire

import (
	"context"
	"crypto/tls"
	"errors"
	"net"

	"github.com/jeroenrinzema/psql-wire/buffer"
	"github.com/jeroenrinzema/psql-wire/types"
	"go.uber.org/zap"
)

// Version represents a connection version presented inside the connection header
type Version uint32

// The below constants can occur during the first message a client
// sends to the server. There are two categories: protocol version and
// request code. The protocol version is (major version number << 16)
// + minor version number. Request codes are (1234 << 16) + 5678 + N,
// where N started at 0 and is increased by 1 for every new request
// code added, which happens rarely during major or minor Postgres
// releases.
//
// See: https://www.postgresql.org/docs/current/protocol-message-formats.html
const (
	Version30         Version = 196608   // (3 << 16) + 0
	VersionCancel     Version = 80877102 // (1234 << 16) + 5678
	VersionSSLRequest Version = 80877103 // (1234 << 16) + 5679
	VersionGSSENC     Version = 80877104 // (1234 << 16) + 5680
)

// Handshake performs the connection handshake and returns the 3 connection
// version and a buffered reader to read incoming messages send by the client.
func (srv *Server) Handshake(conn net.Conn) (_ net.Conn, version Version, reader *buffer.Reader, err error) {
	reader = buffer.NewReader(conn, srv.BufferedMsgSize)
	version, err = srv.ReadVersion(reader)
	if err != nil {
		return conn, version, reader, err
	}

	if version == VersionCancel {
		return conn, version, reader, nil
	}

	// TODO(Jeroen): support GSS encryption

	conn, reader, version, err = srv.PotentialConnUpgrade(conn, reader, version)
	if err != nil {
		return conn, version, reader, err
	}

	return conn, version, reader, nil
}

// ReadVersion reads the start-up protocol version (uint32) and the
// buffer containing the rest.
func (srv *Server) ReadVersion(reader *buffer.Reader) (_ Version, err error) {
	var version uint32
	_, err = reader.ReadUntypedMsg()
	if err != nil {
		return 0, err
	}

	version, err = reader.GetUint32()
	if err != nil {
		return 0, err
	}

	return Version(version), nil
}

// ServerStatus indicates the current server status. Possible values are 'I' if
// idle (not in a transaction block); 'T' if in a transaction block; or 'E' if
// in a failed transaction block (queries will be rejected until block is ended).
type ServerStatus byte

// Possible values are 'I' if idle (not in a transaction block); 'T' if in a
// transaction block; or 'E' if in a failed transaction block
// (queries will be rejected until block is ended).
const (
	ServerIdle              = 'I'
	ServerTransactionBlock  = 'T'
	ServerTransactionFailed = 'E'
)

// ReadyForQuery indicates that the server is ready to receive queries.
// The given server status is included inside the message to indicate the server status.
func ReadyForQuery(writer *buffer.Writer, status ServerStatus) error {
	writer.Start(types.ServerReady)
	writer.AddByte(byte(status))
	return writer.End()
}

// ReadParameters reads the key/value connection parameters send by the client and
// The read parameters will be set inside the given context. A new context containing
// the consumed parameters will be returned.
func (srv *Server) ReadParameters(ctx context.Context, reader *buffer.Reader) (_ context.Context, err error) {
	meta := make(Parameters)

	srv.logger.Debug("reading client parameters")

	for {
		key, err := reader.GetString()
		if err != nil {
			return nil, err
		}

		// an empty key indicates the end of the connection parameters
		if len(key) == 0 {
			break
		}

		value, err := reader.GetString()
		if err != nil {
			return nil, err
		}

		srv.logger.Debug("client parameter", zap.String("key", key), zap.String("value", value))
		meta[ParameterStatus(key)] = value
	}

	return setClientParameters(ctx, meta), nil
}

// WriteParameters writes the server parameters such as client encoding to the client.
// The written parameters will be attached as a value to the given context. A new
// context containing the written parameters will be returned.
// https://www.postgresql.org/docs/10/libpq-status.html
func (srv *Server) WriteParameters(ctx context.Context, writer *buffer.Writer, params Parameters) (_ context.Context, err error) {
	if params == nil {
		params = make(Parameters, 4)
	}

	srv.logger.Debug("writing server parameters")

	params[ParamServerEncoding] = "UTF8"
	params[ParamClientEncoding] = "UTF8"
	params[ParamIsSuperuser] = buffer.EncodeBoolean(IsSuperUser(ctx))
	params[ParamSessionAuthorization] = AuthenticatedUsername(ctx)

	for key, value := range params {
		srv.logger.Debug("server parameter", zap.String("key", string(key)), zap.String("value", value))

		writer.Start(types.ServerParameterStatus)
		writer.AddString(string(key))
		writer.AddNullTerminate()
		writer.AddString(value)
		writer.AddNullTerminate()
		err = writer.End()
		if err != nil {
			return ctx, err
		}
	}

	return setServerParameters(ctx, params), nil
}

// PotentialConnUpgrade potentially upgrades the given connection using TLS
// if the client requests for it. The connection upgrade is ignored if the
// server does not support a secure connection.
func (srv *Server) PotentialConnUpgrade(conn net.Conn, reader *buffer.Reader, version Version) (_ net.Conn, _ *buffer.Reader, _ Version, err error) {
	if version != VersionSSLRequest {
		return conn, reader, version, nil
	}

	srv.logger.Debug("attempting to upgrade the client to a TLS connection")

	if len(srv.Certificates) == 0 {
		srv.logger.Debug("no TLS certificates available continuing with a insecure connection")
		return srv.SSLUnsupported(conn, reader, version)
	}

	_, err = conn.Write(SSLSupported)
	if err != nil {
		return conn, reader, version, err
	}

	tlsConfig := tls.Config{
		Certificates: srv.Certificates,
	}

	// NOTE(Jeroen): initialize the TLS connection and construct a new buffered
	// reader for the constructed TLS connection.
	conn = tls.Server(conn, &tlsConfig)
	reader = buffer.NewReader(conn, srv.BufferedMsgSize)

	version, err = srv.ReadVersion(reader)
	if err != nil {
		return conn, reader, version, err
	}

	srv.logger.Debug("connection has been upgraded successfully")
	return conn, reader, version, err
}

// SSLUnsupported announces to the PostgreSQL client that we are unable to
// upgrade the connection to a secure connection at this time. The client
// version is read again once the insecure connection has been announced.
func (srv *Server) SSLUnsupported(conn net.Conn, reader *buffer.Reader, version Version) (_ net.Conn, _ *buffer.Reader, _ Version, err error) {
	_, err = conn.Write(SSLUnsupported)
	if err != nil {
		return conn, reader, version, err
	}

	version, err = srv.ReadVersion(reader)
	if err != nil {
		return conn, reader, version, err
	}

	if version == VersionCancel {
		return conn, reader, version, errors.New("unexpected cancel version after upgrading the client connection")
	}

	return conn, reader, version, nil
}
