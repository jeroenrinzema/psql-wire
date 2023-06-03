package wire

import (
	"context"
	"crypto/tls"
	"errors"
	"net"

	"github.com/jeroenrinzema/psql-wire/internal/buffer"
	"github.com/jeroenrinzema/psql-wire/internal/types"
	"go.uber.org/zap"
)

// Handshake performs the connection handshake and returns the connection
// version and a buffered reader to read incoming messages send by the client.
func (srv *Server) Handshake(conn net.Conn) (_ net.Conn, version types.Version, reader *buffer.Reader, err error) {
	reader = buffer.NewReader(srv.logger, conn, srv.BufferedMsgSize)
	version, err = srv.readVersion(reader)
	if err != nil {
		return conn, version, reader, err
	}

	if version == types.VersionCancel {
		return conn, version, reader, nil
	}

	// TODO: support GSS encryption
	//
	// `psql-wire` currently does not support GSS encrypted connections. The GSS
	// authentication API is supported inside the PostgreSQL wire protocol and
	// API's should be made available to support these type of connections.
	// https://www.postgresql.org/docs/current/gssapi-auth.html
	// https://www.postgresql.org/docs/current/protocol-flow.html#id-1.10.6.7.13

	conn, reader, version, err = srv.potentialConnUpgrade(conn, reader, version)
	if err != nil {
		return conn, version, reader, err
	}

	return conn, version, reader, nil
}

// readVersion reads the start-up protocol version (uint32) and the
// buffer containing the rest.
func (srv *Server) readVersion(reader *buffer.Reader) (_ types.Version, err error) {
	var version uint32
	_, err = reader.ReadUntypedMsg()
	if err != nil {
		return 0, err
	}

	version, err = reader.GetUint32()
	if err != nil {
		return 0, err
	}

	return types.Version(version), nil
}

// readyForQuery indicates that the server is ready to receive queries.
// The given server status is included inside the message to indicate the server
// status. This message should be written when a command cycle has been completed.
func readyForQuery(writer *buffer.Writer, status types.ServerStatus) error {
	writer.Start(types.ServerReady)
	writer.AddByte(byte(status))
	return writer.End()
}

// readParameters reads the key/value connection parameters send by the client and
// The read parameters will be set inside the given context. A new context containing
// the consumed parameters will be returned.
func (srv *Server) readClientParameters(ctx context.Context, reader *buffer.Reader) (_ context.Context, err error) {
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

// writeParameters writes the server parameters such as client encoding to the client.
// The written parameters will be attached as a value to the given context. A new
// context containing the written parameters will be returned.
// https://www.postgresql.org/docs/10/libpq-status.html
func (srv *Server) writeParameters(ctx context.Context, writer *buffer.Writer, params Parameters) (_ context.Context, err error) {
	if params == nil {
		params = make(Parameters, 4)
	}

	srv.logger.Debug("writing server parameters")

	params[ParamServerEncoding] = "UTF8"
	params[ParamClientEncoding] = "UTF8"
	if srv.Version != "" {
		params[ParamServerVersion] = srv.Version
	}
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

// potentialConnUpgrade potentially upgrades the given connection using TLS
// if the client requests for it. The connection upgrade is ignored if the
// server does not support a secure connection.
func (srv *Server) potentialConnUpgrade(conn net.Conn, reader *buffer.Reader, version types.Version) (_ net.Conn, _ *buffer.Reader, _ types.Version, err error) {
	if version != types.VersionSSLRequest {
		return conn, reader, version, nil
	}

	srv.logger.Debug("attempting to upgrade the client to a TLS connection")

	if len(srv.Certificates) == 0 {
		srv.logger.Debug("no TLS certificates available continuing with a insecure connection")
		return srv.sslUnsupported(conn, reader, version)
	}

	_, err = conn.Write(sslSupported)
	if err != nil {
		return conn, reader, version, err
	}

	tlsConfig := tls.Config{
		Certificates: srv.Certificates,
		ClientAuth:   srv.ClientAuth,
		ClientCAs:    srv.ClientCAs,
	}

	// NOTE: initialize the TLS connection and construct a new buffered
	// reader for the constructed TLS connection.
	conn = tls.Server(conn, &tlsConfig)
	reader = buffer.NewReader(srv.logger, conn, srv.BufferedMsgSize)

	version, err = srv.readVersion(reader)
	if err != nil {
		return conn, reader, version, err
	}

	srv.logger.Debug("connection has been upgraded successfully")
	return conn, reader, version, err
}

// sslUnsupported announces to the PostgreSQL client that we are unable to
// upgrade the connection to a secure connection at this time. The client
// version is read again once the insecure connection has been announced.
func (srv *Server) sslUnsupported(conn net.Conn, reader *buffer.Reader, version types.Version) (_ net.Conn, _ *buffer.Reader, _ types.Version, err error) {
	_, err = conn.Write(sslUnsupported)
	if err != nil {
		return conn, reader, version, err
	}

	version, err = srv.readVersion(reader)
	if err != nil {
		return conn, reader, version, err
	}

	if version == types.VersionCancel {
		return conn, reader, version, errors.New("unexpected cancel version after upgrading the client connection")
	}

	return conn, reader, version, nil
}
