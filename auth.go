package wire

import (
	"context"
	"errors"

	"github.com/jeroenrinzema/psql-wire/codes"
	pgerror "github.com/jeroenrinzema/psql-wire/errors"
	"github.com/jeroenrinzema/psql-wire/pkg/buffer"
	"github.com/jeroenrinzema/psql-wire/pkg/types"
)

// authType represents the manner in which a client is able to authenticate
type authType int32

const (
	// authOK indicates that the connection has been authenticated and the client
	// is allowed to proceed.
	authOK authType = 0
	// authClearTextPassword is an authentication type used to tell the client to identify
	// itself by sending the password in clear text to the Postgres server.
	authClearTextPassword authType = 3
	// authMD5Password requests PostgreSQL's MD5 challenge-response authentication.
	authMD5Password authType = 5
	// authSASL starts a SASL authentication exchange.
	authSASL authType = 10
	// authSASLContinue carries a SASL challenge.
	authSASLContinue authType = 11
	// authSASLFinal carries the successful SASL outcome.
	authSASLFinal authType = 12
)

// AuthStrategy represents an authentication strategy used to authenticate a user.
type AuthStrategy func(ctx context.Context, writer *buffer.Writer, reader *buffer.Reader) (_ context.Context, err error)

// AuthenticationFn resolves the stored authentication credential for a database
// user. The credential must use the format expected by the selected
// authentication strategy. The returned context is used for the remainder of
// the connection.
//
// found must be false when no usable credential exists. Implementations should
// not expose whether the user is unknown, disabled, expired, or otherwise unable
// to authenticate.
type AuthenticationFn func(ctx context.Context, database, username string) (next context.Context, credential string, found bool, err error)

// BackendKeyDataFunc represents a function that generates backend key data for query cancellation.
// It should return a process ID and secret key that can be used by clients to cancel queries.
type BackendKeyDataFunc func(ctx context.Context) (processID int32, secretKey int32)

// handleAuth handles the client authentication for the given connection.
// This methods validates the incoming credentials and writes to the client whether
// the provided credentials are correct. When the provided credentials are invalid
// or any unexpected error occures is an error returned and should the connection be closed.
func (srv *Server) handleAuth(ctx context.Context, reader *buffer.Reader, writer *buffer.Writer) (context.Context, error) {
	srv.logger.Debug("authenticating client connection")

	if srv.Auth == nil {
		// No authentication strategy configured.
		// Announcing to the client that the connection is authenticated
		return ctx, writeAuthType(writer, authOK)
	}

	return srv.Auth(ctx, writer, reader)
}

// ClearTextPassword announces to the client to authenticate by sending a
// clear text password and validates if the provided username and password (received
// inside the client parameters) are valid. If the provided credentials are invalid
// or any unexpected error occurs, an error returned and the connection should be closed.
func ClearTextPassword(validate func(ctx context.Context, database, username, password string) (context.Context, bool, error)) AuthStrategy {
	return func(ctx context.Context, writer *buffer.Writer, reader *buffer.Reader) (_ context.Context, err error) {
		err = writeAuthType(writer, authClearTextPassword)
		if err != nil {
			return ctx, err
		}

		params := ClientParameters(ctx)
		password, err := readPasswordMessage(reader)
		if err != nil {
			return ctx, err
		}
		ctx, valid, err := validate(ctx, params[ParamDatabase], params[ParamUsername], password)
		if err != nil {
			return ctx, err
		}

		if !valid {
			return ctx, authenticationFailed(writer)
		}

		return ctx, writeAuthType(writer, authOK)
	}
}

// writeAuthType writes the auth type to the client informing the client about the
// authentication status and the expected data to be received.
func writeAuthType(writer *buffer.Writer, status authType) error {
	return writeAuthRequest(writer, status, nil)
}

// writeAuthRequest writes an authentication request and its optional
// mechanism-specific payload.
func writeAuthRequest(writer *buffer.Writer, status authType, payload []byte) error {
	writer.Start(types.ServerAuth)
	writer.AddInt32(int32(status))
	writer.AddBytes(payload)
	return writer.End()
}

// readPasswordMessage reads a PasswordMessage. PostgreSQL also uses this
// message type for MD5, SASL, GSSAPI, and SSPI responses.
func readPasswordMessage(reader *buffer.Reader) (string, error) {
	t, _, err := reader.ReadTypedMsg()
	if err != nil {
		return "", err
	}
	if t != types.ClientPassword {
		return "", errors.New("unexpected password message")
	}

	password, err := reader.GetString()
	if err != nil {
		return "", err
	}
	if len(reader.Msg) != 0 {
		return "", errors.New("unexpected data after password")
	}
	return password, nil
}

// authenticationFailed writes the generic authentication failure returned for
// unknown users and invalid credentials alike.
func authenticationFailed(writer *buffer.Writer) error {
	authErr := pgerror.WithSeverity(
		pgerror.WithCode(errors.New("invalid username/password"), codes.InvalidPassword),
		pgerror.LevelFatal,
	)
	if err := WriteUnterminatedError(writer, authErr); err != nil {
		return err
	}
	return authErr
}

// authenticationProtocolFailed writes a fatal PostgreSQL protocol violation.
func authenticationProtocolFailed(writer *buffer.Writer, cause error) error {
	protocolErr := pgerror.WithSeverity(
		pgerror.WithCode(cause, codes.ProtocolViolation),
		pgerror.LevelFatal,
	)
	if err := WriteUnterminatedError(writer, protocolErr); err != nil {
		return err
	}
	return protocolErr
}

// writeBackendKeyData writes the backend key data to the client. This message contains
// cancellation key data that the frontend must save if it wishes to be able to issue
// CancelRequest messages later.
func writeBackendKeyData(writer *buffer.Writer, processID int32, secretKey int32) error {
	writer.Start(types.ServerBackendKeyData)
	writer.AddInt32(processID)
	writer.AddInt32(secretKey)
	return writer.End()
}

// IsSuperUser checks whether the given connection context is a super user.
func IsSuperUser(ctx context.Context) bool {
	return false
}

// AuthenticatedUsername returns the username of the authenticated user of the
// given connection context.
func AuthenticatedUsername(ctx context.Context) string {
	parameters := ClientParameters(ctx)
	return parameters[ParamUsername]
}
