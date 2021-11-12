package wire

import (
	"context"
	"errors"

	"github.com/jeroenrinzema/psql-wire/codes"
	pgerror "github.com/jeroenrinzema/psql-wire/errors"
	"github.com/jeroenrinzema/psql-wire/internal/buffer"
	"github.com/jeroenrinzema/psql-wire/internal/types"
)

// authType represents the manner in which a client is able to authenticate
type authType int32

const (
	// authOK indicates that the connection has been authenticated and the client
	// is allowed to proceed.
	authOK authType = 0
	// authClearTextPassword is a authentication type used to tell the client to identify
	// itself by sending the password in clear text to the Postgres server.
	authClearTextPassword authType = 3
)

// AuthStrategy represents a authentication strategy used to authenticate a user
type AuthStrategy func(ctx context.Context, writer *buffer.Writer, reader *buffer.Reader) (err error)

// handleAuth handles the client authentication for the given connection.
// This methods validates the incoming credentials and writes to the client whether
// the provided credentials are correct. When the provided credentials are invalid
// or any unexpected error occures is an error returned and should the connection be closed.
func (srv *Server) handleAuth(ctx context.Context, reader *buffer.Reader, writer *buffer.Writer) error {
	srv.logger.Debug("authenticating client connection")

	if srv.Auth == nil {
		// No authentication strategy configured.
		// Announcing to the client that the connection is authenticated
		return writeAuthType(writer, authOK)
	}

	return srv.Auth(ctx, writer, reader)
}

// ClearTextPassword announces to the client to authenticate by sending a
// clear text password and validates if the provided username and password (received
// inside the client parameters) are valid. If the provided credentials are invalid
// or any unexpected error occures is an error returned and should the connection be closed.
func ClearTextPassword(validate func(username, password string) (bool, error)) AuthStrategy {
	return func(ctx context.Context, writer *buffer.Writer, reader *buffer.Reader) (err error) {
		err = writeAuthType(writer, authClearTextPassword)
		if err != nil {
			return err
		}

		params := ClientParameters(ctx)
		t, _, err := reader.ReadTypedMsg()
		if err != nil {
			return err
		}

		if t != types.ClientPassword {
			return errors.New("unexpected password message")
		}

		password, err := reader.GetString()
		if err != nil {
			return err
		}

		valid, err := validate(params[ParamUsername], password)
		if err != nil {
			return err
		}

		if !valid {
			return ErrorCode(writer, pgerror.WithCode(errors.New("invalid username/password"), codes.InvalidPassword))
		}

		return writeAuthType(writer, authOK)
	}
}

// writeAuthType writes the auth type to the client informing the client about the
// authentication status and the expected data to be received.
func writeAuthType(writer *buffer.Writer, status authType) error {
	writer.Start(types.ServerAuth)
	writer.AddInt32(int32(status))
	return writer.End()
}

// IsSuperUser checks whether the given connection context is a super user
func IsSuperUser(ctx context.Context) bool {
	return false
}

// AuthenticatedUsername returns the username of the authenticated user of the
// given connection context
func AuthenticatedUsername(ctx context.Context) string {
	parameters := ClientParameters(ctx)
	return parameters[ParamUsername]
}
