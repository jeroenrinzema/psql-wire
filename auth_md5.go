package wire

import (
	"context"
	"crypto/md5" //nolint:gosec // PostgreSQL's legacy MD5 protocol requires MD5.
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"errors"

	"github.com/jeroenrinzema/psql-wire/pkg/buffer"
)

const md5PasswordVerifierLength = len("md5") + md5.Size*2

// NewMD5PasswordVerifier returns a PostgreSQL-compatible MD5 password
// verifier. MD5 authentication is deprecated by PostgreSQL and should only be
// used for compatibility with clients that cannot use SCRAM-SHA-256.
func NewMD5PasswordVerifier(username, password string) string {
	return postgresMD5(password, []byte(username))
}

// MD5Password authenticates clients using PostgreSQL's legacy MD5
// challenge-response protocol. AuthenticationFn must return a verifier created
// by [NewMD5PasswordVerifier] or an equivalent PostgreSQL implementation.
//
// MD5 authentication is deprecated by PostgreSQL. Prefer [SCRAMSHA256].
func MD5Password(authenticate AuthenticationFn) AuthStrategy {
	return func(ctx context.Context, writer *buffer.Writer, reader *buffer.Reader) (context.Context, error) {
		params := ClientParameters(ctx)
		next, verifier, found, err := authenticate(ctx, params[ParamDatabase], params[ParamUsername])
		if err != nil {
			return ctx, err
		}
		ctx = next

		inner, err := parseMD5PasswordVerifier(verifier)
		if !found || err != nil {
			inner, err = randomMD5Digest()
			if err != nil {
				return ctx, err
			}
		}

		var salt [4]byte
		if _, err := rand.Read(salt[:]); err != nil {
			return ctx, err
		}
		if err := writeAuthRequest(writer, authMD5Password, salt[:]); err != nil {
			return ctx, err
		}

		response, err := readPasswordMessage(reader)
		if err != nil {
			return ctx, err
		}
		expected := postgresMD5(inner, salt[:])
		valid := found && len(response) == len(expected) && subtle.ConstantTimeCompare([]byte(response), []byte(expected)) == 1
		if !valid {
			return ctx, authenticationFailed(writer)
		}
		return ctx, writeAuthType(writer, authOK)
	}
}

func parseMD5PasswordVerifier(verifier string) (string, error) {
	if len(verifier) != md5PasswordVerifierLength || verifier[:3] != "md5" {
		return "", errors.New("invalid PostgreSQL MD5 password verifier")
	}

	digest, err := hex.DecodeString(verifier[3:])
	if err != nil || len(digest) != md5.Size {
		return "", errors.New("invalid PostgreSQL MD5 password verifier")
	}
	return hex.EncodeToString(digest), nil
}

func randomMD5Digest() (string, error) {
	var digest [md5.Size]byte
	if _, err := rand.Read(digest[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(digest[:]), nil
}

func postgresMD5(password string, salt []byte) string {
	hash := md5.New() //nolint:gosec // PostgreSQL's legacy MD5 protocol requires MD5.
	_, _ = hash.Write([]byte(password))
	_, _ = hash.Write(salt)
	return "md5" + hex.EncodeToString(hash.Sum(nil))
}
