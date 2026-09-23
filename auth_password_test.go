package wire

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jeroenrinzema/psql-wire/pkg/buffer"
	"github.com/jeroenrinzema/psql-wire/pkg/types"
	_ "github.com/lib/pq"
	"github.com/neilotoole/slogt"
	"github.com/stretchr/testify/require"
)

func TestNewMD5PasswordVerifier(t *testing.T) {
	t.Parallel()
	require.Equal(t, "md520eb1b22a92b5c573dc1eb4331fc49ee", NewMD5PasswordVerifier("user", "secret"))
}

func TestSCRAMSHA256VerifierRFC7677(t *testing.T) {
	t.Parallel()

	salt := []byte{0x5b, 0x6d, 0x99, 0x68, 0x9d, 0x12, 0x35, 0x8e, 0xec, 0xa0, 0x4b, 0x14, 0x12, 0x36, 0xfa, 0x81}
	verifier, err := newSCRAMSHA256Verifier("pencil", salt, 4096)
	require.NoError(t, err)
	require.Equal(t,
		"SCRAM-SHA-256$4096:W22ZaJ0SNY7soEsUEjb6gQ==$WG5d8oPm3OtcPnkdi4Uo7BkeZkBFzpcXkuLmtbsT4qY=:wfPLwcE6nTWhTAmQ7tl2KeoiWGPlZqQxSrmfPwDl2dU=",
		verifier,
	)

	credentials, err := parseSCRAMSHA256Verifier(verifier)
	require.NoError(t, err)
	require.Equal(t, salt, []byte(credentials.Salt))
	require.Equal(t, 4096, credentials.Iters)
	require.Len(t, credentials.StoredKey, sha256.Size)
	require.Len(t, credentials.ServerKey, sha256.Size)
}

func TestReadSASLInitialResponseRejectsLengthMismatch(t *testing.T) {
	t.Parallel()

	input := bytes.NewBuffer(nil)
	incoming := buffer.NewWriter(slogt.New(t), input)
	incoming.Start(types.ServerMessage(types.ClientPassword))
	incoming.AddString(scramSHA256Mechanism)
	incoming.AddNullTerminate()
	incoming.AddInt32(10)
	incoming.AddString("short")
	require.NoError(t, incoming.End())

	reader := buffer.NewReader(slogt.New(t), input, buffer.DefaultBufferSize)
	_, _, _, err := readSASLInitialResponse(reader)
	require.ErrorContains(t, err, "invalid SASL initial response length")
}

func TestValidateSCRAMMechanismBinding(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		mechanism string
		initial   string
		wantError bool
	}{
		{name: "plus with binding", mechanism: scramSHA256PlusMechanism, initial: "p=tls-server-end-point,,n=test,r=nonce"},
		{name: "plus without binding", mechanism: scramSHA256PlusMechanism, initial: "n,,n=test,r=nonce", wantError: true},
		{name: "base with binding", mechanism: scramSHA256Mechanism, initial: "p=tls-server-end-point,,n=test,r=nonce", wantError: true},
		{name: "base without binding", mechanism: scramSHA256Mechanism, initial: "n,,n=test,r=nonce"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := validateSCRAMMechanismBinding(test.mechanism, []byte(test.initial))
			if test.wantError {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
		})
	}
}

func TestTLSServerEndPoint(t *testing.T) {
	t.Parallel()

	certificate, err := tls.LoadX509KeyPair("examples/tls/psql.crt", "examples/tls/psql.key")
	require.NoError(t, err)
	binding, err := tlsServerEndPoint(&certificate)
	require.NoError(t, err)
	expected := sha256.Sum256(certificate.Certificate[0])
	require.Equal(t, expected[:], binding)
}

func TestPasswordAuthenticationClients(t *testing.T) {
	protocols := []struct {
		name     string
		verifier func(t *testing.T) string
		strategy func(AuthenticationFn) AuthStrategy
	}{
		{
			name: "md5",
			verifier: func(t *testing.T) string {
				return NewMD5PasswordVerifier("test", "secret")
			},
			strategy: MD5Password,
		},
		{
			name: "scram-sha-256",
			verifier: func(t *testing.T) string {
				verifier, err := NewSCRAMSHA256Verifier("secret")
				require.NoError(t, err)
				return verifier
			},
			strategy: SCRAMSHA256,
		},
	}
	clients := []struct {
		name    string
		connect func(t *testing.T, address *net.TCPAddr, password, sslMode, channelBinding string) error
	}{
		{name: "pgx", connect: connectPGXAuthClient},
		{name: "lib-pq", connect: connectPQAuthClient},
	}

	for _, protocol := range protocols {
		t.Run(protocol.name, func(t *testing.T) {
			verifier := protocol.verifier(t)
			authenticate := func(ctx context.Context, database, username string) (context.Context, string, bool, error) {
				return ctx, verifier, username == "test", nil
			}
			address := startAuthenticationServer(t, protocol.strategy(authenticate), nil)
			unknownAddress := startAuthenticationServer(t, protocol.strategy(func(ctx context.Context, database, username string) (context.Context, string, bool, error) {
				return ctx, "", false, nil
			}), nil)
			invalidAddress := startAuthenticationServer(t, protocol.strategy(func(ctx context.Context, database, username string) (context.Context, string, bool, error) {
				return ctx, "invalid verifier", true, nil
			}), nil)

			for _, client := range clients {
				t.Run(client.name+"/success", func(t *testing.T) {
					require.NoError(t, client.connect(t, address, "secret", "disable", "disable"))
				})
				t.Run(client.name+"/wrong-password", func(t *testing.T) {
					err := client.connect(t, address, "wrong", "disable", "disable")
					assertSQLState(t, err, "28P01")
				})
				t.Run(client.name+"/unknown-user", func(t *testing.T) {
					assertSQLState(t, client.connect(t, unknownAddress, "secret", "disable", "disable"), "28P01")
				})
				t.Run(client.name+"/invalid-verifier", func(t *testing.T) {
					assertSQLState(t, client.connect(t, invalidAddress, "secret", "disable", "disable"), "28P01")
				})
			}
		})
	}
}

func TestSCRAMSHA256PlusWithPGX(t *testing.T) {
	certificate, err := tls.LoadX509KeyPair("examples/tls/psql.crt", "examples/tls/psql.key")
	require.NoError(t, err)
	verifier, err := NewSCRAMSHA256Verifier("secret")
	require.NoError(t, err)
	authFn := func(ctx context.Context, database, username string) (context.Context, string, bool, error) {
		return ctx, verifier, username == "test", nil
	}

	versions := map[string]uint16{
		"TLS 1.2": tls.VersionTLS12,
		"TLS 1.3": tls.VersionTLS13,
	}
	for name, version := range versions {
		t.Run(name, func(t *testing.T) {
			config := &tls.Config{
				Certificates: []tls.Certificate{certificate},
				MinVersion:   version,
				MaxVersion:   version,
			}
			address := startAuthenticationServer(t, SCRAMSHA256(authFn), config)

			t.Run("channel binding required", func(t *testing.T) {
				require.NoError(t, connectPGXAuthClient(t, address, "secret", "require", "require"))
			})
			t.Run("wrong password", func(t *testing.T) {
				assertSQLState(t, connectPGXAuthClient(t, address, "wrong", "require", "require"), "28P01")
			})
			t.Run("base SCRAM fallback", func(t *testing.T) {
				require.NoError(t, connectPGXAuthClient(t, address, "secret", "require", "disable"))
			})
			t.Run("lib-pq base SCRAM fallback", func(t *testing.T) {
				require.NoError(t, connectPQAuthClient(t, address, "secret", "require", ""))
			})
		})
	}
}

func startAuthenticationServer(t *testing.T, strategy AuthStrategy, tlsConfig *tls.Config) *net.TCPAddr {
	t.Helper()
	handler := func(ctx context.Context, query Query) (PreparedStatements, error) {
		return Prepared(NewStatement(func(ctx context.Context, writer DataWriter, parameters []Parameter) error {
			return writer.Complete("SELECT 0")
		})), nil
	}
	options := []OptionFn{Logger(slogt.New(t)), SessionAuthStrategy(strategy)}
	if tlsConfig != nil {
		options = append(options, TLSConfig(tlsConfig))
	}
	server, err := NewServer(handler, options...)
	require.NoError(t, err)
	return TListenAndServe(t, server)
}

func connectPGXAuthClient(t *testing.T, address *net.TCPAddr, password, sslMode, channelBinding string) error {
	t.Helper()
	connectionString := fmt.Sprintf(
		"postgres://test:%s@%s/postgres?sslmode=%s&channel_binding=%s",
		password,
		address.String(),
		sslMode,
		channelBinding,
	)
	connection, err := pgx.Connect(context.Background(), connectionString)
	if err != nil {
		return err
	}
	defer connection.Close(context.Background()) //nolint:errcheck
	return connection.Ping(context.Background())
}

func connectPQAuthClient(t *testing.T, address *net.TCPAddr, password, sslMode, _ string) error {
	t.Helper()
	connectionString := fmt.Sprintf(
		"host=%s port=%d user=test password=%s dbname=postgres sslmode=%s",
		address.IP,
		address.Port,
		password,
		sslMode,
	)
	connection, err := sql.Open("postgres", connectionString)
	if err != nil {
		return err
	}
	defer connection.Close() //nolint:errcheck
	return connection.PingContext(context.Background())
}

func assertSQLState(t *testing.T, err error, expected string) {
	t.Helper()
	require.Error(t, err)
	var sqlState interface{ SQLState() string }
	require.True(t, errors.As(err, &sqlState), "error does not expose a SQLSTATE: %v", err)
	require.Equal(t, expected, sqlState.SQLState())
}
