package wire

import (
	"context"
	"net"
	"testing"

	"github.com/jeroenrinzema/psql-wire/pkg/mock"
	"github.com/jeroenrinzema/psql-wire/pkg/types"
	"github.com/neilotoole/slogt"
	"github.com/stretchr/testify/require"
)

// versionGrease is the "grease" protocol version (3.9999) that recent libpq
// versions announce by default to verify that servers correctly implement
// protocol version negotiation.
var versionGrease = types.NewVersion(3, 9999)

func TestProtocolVersionNegotiation(t *testing.T) {
	t.Parallel()

	handler := func(ctx context.Context, query Query) (PreparedStatements, error) {
		return Prepared(NewStatement(func(ctx context.Context, writer DataWriter, parameters []Parameter) error {
			return writer.Complete("OK")
		})), nil
	}

	server, err := NewServer(handler, Logger(slogt.New(t)))
	require.NoError(t, err)
	address := TListenAndServe(t, server)

	tests := map[string]struct {
		version         types.Version
		params          []string
		expectNegotiate bool
		expectVersion   types.Version
		expectOptions   []string
	}{
		"current version without options": {
			version:         types.Version30,
			params:          []string{"user", "mock"},
			expectNegotiate: false,
		},
		"newer minor version": {
			version:         types.Version32,
			params:          []string{"user", "mock"},
			expectNegotiate: true,
			expectVersion:   types.Version30,
			expectOptions:   []string{},
		},
		"grease version with protocol option": {
			version:         versionGrease,
			params:          []string{"user", "mock", "_pq_.test_protocol_negotiation", ""},
			expectNegotiate: true,
			expectVersion:   types.Version30,
			expectOptions:   []string{"_pq_.test_protocol_negotiation"},
		},
		"current version with unrecognized protocol option": {
			version:         types.Version30,
			params:          []string{"user", "mock", "_pq_.test_protocol_negotiation", "on"},
			expectNegotiate: true,
			expectVersion:   types.Version30,
			expectOptions:   []string{"_pq_.test_protocol_negotiation"},
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			conn, err := net.Dial("tcp", address.String())
			require.NoError(t, err)

			client := mock.NewClient(t, conn)
			client.HandshakeProtocol(t, test.version, test.params...)

			if test.expectNegotiate {
				version, options := client.NegotiateProtocolVersion(t)
				require.Equal(t, test.expectVersion, version)
				require.Equal(t, test.expectOptions, options)
			}

			// Regardless of negotiation the connection must continue with the
			// regular startup flow. A NegotiateProtocolVersion message (when
			// present) is consumed above, so the next message is always the
			// authentication response.
			client.Authenticate(t)
			client.ReadyForQuery(t, types.ServerIdle)
			client.Close(t)
		})
	}
}

func TestUnsupportedProtocolMajorVersion(t *testing.T) {
	t.Parallel()

	handler := func(ctx context.Context, query Query) (PreparedStatements, error) {
		return Prepared(NewStatement(func(ctx context.Context, writer DataWriter, parameters []Parameter) error {
			return writer.Complete("OK")
		})), nil
	}

	server, err := NewServer(handler, Logger(slogt.New(t)))
	require.NoError(t, err)
	address := TListenAndServe(t, server)

	// A differing major version cannot be negotiated down, so the server must
	// reject the connection with an error rather than sending a
	// NegotiateProtocolVersion message.
	versions := map[string]types.Version{
		"older major version": types.NewVersion(2, 0),
		"newer major version": types.NewVersion(4, 2),
	}

	for name, version := range versions {
		t.Run(name, func(t *testing.T) {
			conn, err := net.Dial("tcp", address.String())
			require.NoError(t, err)

			client := mock.NewClient(t, conn)
			client.HandshakeProtocol(t, version, "user", "mock")
			client.Error(t, `unsupported frontend protocol`)
		})
	}
}
