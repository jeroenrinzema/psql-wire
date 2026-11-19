package wire

import (
	"context"
	"crypto/tls"
	"net"

	"github.com/jackc/pgx/v5/pgtype"
)

type ctxKey int

const (
	ctxTypeMap ctxKey = iota
	ctxClientMetadata
	ctxServerMetadata
	ctxRemoteAddr
	ctxTLSConnectionState
)

// setTypeInfo constructs a new Postgres type connection info for the given value
func setTypeInfo(ctx context.Context, info *pgtype.Map) context.Context {
	return context.WithValue(ctx, ctxTypeMap, info)
}

// TypeMap returns the Postgres type connection info if it has been set inside
// the given context.
func TypeMap(ctx context.Context) *pgtype.Map {
	val := ctx.Value(ctxTypeMap)
	if val == nil {
		return nil
	}

	return val.(*pgtype.Map)
}

func setRemoteAddress(ctx context.Context, addr net.Addr) context.Context {
	return context.WithValue(ctx, ctxRemoteAddr, addr)
}

// RemoteAddress returns the Postgres remote address connection info if it has been set inside
// the given context.
func RemoteAddress(ctx context.Context) net.Addr {
	val := ctx.Value(ctxRemoteAddr)
	if val == nil {
		return nil
	}

	return val.(net.Addr)
}

func setTLSConnectionState(ctx context.Context, state *tls.ConnectionState) context.Context {
	return context.WithValue(ctx, ctxTLSConnectionState, state)
}

// TLSConnectionState returns the state of the TLS connection, including any
// client certificates presented by the client, if the connection has been
// upgraded to TLS. It returns nil for plaintext connections. The state is
// available to the authentication strategy, the session middleware and the
// query handlers.
//
// PeerCertificates holds whatever the client presented; only VerifiedChains is
// verified, and only when the [TLSConfig] ClientAuth verifies certificates or
// the caller verifies them in VerifyConnection.
func TLSConnectionState(ctx context.Context) *tls.ConnectionState {
	val := ctx.Value(ctxTLSConnectionState)
	if val == nil {
		return nil
	}

	return val.(*tls.ConnectionState)
}

// Parameters represents a parameters collection of parameter status keys and
// their values.
type Parameters map[ParameterStatus]string

// ParameterStatus represents a metadata key that could be defined inside a server/client
// metadata definition.
type ParameterStatus string

// At present there is a hard-wired set of parameters for which ParameterStatus
// will be generated.
// https://www.postgresql.org/docs/13/protocol-flow.html#PROTOCOL-ASYNC
const (
	ParamServerEncoding       ParameterStatus = "server_encoding"
	ParamClientEncoding       ParameterStatus = "client_encoding"
	ParamIsSuperuser          ParameterStatus = "is_superuser"
	ParamSessionAuthorization ParameterStatus = "session_authorization"
	ParamApplicationName      ParameterStatus = "application_name"
	ParamDatabase             ParameterStatus = "database"
	ParamUsername             ParameterStatus = "user"
	ParamServerVersion        ParameterStatus = "server_version"
)

// setClientParameters constructs a new context containing the given parameters.
// Any previously defined metadata will be overriden.
func setClientParameters(ctx context.Context, params Parameters) context.Context {
	if params == nil {
		return ctx
	}

	return context.WithValue(ctx, ctxClientMetadata, params)
}

// ClientParameters returns the connection parameters if it has been set inside
// the given context.
func ClientParameters(ctx context.Context) Parameters {
	val := ctx.Value(ctxClientMetadata)
	if val == nil {
		return nil
	}

	return val.(Parameters)
}

// setServerParameters constructs a new context containing the given parameters map.
// Any previously defined metadata will be overriden.
func setServerParameters(ctx context.Context, params Parameters) context.Context {
	if params == nil {
		return ctx
	}

	return context.WithValue(ctx, ctxServerMetadata, params)
}

// ServerParameters returns the connection parameters if it has been set inside
// the given context.
func ServerParameters(ctx context.Context) Parameters {
	val := ctx.Value(ctxServerMetadata)
	if val == nil {
		return nil
	}

	return val.(Parameters)
}
