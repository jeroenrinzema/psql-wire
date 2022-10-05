package wire

import (
	"context"

	"github.com/jackc/pgtype"
)

type ctxKey int

const (
	ctxTypeInfo ctxKey = iota
	ctxClientMetadata
	ctxServerMetadata
)

// setTypeInfo constructs a new Postgres type connection info for the given value
func setTypeInfo(ctx context.Context, info *pgtype.ConnInfo) context.Context {
	return context.WithValue(ctx, ctxTypeInfo, info)
}

// TypeInfo returns the Postgres type connection info if it has been set inside
// the given context.
func TypeInfo(ctx context.Context) *pgtype.ConnInfo {
	val := ctx.Value(ctxTypeInfo)
	if val == nil {
		return nil
	}

	return val.(*pgtype.ConnInfo)
}

// Parameters represents a parameters collection of parameter status keys and
// their values
type Parameters map[ParameterStatus]string

// ParameterStatus represents a metadata key that could be defined inside a server/client
// metadata definition
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
