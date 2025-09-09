package wire

import (
	"context"
	"fmt"
	"sync"

	"github.com/jeroenrinzema/psql-wire/pkg/buffer"
	"github.com/lib/pq/oid"
)

type Statement struct {
	fn         PreparedStatementFn
	parameters []oid.Oid
	columns    Columns
}

func DefaultStatementCacheFn() StatementCache {
	return &DefaultStatementCache{}
}

type DefaultStatementCache struct {
	statements map[string]*Statement
	mu         sync.RWMutex
}

// Set attempts to bind the given statement to the given name. Any
// previously defined statement is overridden.
func (cache *DefaultStatementCache) Set(ctx context.Context, name string, stmt *PreparedStatement) error {
	cache.mu.Lock()
	defer cache.mu.Unlock()

	if cache.statements == nil {
		cache.statements = map[string]*Statement{}
	}

	cache.statements[name] = &Statement{
		fn:         stmt.fn,
		parameters: stmt.parameters,
		columns:    stmt.columns,
	}

	return nil
}

// Get attempts to get the prepared statement for the given name. An error
// is returned when no statement has been found.
func (cache *DefaultStatementCache) Get(ctx context.Context, name string) (*Statement, error) {
	cache.mu.RLock()
	defer cache.mu.RUnlock()

	if cache.statements == nil {
		return nil, nil
	}

	stmt, has := cache.statements[name]
	if !has {
		return nil, nil
	}

	return stmt, nil
}

func (cache *DefaultStatementCache) Close() {}

type Portal struct {
	statement  *Statement
	parameters []Parameter
	formats    []FormatCode
}

func DefaultPortalCacheFn() PortalCache {
	return &DefaultPortalCache{}
}

type DefaultPortalCache struct {
	portals map[string]*Portal
	mu      sync.RWMutex
}

func (cache *DefaultPortalCache) Bind(ctx context.Context, name string, stmt *Statement, parameters []Parameter, formats []FormatCode) error {
	cache.mu.Lock()
	defer cache.mu.Unlock()

	if cache.portals == nil {
		cache.portals = map[string]*Portal{}
	}

	// Optimize formats for binary-compatible types when client supports binary
	optimizedFormats := optimizeColumnFormats(stmt.columns, formats)

	// Debug logging can be enabled by uncommenting the line below
	// fmt.Printf("DEBUG: Portal.Bind - original formats: %v, optimized formats: %v, columns: %d\n", formats, optimizedFormats, len(stmt.columns))

	cache.portals[name] = &Portal{
		statement:  stmt,
		parameters: parameters,
		formats:    optimizedFormats,
	}

	return nil
}

func (cache *DefaultPortalCache) Get(ctx context.Context, name string) (*Portal, error) {
	cache.mu.Lock()
	defer cache.mu.Unlock()

	if cache.portals == nil {
		return nil, nil
	}

	portal, has := cache.portals[name]
	if !has {
		return nil, nil
	}

	return portal, nil
}

func (cache *DefaultPortalCache) Execute(ctx context.Context, name string, reader *buffer.Reader, writer *buffer.Writer) (err error) {
	defer func() {
		r := recover()
		if r != nil {
			err = fmt.Errorf("unexpected panic: %s", r)
		}
	}()

	cache.mu.Lock()
	defer cache.mu.Unlock()

	if cache.portals == nil {
		return nil
	}

	portal, has := cache.portals[name]
	if !has {
		return nil
	}

	return portal.statement.fn(ctx, NewDataWriter(ctx, portal.statement.columns, portal.formats, reader, writer), portal.parameters)
}

func (cache *DefaultPortalCache) Close() {}

// optimizeColumnFormats respects client format preferences and automatically enables binary
// for timestamp types when client sends empty formats (which indicates binaryTransfer=true)
func optimizeColumnFormats(columns Columns, clientFormats []FormatCode) []FormatCode {
	// Create formats array matching columns length exactly  
	optimized := make([]FormatCode, len(columns))

	for i, column := range columns {
		// Use exactly what the client requested, with proper fallbacks
		format := getClientRequestedFormat(clientFormats, i, len(columns))
		
		// If client sends empty formats array, they may have binaryTransfer=true
		// In this case, auto-enable binary for timestamp types
		if len(clientFormats) == 0 && isTimestampType(column.Oid) {
			format = BinaryFormat
		}
		
		optimized[i] = format
	}

	return optimized
}


// getClientRequestedFormat returns the format requested by the client for a specific column
// following PostgreSQL wire protocol semantics exactly
func getClientRequestedFormat(clientFormats []FormatCode, columnIndex int, totalColumns int) FormatCode {
	if len(clientFormats) == 0 {
		// No format codes specified - use default text format
		return TextFormat
	}
	
	if len(clientFormats) == 1 {
		// Single format code applies to all columns
		return clientFormats[0]
	}
	
	if columnIndex < len(clientFormats) {
		// Per-column format specified
		return clientFormats[columnIndex]
	}
	
	// Client provided fewer formats than columns - default remaining to text
	return TextFormat
}

// isTimestampType checks if the given OID is a timestamp type
func isTimestampType(columnOid oid.Oid) bool {
	return columnOid == oid.T_timestamp || columnOid == oid.T_timestamptz
}
