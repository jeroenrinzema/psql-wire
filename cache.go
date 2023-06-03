package wire

import (
	"context"
	"sync"

	"github.com/jeroenrinzema/psql-wire/internal/buffer"
	"github.com/lib/pq/oid"
)

type Statement struct {
	fn         PreparedStatementFn
	parameters []oid.Oid
	columns    Columns
}

type DefaultStatementCache struct {
	statements map[string]*Statement
	mu         sync.RWMutex
}

// Set attempts to bind the given statement to the given name. Any
// previously defined statement is overridden.
func (cache *DefaultStatementCache) Set(ctx context.Context, name string, fn PreparedStatementFn, parameters []oid.Oid, columns Columns) error {
	cache.mu.Lock()
	defer cache.mu.Unlock()

	if cache.statements == nil {
		cache.statements = map[string]*Statement{}
	}

	cache.statements[name] = &Statement{
		fn:         fn,
		parameters: parameters,
		columns:    columns,
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

type portal struct {
	statement  *Statement
	parameters []string
}

type DefaultPortalCache struct {
	portals map[string]portal
	mu      sync.RWMutex
}

func (cache *DefaultPortalCache) Bind(ctx context.Context, name string, stmt *Statement, parameters []string) error {
	cache.mu.Lock()
	defer cache.mu.Unlock()

	if cache.portals == nil {
		cache.portals = map[string]portal{}
	}

	cache.portals[name] = portal{
		statement:  stmt,
		parameters: parameters,
	}

	return nil
}

func (cache *DefaultPortalCache) Get(ctx context.Context, name string) (*Statement, error) {
	cache.mu.Lock()
	defer cache.mu.Unlock()

	if cache.portals == nil {
		return nil, nil
	}

	portal, has := cache.portals[name]
	if !has {
		return nil, nil
	}

	return portal.statement, nil
}

func (cache *DefaultPortalCache) Execute(ctx context.Context, name string, writer *buffer.Writer) error {
	cache.mu.Lock()
	defer cache.mu.Unlock()

	portal, has := cache.portals[name]
	if !has {
		return nil
	}

	return portal.statement.fn(ctx, NewDataWriter(ctx, portal.statement.columns, writer), portal.parameters)
}
