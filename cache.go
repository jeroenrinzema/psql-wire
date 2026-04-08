package wire

import (
	"context"
	"fmt"
	"sync"

	"github.com/jeroenrinzema/psql-wire/pkg/buffer"
)

type Statement struct {
	fn         PreparedStatementFn
	parameters []uint32
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

func (cache *DefaultStatementCache) Delete(ctx context.Context, name string) error {
	cache.mu.Lock()
	defer cache.mu.Unlock()
	delete(cache.statements, name)
	return nil
}

func (cache *DefaultStatementCache) Close() {
	cache.mu.Lock()
	defer cache.mu.Unlock()
	clear(cache.statements)
}

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

	cache.portals[name] = &Portal{
		statement:  stmt,
		parameters: parameters,
		formats:    formats,
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

func (cache *DefaultPortalCache) Execute(ctx context.Context, name string, limit Limit, reader *buffer.Reader, writer *buffer.Writer) (err error) {
	defer func() {
		r := recover()
		if r != nil {
			err = fmt.Errorf("unexpected panic: %s", r)
		}
	}()

	// Look up the portal under the lock, then release before executing.
	// The handler may call back into the cache so we must make sure we don't
	// hold the lock for the execute duration. e.g. a handler for DEALLOCATE
	// may call back into the cache to delete a portal.
	cache.mu.RLock()
	if cache.portals == nil {
		cache.mu.RUnlock()
		return nil
	}

	portal, has := cache.portals[name]
	cache.mu.RUnlock()
	if !has {
		return nil
	}

	session, _ := GetSession(ctx)
	return portal.statement.fn(ctx, NewDataWriter(ctx, session, portal.statement.columns, portal.formats, limit, reader, writer), portal.parameters)
}

func (cache *DefaultPortalCache) Delete(ctx context.Context, name string) error {
	cache.mu.Lock()
	defer cache.mu.Unlock()
	delete(cache.portals, name)
	return nil
}

func (cache *DefaultPortalCache) DeleteByStatement(ctx context.Context, stmt *Statement) error {
	cache.mu.Lock()
	defer cache.mu.Unlock()
	for name, portal := range cache.portals {
		if portal.statement == stmt {
			delete(cache.portals, name)
		}
	}
	return nil
}

func (cache *DefaultPortalCache) Close() {
	cache.mu.Lock()
	defer cache.mu.Unlock()
	clear(cache.portals)
}
