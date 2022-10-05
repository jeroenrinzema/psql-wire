package wire

import (
	"context"
	"errors"
	"sync"
)

// ErrStatementAlreadyExists is thrown whenever an prepared statement already
// exists within the given statement cache.
var ErrStatementAlreadyExists = errors.New("prepared statement already exists")

type DefaultStatementCache struct {
	statements map[string]PreparedStatementFn
	mu         sync.RWMutex
}

// Set attempts to bind the given statement to the given name. Any
// previously defined statement is overridden.
func (cache *DefaultStatementCache) Set(ctx context.Context, name string, fn PreparedStatementFn) error {
	cache.mu.Lock()
	defer cache.mu.Unlock()

	if cache.statements == nil {
		cache.statements = map[string]PreparedStatementFn{}
	}

	_, has := cache.statements[name]
	if has {
		return ErrStatementAlreadyExists
	}

	cache.statements[name] = fn
	return nil
}

// Get attempts to get the prepared statement for the given name. An error
// is returned when no statement has been found.
func (cache *DefaultStatementCache) Get(ctx context.Context, name string) (PreparedStatementFn, error) {
	cache.mu.RLock()
	defer cache.mu.RUnlock()

	if cache.statements == nil {
		return nil, nil
	}

	return cache.statements[name], nil
}

type DefaultPortalCache struct {
	portals map[string]PreparedStatementFn
	mu      sync.RWMutex
}

func (cache *DefaultPortalCache) Bind(ctx context.Context, name string, fn PreparedStatementFn) error {
	cache.mu.Lock()
	defer cache.mu.Unlock()

	if cache.portals == nil {
		cache.portals = map[string]PreparedStatementFn{}
	}

	cache.portals[name] = fn
	return nil
}

func (cache *DefaultPortalCache) Execute(ctx context.Context, name string, writer DataWriter) error {
	cache.mu.Lock()
	defer cache.mu.Unlock()

	fn, has := cache.portals[name]
	if !has {
		return nil
	}

	return fn(ctx, writer)
}
