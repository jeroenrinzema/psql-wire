package wire

import (
	"context"
	"sync"
)

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
}
