package wire

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"sync"

	"github.com/jeroenrinzema/psql-wire/pkg/buffer"
	"github.com/jeroenrinzema/psql-wire/pkg/types"
)

// Limit represents the maximum number of rows to be written.
// Zero denotes "no limit".
type Limit uint32

const NoLimit Limit = 0

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

	// The iterator state (created by iter.Pull)
	next func() (struct{}, bool)
	stop func()
	// Filled in by dataWriter.Complete when the handler finishes. Used to
	// return the tag when re-executing a completed portal.
	tag string
	err error
	// Set to true when execution of the portal has finished.
	done bool

	// pending is closed when the most recently launched async goroutine
	// finishes. A new goroutine for the same portal waits on this channel
	// before starting, so same-portal executes serialize while different
	// portals run in parallel.
	pending chan struct{}
}

// Close tears down the portal from outside the execution goroutine (e.g.
// explicit Close message or portal re-bind). If an async goroutine is still
// running, cleanup is deferred to a background goroutine that waits for it.
func (p *Portal) Close() {
	if p.pending != nil {
		pending := p.pending
		go func() {
			<-pending
			p.close()
		}()
		return
	}
	p.close()
}

// close tears down the iterator inline. Used from within execute where we
// are already inside the execution context and don't need to wait on pending.
func (p *Portal) close() {
	if p.stop != nil {
		p.stop()
		p.next = nil
		p.stop = nil
	}
}

func portalSuspended(writer *buffer.Writer) error {
	writer.Start(types.ServerPortalSuspended)
	return writer.End()
}

func (p *Portal) execute(ctx context.Context, limit Limit, reader *buffer.Reader, writer *buffer.Writer) error {
	if p.done {
		// Re-executing an already completed portal simply returns the tag or
		// error again.
		if p.err != nil {
			return p.err
		}
		return commandComplete(writer, p.tag)
	}

	if p.next == nil {
		// This is the first execute call on this portal. So let's start the
		// execution. Otherwise we continue from where we left off.
		session, _ := GetSession(ctx)
		// Create a simple push-style iterator (iter.Seq) around the
		// statement.fn.
		seq := func(yield func(struct{}) bool) {
			dw := &dataWriter{
				ctx:     ctx,
				session: session,
				columns: p.statement.columns,
				formats: p.formats,
				reader:  reader,
				client:  writer,
				yield:   yield,
				tag:     &p.tag,
			}
			err := p.statement.fn(ctx, dw, p.parameters)
			if err != nil && !errors.Is(err, ErrSuspendedHandlerClosed) {
				p.err = err
			}
		}

		// Then we convert that push-style iterator into a pull-style iterator,
		// so we can suspend the iterator when we reach the row limit.
		p.next, p.stop = iter.Pull(seq)
	}

	var count Limit
	for {
		if limit != NoLimit && count >= limit {
			// We've reached the row limit. Suspend the portal and let the
			// client know, so it can either issue a new Execute to continue or
			// close the portal.
			return portalSuspended(writer)
		}

		// Run the handler until it has produced the next row or finishes. The
		// dataWriter inside the handler will call yield to "teleport" back
		// here.
		_, ok := p.next()
		if !ok {
			// The handler has finished. CommandComplete was already written
			// by dataWriter.Complete.
			p.close()
			p.done = true
			return p.err
		}

		count++
	}
}

func DefaultPortalCacheFn() PortalCache {
	return &DefaultPortalCache{}
}

type DefaultPortalCache struct {
	portals      map[string]*Portal
	executing    *Portal
	closePending bool
	mu           sync.RWMutex
}

func (cache *DefaultPortalCache) Bind(ctx context.Context, name string, stmt *Statement, parameters []Parameter, formats []FormatCode) error {
	cache.mu.Lock()
	defer cache.mu.Unlock()

	if cache.portals == nil {
		cache.portals = map[string]*Portal{}
	}

	if existing, ok := cache.portals[name]; ok {
		existing.Close()
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

	cache.mu.Lock()
	cache.executing = portal
	cache.mu.Unlock()

	err = portal.execute(ctx, limit, reader, writer)

	cache.mu.Lock()
	cache.executing = nil
	needsClose := cache.closePending
	cache.closePending = false
	cache.mu.Unlock()
	if needsClose {
		portal.close()
	}

	return err
}

// closePortal closes the portal immediately, unless it is the currently
// executing portal, in which case it marks the portal for deferred closing
// after the current Execute call returns.
func (cache *DefaultPortalCache) closePortal(portal *Portal) {
	if portal == cache.executing {
		cache.closePending = true
	} else {
		portal.Close()
	}
}

func (cache *DefaultPortalCache) Delete(ctx context.Context, name string) error {
	cache.mu.Lock()
	defer cache.mu.Unlock()
	if portal, ok := cache.portals[name]; ok {
		cache.closePortal(portal)
	}
	delete(cache.portals, name)
	return nil
}

func (cache *DefaultPortalCache) DeleteByStatement(ctx context.Context, stmt *Statement) error {
	cache.mu.Lock()
	defer cache.mu.Unlock()
	for name, portal := range cache.portals {
		if portal.statement == stmt {
			cache.closePortal(portal)
			delete(cache.portals, name)
		}
	}
	return nil
}

func (cache *DefaultPortalCache) Close() {
	cache.mu.Lock()
	defer cache.mu.Unlock()
	for _, portal := range cache.portals {
		cache.closePortal(portal)
	}
	clear(cache.portals)
}
