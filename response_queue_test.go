package wire

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/lib/pq/oid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newPendingExecuteEvent creates an Execute event that blocks (not ready).
func newPendingExecuteEvent() *ResponseEvent {
	return NewExecuteEvent(make(chan *QueuedDataWriter), nil)
}

// newReadyExecuteEvent creates an Execute event with a completed result.
func newReadyExecuteEvent(rows [][]any) *ResponseEvent {
	ch := make(chan *QueuedDataWriter, 1)
	ch <- &QueuedDataWriter{rows: rows}
	return NewExecuteEvent(ch, nil)
}

// newErrorExecuteEvent creates an Execute event with an error result.
func newErrorExecuteEvent(err error) *ResponseEvent {
	ch := make(chan *QueuedDataWriter, 1)
	result := &QueuedDataWriter{}
	result.SetError(err)
	ch <- result
	return NewExecuteEvent(ch, nil)
}

// TestResponseQueueBasicOperations tests enqueue and drain operations
func TestResponseQueueBasicOperations(t *testing.T) {
	t.Parallel()

	queue := NewResponseQueue()

	// Enqueue some control-plane events
	queue.Enqueue(NewParseCompleteEvent())
	queue.Enqueue(NewBindCompleteEvent())

	assert.Equal(t, 2, queue.Len(), "queue should have 2 events")

	// Drain all events
	events := queue.DrainAll()
	require.Len(t, events, 2)

	// Verify event types and order
	assert.Equal(t, ResponseParseComplete, events[0].Kind)
	assert.Equal(t, ResponseBindComplete, events[1].Kind)

	// Queue should be empty after drain
	assert.Equal(t, 0, queue.Len())
}


// TestResponseQueueClear tests clearing the queue for a new cycle
func TestResponseQueueClear(t *testing.T) {
	t.Parallel()

	queue := NewResponseQueue()

	// Add some events
	queue.Enqueue(NewParseCompleteEvent())
	queue.Enqueue(NewBindCompleteEvent())

	assert.Equal(t, 2, queue.Len())

	// Clear the queue
	queue.Clear()

	assert.Equal(t, 0, queue.Len())

	// New events should work after clear
	queue.Enqueue(NewParseCompleteEvent())
	events := queue.DrainAll()
	assert.Equal(t, ResponseParseComplete, events[0].Kind)
}

// TestDrainSyncNormalOperation tests successful draining of all events
func TestDrainSyncNormalOperation(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	queue := NewResponseQueue()

	// Add some control events
	queue.Enqueue(NewParseCompleteEvent())
	queue.Enqueue(NewBindCompleteEvent())
	queue.Enqueue(newReadyExecuteEvent([][]any{{"value1"}, {"value2"}}))
	queue.Enqueue(NewParseCompleteEvent())
	queue.Enqueue(newReadyExecuteEvent([][]any{{"value3"}}))

	// Drain all events
	events, err := queue.DrainSync(ctx)
	require.NoError(t, err)
	require.Len(t, events, 5)

	// Verify all events and their results
	assert.Equal(t, ResponseParseComplete, events[0].Kind)
	assert.Equal(t, ResponseBindComplete, events[1].Kind)
	assert.Equal(t, ResponseExecute, events[2].Kind)
	assert.NotNil(t, events[2].Result)
	assert.Len(t, events[2].Result.rows, 2)
	assert.Equal(t, ResponseParseComplete, events[3].Kind)
	assert.Equal(t, ResponseExecute, events[4].Kind)
	assert.NotNil(t, events[4].Result)
	assert.Len(t, events[4].Result.rows, 1)
}

// TestDrainSyncWithError tests early exit on error
func TestDrainSyncWithError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	queue := NewResponseQueue()

	// Add some control events
	queue.Enqueue(NewParseCompleteEvent())
	queue.Enqueue(NewBindCompleteEvent())
	queue.Enqueue(newReadyExecuteEvent([][]any{{"success"}}))

	// Add Execute with error result
	testError := errors.New("query execution failed")
	queue.Enqueue(newErrorExecuteEvent(testError))

	// Add more events that shouldn't be processed
	queue.Enqueue(NewParseCompleteEvent())
	queue.Enqueue(newReadyExecuteEvent(nil))

	// Drain should stop at the error
	events, err := queue.DrainSync(ctx)

	// Should return the error from the failed result
	require.Error(t, err)
	assert.Equal(t, testError, err)

	// Should only return events up to but not including the error (3 events)
	assert.Len(t, events, 3)

	// First Execute should have its result
	assert.NotNil(t, events[2].Result)
	assert.NoError(t, events[2].Result.GetError())
}

// TestDrainSyncContextCancellation tests context cancellation during drain
func TestDrainSyncContextCancellation(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	queue := NewResponseQueue()

	// Add some events
	queue.Enqueue(NewParseCompleteEvent())
	queue.Enqueue(NewBindCompleteEvent())
	queue.Enqueue(newReadyExecuteEvent(nil))
	queue.Enqueue(newPendingExecuteEvent())
	queue.Enqueue(NewParseCompleteEvent())

	// Cancel context after a short delay
	go func() {
		time.Sleep(100 * time.Millisecond)
		cancel()
	}()

	// Drain should be interrupted by context cancellation
	events, err := queue.DrainSync(ctx)

	// Should return context error
	require.Error(t, err)
	assert.Equal(t, context.Canceled, err)

	// Should return events up to but not including the error event
	assert.Len(t, events, 3)

	// First Execute should have been processed
	assert.NotNil(t, events[2].Result)
}

// TestDrainSyncWithTimeout tests timeout handling
func TestDrainSyncWithTimeout(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	queue := NewResponseQueue()

	// Add Execute that will block forever
	queue.Enqueue(newPendingExecuteEvent())

	start := time.Now()
	events, err := queue.DrainSync(ctx)
	elapsed := time.Since(start)

	// Should timeout
	require.Error(t, err)
	assert.Equal(t, context.DeadlineExceeded, err)

	// Should have taken about the timeout duration
	assert.InDelta(t, 200, elapsed.Milliseconds(), 50)

	// Should return no events
	assert.Len(t, events, 0)
}

// TestDrainSyncEmptyQueue tests draining an empty queue
func TestDrainSyncEmptyQueue(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	queue := NewResponseQueue()

	events, err := queue.DrainSync(ctx)

	require.NoError(t, err)
	assert.Empty(t, events)
}

// TestDrainSyncNoExecuteEvents tests draining with only control events
func TestDrainSyncNoExecuteEvents(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	queue := NewResponseQueue()

	// Add only control events (no Execute events)
	queue.Enqueue(NewParseCompleteEvent())
	queue.Enqueue(NewBindCompleteEvent())
	queue.Enqueue(NewStmtDescribeEvent(
		[]oid.Oid{oid.T_int4, oid.T_text},
		Columns{
			{Name: "id", Oid: oid.T_int8},
			{Name: "name", Oid: oid.T_text},
		},
	))
	queue.Enqueue(NewPortalDescribeEvent(
		Columns{{Name: "result", Oid: oid.T_text}},
		[]FormatCode{BinaryFormat},
	))

	events, err := queue.DrainSync(ctx)

	require.NoError(t, err)
	require.Len(t, events, 4)

	// All events should be present and in order
	assert.Equal(t, ResponseParseComplete, events[0].Kind)
	assert.Equal(t, ResponseBindComplete, events[1].Kind)

	// Verify StmtDescribe data
	assert.Equal(t, ResponseStmtDescribe, events[2].Kind)
	assert.Equal(t, []oid.Oid{oid.T_int4, oid.T_text}, events[2].Parameters)
	require.Len(t, events[2].Columns, 2)
	assert.Equal(t, "id", events[2].Columns[0].Name)

	// Verify PortalDescribe data
	assert.Equal(t, ResponsePortalDescribe, events[3].Kind)
	require.Len(t, events[3].Columns, 1)
	assert.Equal(t, "result", events[3].Columns[0].Name)
	assert.Equal(t, []FormatCode{BinaryFormat}, events[3].Formats)
}
