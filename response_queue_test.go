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

// TestResponseQueueFlushBoundary tests that Flush stops at incomplete Execute
func TestResponseQueueFlushBoundary(t *testing.T) {
	t.Parallel()

	queue := NewResponseQueue()

	// Scenario: Parse, Bind, Execute (incomplete), Parse, Bind
	// Flush should stop at incomplete Execute
	queue.Enqueue(NewParseCompleteEvent())
	queue.Enqueue(NewBindCompleteEvent())
	queue.Enqueue(NewExecuteEvent(make(chan *ResultCollector), nil)) // Empty channel = not ready
	queue.Enqueue(NewParseCompleteEvent())
	queue.Enqueue(NewBindCompleteEvent())

	// DrainFlushable should return only events before incomplete Execute
	flushable := queue.DrainFlushable()
	require.Len(t, flushable, 2, "should drain only Parse and Bind before Execute")

	assert.Equal(t, ResponseParseComplete, flushable[0].Kind)
	assert.Equal(t, ResponseBindComplete, flushable[1].Kind)

	// Queue should still have 3 events (Execute and subsequent events)
	assert.Equal(t, 3, queue.Len())

	// Drain remaining to verify they're still there
	remaining := queue.DrainAll()
	require.Len(t, remaining, 3)
	assert.Equal(t, ResponseExecute, remaining[0].Kind)
	assert.Equal(t, ResponseParseComplete, remaining[1].Kind)
	assert.Equal(t, ResponseBindComplete, remaining[2].Kind)
}

// TestResponseQueueNoExecuteBoundary tests Flush when there's no Execute boundary
func TestResponseQueueNoExecuteBoundary(t *testing.T) {
	t.Parallel()

	queue := NewResponseQueue()

	// Only control-plane events, no Execute
	queue.Enqueue(NewParseCompleteEvent())
	queue.Enqueue(NewStmtDescribeEvent(nil, nil))
	queue.Enqueue(NewBindCompleteEvent())

	// DrainFlushable should return all events when no Execute boundary exists
	flushable := queue.DrainFlushable()
	require.Len(t, flushable, 3, "should drain all events when no Execute boundary")

	// Queue should be empty
	assert.Equal(t, 0, queue.Len())
}

// TestResponseQueueStatementDescribe tests handling of Statement Describe events
func TestResponseQueueStatementDescribe(t *testing.T) {
	t.Parallel()

	queue := NewResponseQueue()

	// Statement Describe is a regular event that's always ready
	queue.Enqueue(NewParseCompleteEvent())
	queue.Enqueue(NewStmtDescribeEvent(
		[]oid.Oid{oid.T_int8},
		Columns{
			{Name: "id", Oid: oid.T_int8},
			{Name: "name", Oid: oid.T_text},
		},
	))
	queue.Enqueue(NewExecuteEvent(make(chan *ResultCollector), nil)) // Not ready

	// Flush should include Statement Describe but stop at incomplete Execute
	flushable := queue.DrainFlushable()
	require.Len(t, flushable, 2)

	assert.Equal(t, ResponseParseComplete, flushable[0].Kind)
	assert.Equal(t, ResponseStmtDescribe, flushable[1].Kind)
	assert.Len(t, flushable[1].Parameters, 1)
	assert.Len(t, flushable[1].Columns, 2)
}

// TestResponseQueuePortalDescribe tests handling of Portal Describe events
func TestResponseQueuePortalDescribe(t *testing.T) {
	t.Parallel()

	queue := NewResponseQueue()

	// Portal Describe is always ready
	queue.Enqueue(NewBindCompleteEvent())
	queue.Enqueue(NewPortalDescribeEvent(
		Columns{
			{Name: "result", Oid: oid.T_text},
		},
		[]FormatCode{TextFormat},
	))
	queue.Enqueue(NewExecuteEvent(make(chan *ResultCollector), nil)) // Not ready

	flushable := queue.DrainFlushable()
	require.Len(t, flushable, 2)

	assert.Equal(t, ResponseBindComplete, flushable[0].Kind)
	assert.Equal(t, ResponsePortalDescribe, flushable[1].Kind)
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

// TestResponseQueuePipelineSequence tests a realistic pipeline sequence
func TestResponseQueuePipelineSequence(t *testing.T) {
	t.Parallel()

	queue := NewResponseQueue()

	// Simulate: Parse A, Bind A, Describe Portal A, Execute A, Parse B, Bind B, Execute B
	queue.Enqueue(NewParseCompleteEvent())
	queue.Enqueue(NewBindCompleteEvent())
	queue.Enqueue(NewPortalDescribeEvent(nil, nil))
	queue.Enqueue(NewExecuteEvent(make(chan *ResultCollector), nil)) // Not ready
	queue.Enqueue(NewParseCompleteEvent())
	queue.Enqueue(NewBindCompleteEvent())
	queue.Enqueue(NewExecuteEvent(make(chan *ResultCollector), nil)) // Not ready

	// First Flush stops at first incomplete Execute
	flush1 := queue.DrainFlushable()
	require.Len(t, flush1, 3, "should get Parse, Bind, and Portal Describe for A")
	assert.Equal(t, ResponseParseComplete, flush1[0].Kind)
	assert.Equal(t, ResponseBindComplete, flush1[1].Kind)
	assert.Equal(t, ResponsePortalDescribe, flush1[2].Kind)

	// Second Flush immediately hits the first Execute (still incomplete), returns nothing
	flush2 := queue.DrainFlushable()
	require.Len(t, flush2, 0, "should get nothing - blocked by incomplete Execute")

	// Queue should still have all remaining events
	remaining := queue.DrainAll()
	require.Len(t, remaining, 4)
	assert.Equal(t, ResponseExecute, remaining[0].Kind)
	assert.Equal(t, ResponseParseComplete, remaining[1].Kind)
	assert.Equal(t, ResponseBindComplete, remaining[2].Kind)
	assert.Equal(t, ResponseExecute, remaining[3].Kind)
}

// TestResponseQueueCompletedFlush tests the completed-only flush policy
func TestResponseQueueCompletedFlush(t *testing.T) {
	t.Parallel()

	queue := NewResponseQueue()

	// Two executes; first completed before Flush, second pending
	queue.Enqueue(NewParseCompleteEvent())
	queue.Enqueue(NewBindCompleteEvent())
	queue.Enqueue(NewPortalDescribeEvent(nil, nil))

	// Create result channel with completed result
	resultChan1 := make(chan *ResultCollector, 1)
	resultChan1 <- &ResultCollector{} // Simulate completed result
	queue.Enqueue(NewExecuteEvent(resultChan1, nil))

	queue.Enqueue(NewParseCompleteEvent())
	queue.Enqueue(NewBindCompleteEvent())
	queue.Enqueue(NewPortalDescribeEvent(nil, nil))

	// Second Execute not ready yet
	resultChan2 := make(chan *ResultCollector, 1)
	queue.Enqueue(NewExecuteEvent(resultChan2, nil)) // Empty channel = not ready

	// Flush should emit all events up to the first incomplete Execute
	// This includes Q1's complete result but stops before Q2's incomplete Execute
	events := queue.DrainFlushable()

	require.Len(t, events, 7, "should get all events for Q1")
	assert.Equal(t, ResponseParseComplete, events[0].Kind)
	assert.Equal(t, ResponseBindComplete, events[1].Kind)
	assert.Equal(t, ResponsePortalDescribe, events[2].Kind)
	assert.Equal(t, ResponseExecute, events[3].Kind)
	assert.NotNil(t, events[3].Result, "Execute result should be cached")

	// Queue should still have Q2's events
	remaining := queue.DrainAll()
	require.Len(t, remaining, 1, "Q2's events should remain")
	assert.Equal(t, ResponseExecute, remaining[0].Kind)
}

// TestResponseQueueMultipleCompletedFlush tests flushing multiple completed executes
func TestResponseQueueMultipleCompletedFlush(t *testing.T) {
	t.Parallel()

	queue := NewResponseQueue()

	// Two completed executes followed by an incomplete one
	queue.Enqueue(NewParseCompleteEvent())
	queue.Enqueue(NewBindCompleteEvent())
	resultChan1 := make(chan *ResultCollector, 1)
	resultChan1 <- &ResultCollector{} // Completed
	queue.Enqueue(NewExecuteEvent(resultChan1, nil))

	queue.Enqueue(NewParseCompleteEvent())
	queue.Enqueue(NewBindCompleteEvent())
	resultChan2 := make(chan *ResultCollector, 1)
	resultChan2 <- &ResultCollector{} // Also completed
	queue.Enqueue(NewExecuteEvent(resultChan2, nil))

	queue.Enqueue(NewParseCompleteEvent())
	queue.Enqueue(NewBindCompleteEvent())
	resultChan3 := make(chan *ResultCollector) // Not ready (no buffer)
	queue.Enqueue(NewExecuteEvent(resultChan3, nil))

	// Flush should get both completed executes and stop at the incomplete one
	events := queue.DrainFlushable()

	// Should get all events through Q2 (both completed)
	require.Len(t, events, 8, "should get all events for Q1 and Q2")
	assert.Equal(t, ResponseParseComplete, events[0].Kind)
	assert.Equal(t, ResponseBindComplete, events[1].Kind)
	assert.Equal(t, ResponseExecute, events[2].Kind)
	assert.Equal(t, ResponseParseComplete, events[3].Kind)
	assert.Equal(t, ResponseExecute, events[5].Kind)

	// Q3 and its control-plane remain
	remaining := queue.DrainAll()
	assert.Equal(t, 1, len(remaining), "only Q3's Execute should remain")
	assert.Equal(t, ResponseExecute, remaining[0].Kind)
}

// TestDrainSyncNormalOperation tests successful draining of all events
func TestDrainSyncNormalOperation(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	queue := NewResponseQueue()

	// Add some control events
	queue.Enqueue(NewParseCompleteEvent())
	queue.Enqueue(NewBindCompleteEvent())

	// Add Execute events with results ready
	resultChan1 := make(chan *ResultCollector, 1)
	result1 := &ResultCollector{}
	result1.rows = [][]any{{"value1"}, {"value2"}}
	resultChan1 <- result1

	queue.Enqueue(NewExecuteEvent(resultChan1, nil))

	// Add another control event
	queue.Enqueue(NewParseCompleteEvent())

	// Add another Execute
	resultChan2 := make(chan *ResultCollector, 1)
	result2 := &ResultCollector{}
	result2.rows = [][]any{{"value3"}}
	resultChan2 <- result2

	queue.Enqueue(NewExecuteEvent(resultChan2, nil))

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

	// Add Execute with successful result
	resultChan1 := make(chan *ResultCollector, 1)
	result1 := &ResultCollector{}
	result1.rows = [][]any{{"success"}}
	resultChan1 <- result1

	queue.Enqueue(NewExecuteEvent(resultChan1, nil))

	// Add Execute with error result
	resultChan2 := make(chan *ResultCollector, 1)
	result2 := &ResultCollector{}
	testError := errors.New("query execution failed")
	result2.SetError(testError)
	resultChan2 <- result2

	queue.Enqueue(NewExecuteEvent(resultChan2, nil))

	// Add more events that shouldn't be processed
	queue.Enqueue(NewParseCompleteEvent())

	resultChan3 := make(chan *ResultCollector, 1)
	result3 := &ResultCollector{}
	resultChan3 <- result3

	queue.Enqueue(NewExecuteEvent(resultChan3, nil))

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

	// Add Execute with successful result (instant)
	resultChan1 := make(chan *ResultCollector, 1)
	result1 := &ResultCollector{}
	resultChan1 <- result1

	queue.Enqueue(NewExecuteEvent(resultChan1, nil))

	// Add Execute that will block (no result sent)
	resultChan2 := make(chan *ResultCollector)

	queue.Enqueue(NewExecuteEvent(resultChan2, nil))

	// Add more events that shouldn't be reached
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
	resultChan := make(chan *ResultCollector)

	queue.Enqueue(NewExecuteEvent(resultChan, nil))

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
	queue.Enqueue(NewStmtDescribeEvent(nil, nil))
	queue.Enqueue(NewPortalDescribeEvent(nil, nil))

	events, err := queue.DrainSync(ctx)

	require.NoError(t, err)
	assert.Len(t, events, 4)

	// All events should be present and in order
	assert.Equal(t, ResponseParseComplete, events[0].Kind)
	assert.Equal(t, ResponseBindComplete, events[1].Kind)
	assert.Equal(t, ResponseStmtDescribe, events[2].Kind)
	assert.Equal(t, ResponsePortalDescribe, events[3].Kind)
}
