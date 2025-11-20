package wire

import (
	"context"
	"errors"
)

// ResultCollector implements DataWriter interface
// It collects query results in memory for later replay
type ResultCollector struct {
	columns Columns
	rows    [][]any
	tag     string
	empty   bool
	written uint32
	err     error
	limit   Limit
}

// Implement DataWriter interface

func (rc *ResultCollector) Row(values []any) error {
	if rc.err != nil {
		return rc.err
	}

	rc.rows = append(rc.rows, values)
	rc.written++
	return nil
}

func (rc *ResultCollector) Complete(tag string) error {
	rc.tag = tag
	return nil
}

func (rc *ResultCollector) Empty() error {
	rc.empty = true
	return nil
}

func (rc *ResultCollector) Columns() Columns {
	return rc.columns
}

func (rc *ResultCollector) Written() uint32 {
	return rc.written
}

func (rc *ResultCollector) CopyIn(format FormatCode) (*CopyReader, error) {
	return nil, errors.New("CopyIn not supported in pipeline mode")
}

func (rc *ResultCollector) Limit() uint32 {
	return uint32(rc.limit)
}

// SetError sets the error state
func (rc *ResultCollector) SetError(err error) {
	rc.err = err
}

// GetError gets the error state
func (rc *ResultCollector) GetError() error {
	return rc.err
}

// Replay writes all collected data to a real DataWriter
func (rc *ResultCollector) Replay(ctx context.Context, writer DataWriter) error {
	if rc.err != nil {
		return rc.err
	}

	// Write all collected rows
	for _, row := range rc.rows {
		if err := writer.Row(row); err != nil {
			return err
		}
	}

	// Send completion
	if rc.tag != "" {
		return writer.Complete(rc.tag)
	}

	return nil
}

// NewResultCollector creates a DataWriter that collects results
func NewResultCollector(ctx context.Context, columns Columns, limit Limit) *ResultCollector {
	return &ResultCollector{
		columns: columns,
		limit:   limit,
	}
}
