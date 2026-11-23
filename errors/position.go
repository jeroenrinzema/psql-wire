package errors

import "errors"

// WithPosition decorates the error with a cursor position in the original
// query. Positions are one-based and measured in characters, not bytes.
// A non-positive position is omitted from the wire response.
func WithPosition(err error, position int32) error {
	if err == nil {
		return nil
	}

	return &withPosition{cause: err, position: position}
}

// GetPosition returns the outermost query position inside the given error,
// or zero if no position is set.
func GetPosition(err error) int32 {
	if p, ok := err.(*withPosition); ok {
		return p.position
	}

	if n := errors.Unwrap(err); n != nil {
		return GetPosition(n)
	}

	return 0
}

type withPosition struct {
	cause    error
	position int32
}

func (w *withPosition) Error() string { return w.cause.Error() }
func (w *withPosition) Unwrap() error { return w.cause }
