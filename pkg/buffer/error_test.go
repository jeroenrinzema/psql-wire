package buffer

import (
	"errors"
	"testing"
)

func TestErrMessageSizeExceeded(t *testing.T) {
	max := DefaultBufferSize
	size := max + 1024

	err := NewMessageSizeExceeded(max, size)

	if !errors.Is(err, ErrMessageSizeExceeded) {
		t.Error("unexpected comparison, error should contain message size exceeded type")
	}

	exceeded, has := UnwrapMessageSizeExceeded(err)
	if !has {
		t.Fatal("unexpected result, expected message size exceeded to be wrapped")
	}

	if exceeded.Max != max {
		t.Errorf("unexpected max size %d, expected %d", exceeded.Max, max)
	}

	if exceeded.Size != size {
		t.Errorf("unexpected message size %d, expected %d", exceeded.Size, size)
	}
}
