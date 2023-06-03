package buffer

import (
	"bytes"
	"errors"
	"math"
	"testing"

	"github.com/jeroenrinzema/psql-wire/internal/types"
	"go.uber.org/zap"
)

func TestNewWriterNil(t *testing.T) {
	NewWriter(zap.NewNop(), nil)
}

func TestWriteMsg(t *testing.T) {
	buffer := bytes.NewBuffer([]byte{})
	writer := NewWriter(zap.NewNop(), buffer)

	writer.Start(types.ServerDataRow)
	writer.AddString("John Doe")
	writer.AddNullTerminate()
	err := writer.End()
	if err != nil {
		t.Error(err)
	}

	if len(writer.Bytes()) != 0 {
		t.Errorf("unexpected bytes %+v, expected the writer to be empty", writer.Bytes())
	}

	if writer.Error() != nil {
		t.Error(writer.Error())
	}
}

func TestWriteMsgErr(t *testing.T) {
	expected := errors.New("unexpected error")

	buffer := bytes.NewBuffer([]byte{})
	writer := NewWriter(zap.NewNop(), buffer)

	writer.Start(types.ServerDataRow)
	writer.err = expected

	writer.AddString("John Doe")
	writer.AddNullTerminate()
	err := writer.End()
	if err != expected {
		t.Errorf("unexpected error %s, expected %s", err, expected)
	}

	if len(writer.Bytes()) != 0 {
		t.Errorf("unexpected bytes %+v, expected the writer to be empty", writer.Bytes())
	}

	if writer.Error() != nil {
		t.Errorf("unexpected error %s, error should be empty after end", writer.Error())
	}
}

func TestWriteTypes(t *testing.T) {
	buffer := bytes.NewBuffer([]byte{})
	writer := NewWriter(zap.NewNop(), buffer)

	t.Run("byte", func(t *testing.T) {
		writer.AddByte(byte(types.ServerAuth))
		if writer.Error() != nil {
			t.Error(writer.Error())
		}
	})

	t.Run("bytes", func(t *testing.T) {
		writer.AddBytes([]byte("John Doe"))
		if writer.Error() != nil {
			t.Error(writer.Error())
		}
	})

	t.Run("string", func(t *testing.T) {
		writer.AddString("John Doe")
		writer.AddNullTerminate()
		if writer.Error() != nil {
			t.Error(writer.Error())
		}
	})

	t.Run("int16", func(t *testing.T) {
		writer.AddInt16(math.MaxInt16)
		if writer.Error() != nil {
			t.Error(writer.Error())
		}
	})

	t.Run("int32", func(t *testing.T) {
		writer.AddInt32(math.MaxInt32)
		if writer.Error() != nil {
			t.Error(writer.Error())
		}
	})
}

func TestWriteTypesErr(t *testing.T) {
	expected := errors.New("unexpected error")

	buffer := bytes.NewBuffer([]byte{})
	writer := NewWriter(zap.NewNop(), buffer)
	writer.err = expected

	t.Run("byte", func(t *testing.T) {
		writer.AddByte(byte(types.ServerAuth))
		if writer.Error() != expected {
			t.Errorf("unexpected err %s, expected %s", writer.Error(), expected)
		}

		if len(writer.Bytes()) != 0 {
			t.Fatalf("unexpected bytes, no bytes should have been written")
		}
	})

	t.Run("bytes", func(t *testing.T) {
		writer.AddBytes([]byte("John Doe"))
		if writer.Error() != expected {
			t.Errorf("unexpected err %s, expected %s", writer.Error(), expected)
		}

		if len(writer.Bytes()) != 0 {
			t.Fatalf("unexpected bytes, no bytes should have been written")
		}
	})

	t.Run("string", func(t *testing.T) {
		writer.AddString("John Doe")
		writer.AddNullTerminate()
		if writer.Error() != expected {
			t.Errorf("unexpected err %s, expected %s", writer.Error(), expected)
		}

		if len(writer.Bytes()) != 0 {
			t.Fatalf("unexpected bytes, no bytes should have been written")
		}
	})

	t.Run("int16", func(t *testing.T) {
		writer.AddInt16(math.MaxInt16)
		if writer.Error() != expected {
			t.Errorf("unexpected err %s, expected %s", writer.Error(), expected)
		}

		if len(writer.Bytes()) != 0 {
			t.Fatalf("unexpected bytes, no bytes should have been written")
		}
	})

	t.Run("int32", func(t *testing.T) {
		writer.AddInt32(math.MaxInt32)
		if writer.Error() != expected {
			t.Errorf("unexpected err %s, expected %s", writer.Error(), expected)
		}

		if len(writer.Bytes()) != 0 {
			t.Fatalf("unexpected bytes, no bytes should have been written")
		}
	})
}
