package buffer

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"math"
	"testing"

	"github.com/jeroenrinzema/psql-wire/internal/types"
)

func TestNewReaderNil(t *testing.T) {
	reader := NewReader(nil, 0)
	if reader != nil {
		t.Fatalf("unexpected result, expected reader to be nil %+v", reader)
	}
}

func TestReadTypedMsg(t *testing.T) {
	expected := types.ClientSimpleQuery
	_text := append([]byte("John Doe"), 0) // 0 represents the NUL termination

	buffer := bytes.NewBuffer([]byte{})
	buffer.WriteByte(byte(expected))

	size := make([]byte, 4)
	binary.BigEndian.PutUint32(size, uint32(len(_text)))

	buffer.Write(size)
	buffer.Write(_text)

	reader := NewReader(buffer, DefaultBufferSize)

	ty, ln, err := reader.ReadTypedMsg()
	if err != nil {
		t.Fatal(err)
	}

	if ty != expected {
		t.Errorf("unexpected message type %s, expected %s", string(ty), string(expected))
	}

	if ln != len(_text) {
		t.Errorf("unexpected message length %d, expected %d", ln, len(_text))
	}
}

func TestReadUntypedMsg(t *testing.T) {
	_text := append([]byte("John Doe"), 0) // 0 represents the NUL termination
	buffer := bytes.NewBuffer([]byte{})

	size := make([]byte, 4)
	binary.BigEndian.PutUint32(size, uint32(len(_text)))

	buffer.Write(size)
	buffer.Write(_text)

	reader := NewReader(buffer, DefaultBufferSize)

	ln, err := reader.ReadUntypedMsg()
	if err != nil {
		t.Fatal(err)
	}

	if ln != len(_text) {
		t.Errorf("unexpected message length %d, expected %d", ln, len(_text))
	}
}

func TestReadUntypedMsgParameters(t *testing.T) {
	_text := append([]byte("John Doe"), 0) // 0 represents the NUL termination
	_prepare := PrepareStatement
	_bytes := []byte{0, 1, 0}
	_uint16 := make([]byte, 2)
	_uint32 := make([]byte, 4)

	binary.BigEndian.PutUint16(_uint16, uint16(math.MaxUint16))
	binary.BigEndian.PutUint32(_uint32, uint32(math.MaxUint32))

	msg := bytes.NewBuffer(make([]byte, 4)) // first 4 bytes should represent the message size
	msg.Write(_text)
	msg.WriteByte(byte(_prepare))
	msg.Write(_bytes)
	msg.Write(_uint16)
	msg.Write(_uint32)

	buffer := msg.Bytes()
	binary.BigEndian.PutUint32(buffer, uint32(msg.Len()))

	reader := NewReader(bytes.NewReader(buffer), DefaultBufferSize)
	ln, err := reader.ReadUntypedMsg()
	if err != nil {
		t.Fatal(err)
	}

	if ln != msg.Len() {
		t.Errorf("unexpected message length %d, expected %d", ln, msg.Len())
	}

	t.Log("reading string")

	expected := string(_text[:len(_text)-1]) // remove NUL termination
	rstring, err := reader.GetString()
	if err != nil {
		t.Fatal(err)
	}

	if rstring != expected {
		t.Fatalf("unexpected string '%s', expected '%s'", rstring, expected)
	}

	t.Log("read string:", rstring)
	t.Log("reading prepare")

	rprepare, err := reader.GetPrepareType()
	if err != nil {
		t.Fatal(err)
	}

	if rprepare != _prepare {
		t.Fatalf("unexpected prepare type %+v, expected %+v", rprepare, _prepare)
	}

	t.Log("read prepare:", rprepare)
	t.Log("reading bytes")

	rbytes, err := reader.GetBytes(len(_bytes))
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(rbytes, _bytes) {
		t.Fatalf("unexpected bytes %+v, expected %+v", rbytes, _bytes)
	}

	t.Log("read bytes:", rbytes)
	t.Log("reading uint16")

	ruint16, err := reader.GetUint16()
	if err != nil {
		t.Fatal(err)
	}

	if ruint16 != math.MaxUint16 {
		t.Fatalf("unexpected uint16 %+v, expected %+v", ruint16, math.MaxUint16)
	}

	t.Log("read uint16:", ruint16)
	t.Log("reading uint32")

	ruint32, err := reader.GetUint32()
	if err != nil {
		t.Fatal(err)
	}

	if ruint32 != math.MaxUint32 {
		t.Fatalf("unexpected uint32 %+v, expected %+v", ruint32, math.MaxUint32)
	}

	t.Log("read uint32:", ruint32)
}

func TestGetStringNulTerminatorNotfound(t *testing.T) {
	reader := &Reader{
		Msg: []byte("John Doe"),
	}

	_, err := reader.GetString()
	if !errors.Is(err, ErrMissingNulTerminator) {
		t.Fatalf("unexpected err %s, expected %s", err, ErrMissingNulTerminator)
	}
}

func TestGetInsufficientData(t *testing.T) {
	buffer := bytes.NewBuffer([]byte{})
	reader := &Reader{
		Msg:    []byte{},
		Buffer: bufio.NewReader(buffer),
	}

	t.Run("typed header msg", func(t *testing.T) {
		_, _, err := reader.ReadTypedMsg()
		if err == nil {
			t.Fatal("unexpected pass")
		}
	})

	t.Run("typed msg", func(t *testing.T) {
		buffer.WriteByte(byte(types.ClientSimpleQuery))
		_, _, err := reader.ReadTypedMsg()
		if err == nil {
			t.Fatal("unexpected pass")
		}
	})

	t.Run("untyped msg", func(t *testing.T) {
		_, err := reader.ReadUntypedMsg()
		if err == nil {
			t.Fatal("unexpected pass")
		}
	})

	t.Run("prepare", func(t *testing.T) {
		_, err := reader.GetPrepareType()
		if err == nil {
			t.Fatal("unexpected pass")
		}
	})

	t.Run("string", func(t *testing.T) {
		_, err := reader.GetString()
		if !errors.Is(err, ErrMissingNulTerminator) {
			t.Fatalf("unexpected err %s, expected %s", err, ErrMissingNulTerminator)
		}
	})

	t.Run("bytes", func(t *testing.T) {
		_, err := reader.GetBytes(5)
		if !errors.Is(err, ErrInsufficientData) {
			t.Fatalf("unexpected err %s, expected %s", err, ErrInsufficientData)
		}
	})

	t.Run("uint16", func(t *testing.T) {
		_, err := reader.GetUint16()
		if !errors.Is(err, ErrInsufficientData) {
			t.Fatalf("unexpected err %s, expected %s", err, ErrInsufficientData)
		}
	})

	t.Run("uint32", func(t *testing.T) {
		_, err := reader.GetUint32()
		if !errors.Is(err, ErrInsufficientData) {
			t.Fatalf("unexpected err %s, expected %s", err, ErrInsufficientData)
		}
	})
}

func TestMsgReset(t *testing.T) {
	expected := 4096

	t.Run("undefined", func(t *testing.T) {
		reader := &Reader{}
		reader.reset(expected)

		if len(reader.Msg) != expected {
			t.Errorf("unexpected reader message size %d, expected %d", len(reader.Msg), expected)
		}
	})

	t.Run("greater", func(t *testing.T) {
		reader := &Reader{
			Msg: make([]byte, 0, expected*2),
		}

		reader.reset(expected)

		if len(reader.Msg) != expected {
			t.Errorf("unexpected reader message size %d, expected %d", len(reader.Msg), expected)
		}
	})

	t.Run("smaller", func(t *testing.T) {
		reader := &Reader{
			Msg: make([]byte, 0, expected/2),
		}
		reader.reset(expected)

		if len(reader.Msg) != expected {
			t.Errorf("unexpected reader message size %d, expected %d", len(reader.Msg), expected)
		}
	})
}
