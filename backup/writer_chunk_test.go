package backup

import (
	"errors"
	"io"
	"testing"
)

func TestChunkReader(t *testing.T) {
	t.Parallel()

	chunk := &Chunk{
		Data: []byte("foo+bar"),
	}
	reader := chunk.NewReader()

	buf := make([]byte, 4)
	if len, err := reader.Read(buf); err != nil {
		t.Errorf("unexpected error: %s", err)
	} else if len != 4 {
		t.Errorf("unexpected length: %d", len)
	} else if string(buf[:]) != "foo+" {
		t.Errorf("unexpected data: %s", string(buf[:]))
	}

	if cur, err := reader.Seek(-3, io.SeekEnd); err != nil {
		t.Errorf("unexpected error: %s", err)
	} else if cur != 4 {
		t.Errorf("unexpected position: %d", cur)
	}

	buf = make([]byte, 4)
	if len, err := reader.Read(buf); err != nil {
		t.Errorf("unexpected error: %s", err)
	} else if len != 3 {
		t.Errorf("unexpected length: %d", len)
	} else if string(buf[:]) != "bar\x00" {
		t.Errorf("unexpected data: %s", string(buf[:]))
	}
}

func TestChunkWriter(t *testing.T) {
	t.Parallel()

	writer := NewChunkWriter(8)

	if b, err := writer.Write([]byte("foo")); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if b != 3 {
		t.Errorf("expected 3 bytes written, got %d", b)
	}

	if len(writer.Chunks) != 0 {
		t.Error("chunks should be empty")
	}

	if b, err := writer.Write([]byte("bar++")); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if b != 5 {
		t.Errorf("expected 5 bytes written, got %d", b)
	}

	if len(writer.Chunks) != 1 {
		t.Errorf("chunks should have 1 element, got %d", len(writer.Chunks))
	} else if chunk := <-writer.Chunks; string(chunk.Data) != "foobar++" {
		t.Errorf("first chunk data should be 'foobar++', got %s", string(chunk.Data))
	} else if chunk.Done {
		t.Error("first chunk should not be last")
	} else if chunk.Error != nil {
		t.Errorf("first chunk should not have error: %s", chunk.Error)
	}

	if b, err := writer.Write([]byte("baz bazinga go lang go")); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if b != 22 {
		t.Errorf("expected 22 bytes written, got %d", b)
	}

	if len(writer.Chunks) != 2 {
		t.Errorf("chunks should have 2 elements, got %d", len(writer.Chunks))
	} else if chunk := <-writer.Chunks; string(chunk.Data) != "baz bazi" {
		t.Errorf("second chunk data should be 'baz bazi', got %s", string(chunk.Data))
	} else if chunk.Done {
		t.Error("second chunk should not be last")
	} else if chunk.Error != nil {
		t.Errorf("second chunk should not have error: %s", chunk.Error)
	} else if chunk := <-writer.Chunks; string(chunk.Data) != "nga go l" {
		t.Errorf("third chunk data should be 'nga go l', got %s", string(chunk.Data))
	} else if chunk.Done {
		t.Error("third chunk should not be last")
	} else if chunk.Error != nil {
		t.Errorf("third chunk should not have error: %s", chunk.Error)
	}

	writer.Close()

	if len(writer.Chunks) != 1 {
		t.Errorf("chunks should have 1 element, got %d", len(writer.Chunks))
	} else if chunk := <-writer.Chunks; string(chunk.Data) != "ang go" {
		t.Errorf("fourth chunk data should be 'ang go', got %s", string(chunk.Data))
	} else if !chunk.Done {
		t.Error("fourth chunk should be last")
	} else if chunk.Error != nil {
		t.Errorf("fourth chunk should not have error: %s", chunk.Error)
	}
}

func TestChunkWriterAbort(t *testing.T) {
	t.Parallel()

	writer := NewChunkWriter(8)

	if b, err := writer.Write([]byte("foo")); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if b != 3 {
		t.Errorf("expected 3 bytes written, got %d", b)
	}

	if len(writer.Chunks) != 0 {
		t.Error("chunks should be empty")
	}

	testErr := errors.New("test error")
	writer.Abort(testErr)

	if len(writer.Chunks) != 1 {
		t.Errorf("chunks should have 1 element, got %d", len(writer.Chunks))
	} else if chunk := <-writer.Chunks; string(chunk.Data) != "foo" {
		t.Errorf("first chunk data should be 'foo', got %s", string(chunk.Data))
	} else if !chunk.Done {
		t.Error("first chunk should be last")
	} else if chunk.Error != testErr {
		t.Errorf("expected %q, got %q", testErr, chunk.Error)
	}
}
