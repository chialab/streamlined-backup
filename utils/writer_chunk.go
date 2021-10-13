package utils

import (
	"bytes"
	"errors"
	"io"
	"sync"
)

type Chunk struct {
	Data  []byte
	Error error
}

func (c *Chunk) NewReader() io.ReadSeeker {
	return bytes.NewReader(c.Data)
}

var ClosedWriterError = errors.New("writer is closed")

type ChunkWriter struct {
	data   []byte
	size   int
	mutex  *sync.Mutex
	closed bool
	Chunks chan Chunk
}

func NewChunkWriter(size int, buffer int) *ChunkWriter {
	return &ChunkWriter{
		data:   make([]byte, 0),
		size:   size,
		mutex:  &sync.Mutex{},
		closed: false,
		Chunks: make(chan Chunk, buffer),
	}
}
func (w *ChunkWriter) Write(p []byte) (n int, err error) {
	w.mutex.Lock()
	defer w.mutex.Unlock()
	if w.closed {
		return 0, ClosedWriterError
	}

	w.data = append(w.data, p...)
	for len(w.data) >= w.size {
		w.Chunks <- Chunk{Data: w.data[:w.size]}
		w.data = w.data[w.size:]
	}

	return len(p), nil
}
func (w *ChunkWriter) Abort(err error) error {
	w.mutex.Lock()
	defer w.mutex.Unlock()
	if w.closed {
		return ClosedWriterError
	}

	if len(w.data) > 0 || err != nil {
		w.Chunks <- Chunk{Data: w.data, Error: err}
	}
	w.closed = true
	close(w.Chunks)

	return nil
}
func (w *ChunkWriter) Close() error {
	return w.Abort(nil)
}
