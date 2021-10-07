package backup

import (
	"bytes"
	"io"
)

const CHUNK_BUFFER = 8

type Chunk struct {
	Data  []byte
	Error error
	Done  bool
}

func (c *Chunk) NewReader() io.ReadSeeker {
	return bytes.NewReader(c.Data)
}

type ChunkWriter struct {
	data   []byte
	size   int64
	Chunks chan Chunk
}

func NewChunkWriter(size int64) *ChunkWriter {
	return &ChunkWriter{
		data:   make([]byte, 0),
		size:   size,
		Chunks: make(chan Chunk, CHUNK_BUFFER),
	}
}
func (w *ChunkWriter) Write(p []byte) (n int, err error) {
	w.Append(p)

	return len(p), nil
}
func (w *ChunkWriter) Append(p []byte) int64 {
	w.data = append(w.data, p...)
	written := int64(0)
	for int64(len(w.data)) >= w.size {
		w.Chunks <- Chunk{Data: w.data[:w.size], Done: false}
		w.data = w.data[w.size:]
		written += w.size
	}

	return written
}
func (w *ChunkWriter) Close() error {
	w.Chunks <- Chunk{Data: w.data, Done: true}

	return nil
}
func (w *ChunkWriter) Abort(err error) error {
	w.Chunks <- Chunk{Data: w.data, Error: err, Done: true}

	return nil
}
