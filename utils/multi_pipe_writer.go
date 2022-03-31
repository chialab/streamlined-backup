package utils

import "io"

type WriteCloserWithError interface {
	io.WriteCloser
	CloseWithError(error) error
}

type multiPipeWriter struct {
	writers []*io.PipeWriter
}

func (w *multiPipeWriter) Write(p []byte) (int, error) {
	for _, writer := range w.writers {
		if n, err := writer.Write(p); err != nil {
			return n, err
		}
	}

	return len(p), nil
}

func (w *multiPipeWriter) Close() error {
	return w.CloseWithError(nil)
}

func (w *multiPipeWriter) CloseWithError(err error) error {
	for _, writer := range w.writers {
		if e := writer.CloseWithError(err); e != nil {
			return e
		}
	}

	return nil
}

func MultiPipeWriter(writers ...*io.PipeWriter) WriteCloserWithError {
	return &multiPipeWriter{writers}
}
