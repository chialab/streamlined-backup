package utils

import (
	"errors"
	"io"
	"sync"
	"testing"
)

func TestMultiPipeWriter(t *testing.T) {
	wg := sync.WaitGroup{}
	writers := []*io.PipeWriter{}

	for i := 0; i < 10; i++ {
		r, w := io.Pipe()
		writers = append(writers, w)

		wg.Add(1)
		go func(r *io.PipeReader) {
			defer wg.Done()

			buf := make([]byte, 1024)
			if n, err := r.Read(buf); err != nil {
				t.Errorf("read failed: %#v", err)
			} else if n != 5 {
				t.Errorf("read returned %d bytes, expected 5", n)
			} else if string(buf[:n]) != "hello" {
				t.Errorf("read returned %s, expected hello", string(buf[:n]))
			}

			if n, err := r.Read(buf); err != io.EOF {
				t.Errorf("read failed: expected %#v, got %#v", io.EOF, err)
			} else if n != 0 {
				t.Errorf("read returned %d bytes, expected 0", n)
			}

			if err := r.Close(); err != nil {
				t.Errorf("close failed: %#v", err)
			}
		}(r)
	}

	mw := MultiPipeWriter(writers...)
	if n, err := mw.Write([]byte("hello")); err != nil {
		t.Errorf("write failed: %#v", err)
	} else if n != 5 {
		t.Errorf("write returned %d bytes, expected 5", n)
	}

	if err := mw.Close(); err != nil {
		t.Errorf("close failed: %#v", err)
	}

	wg.Wait()
}

func TestMultiPipeWriterClose(t *testing.T) {
	wg := sync.WaitGroup{}
	writers := []*io.PipeWriter{}

	testErr := errors.New("test error")

	for i := 0; i < 2; i++ {
		r, w := io.Pipe()
		writers = append(writers, w)

		wg.Add(1)
		go func(r *io.PipeReader) {
			defer wg.Done()

			buf := make([]byte, 1024)
			if n, err := r.Read(buf); err != nil {
				t.Errorf("read failed: %#v", err)
			} else if n != 5 {
				t.Errorf("read returned %d bytes, expected 5", n)
			} else if string(buf[:n]) != "hello" {
				t.Errorf("read returned %s, expected hello", string(buf[:n]))
			}

			if n, err := r.Read(buf); err != testErr {
				t.Errorf("read failed: expected %#v, got %#v", testErr, err)
			} else if n != 0 {
				t.Errorf("read returned %d bytes, expected 0", n)
			}

			if err := r.Close(); err != nil {
				t.Errorf("close failed: %#v", err)
			}
		}(r)
	}

	mw := MultiPipeWriter(writers...)
	if n, err := mw.Write([]byte("hello")); err != nil {
		t.Errorf("write failed: %#v", err)
	} else if n != 5 {
		t.Errorf("write returned %d bytes, expected 5", n)
	}

	if err := mw.CloseWithError(testErr); err != nil {
		t.Errorf("close failed: %#v", err)
	}

	wg.Wait()
}

func TestMultiPipeWriterReaderClose(t *testing.T) {
	wg := sync.WaitGroup{}
	writers := []*io.PipeWriter{}
	wait := make(chan bool)

	testErr := errors.New("test error")

	for i := 0; i < 3; i++ {
		r, w := io.Pipe()
		writers = append(writers, w)

		wg.Add(1)
		go func(r *io.PipeReader, i int) {
			defer wg.Done()

			buf := make([]byte, 1024)
			if n, err := r.Read(buf); err != nil {
				t.Errorf("read failed: %#v", err)
			} else if n != 5 {
				t.Errorf("read returned %d bytes, expected 5", n)
			} else if string(buf[:n]) != "hello" {
				t.Errorf("read returned %s, expected hello", string(buf[:n]))
			}

			switch i {
			case 0:
				if n, err := r.Read(buf); err != nil {
					t.Errorf("read failed: %#v", err)
				} else if n != 5 {
					t.Errorf("read returned %d bytes, expected 5", n)
				} else if string(buf[:n]) != "world" {
					t.Errorf("read returned %s, expected world", string(buf[:n]))
				}

				if n, err := r.Read(buf); err != testErr {
					t.Errorf("read failed: expected %#v, got %#v", testErr, err)
				} else if n != 0 {
					t.Errorf("read returned %d bytes, expected 0", n)

				}

				if err := r.Close(); err != nil {
					t.Errorf("close failed: %#v", err)
				}
			case 1:
				if err := r.CloseWithError(testErr); err != nil {
					t.Errorf("close failed: %#v", err)
				}
				wait <- true
			case 2:
				if n, err := r.Read(buf); err != testErr {
					t.Errorf("read failed: expected %#v, got %#v", testErr, err)
				} else if n != 0 {
					t.Errorf("read returned %d bytes, expected 0", n)

				}

				if err := r.Close(); err != nil {
					t.Errorf("close failed: %#v", err)
				}
			}
		}(r, i)
	}

	mw := MultiPipeWriter(writers...)
	if n, err := mw.Write([]byte("hello")); err != nil {
		t.Errorf("write failed: %#v", err)
	} else if n != 5 {
		t.Errorf("write returned %d bytes, expected 5", n)
	}
	<-wait
	if n, err := mw.Write([]byte("world")); err != testErr {
		t.Errorf("write failed: expected %#v, got %#v", testErr, err)
	} else if n != 0 {
		t.Errorf("write returned %d bytes, expected 0", n)
	}

	if err := mw.CloseWithError(testErr); err != nil {
		t.Errorf("close failed: %#v", err)
	}

	wg.Wait()
}
