package backup

import (
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"
)

type testLastRunner struct {
	lastRun time.Time
	err     error
}

func (r *testLastRunner) LastRun(Destination) (time.Time, error) {
	return r.lastRun, r.err
}

func TestShouldRun(t *testing.T) {
	t.Parallel()

	type testCase struct {
		expected bool
		lastRun  time.Time
		schedule string
	}
	cases := map[string]testCase{
		"yes": {
			expected: true,
			lastRun:  time.Date(2021, 10, 3, 19, 10, 38, 0, time.Local),
			schedule: "0 10 * * *",
		},
		"no": {
			expected: false,
			lastRun:  time.Date(2021, 10, 3, 19, 10, 38, 0, time.Local),
			schedule: "@weekly",
		},
	}
	now := time.Date(2021, 10, 6, 19, 10, 38, 0, time.Local)
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			var (
				schedule *ScheduleExpression
				err      error
			)
			if schedule, err = NewSchedule(tc.schedule); err != nil {
				t.Fatalf("unexpected error: %s", err)
			}
			op := &Operation{Schedule: *schedule}

			runner := &testLastRunner{lastRun: tc.lastRun}
			if result, err := op.ShouldRun(runner, now); err != nil {
				t.Errorf("unexpected error: %s", err)
			} else if result != tc.expected {
				t.Errorf("expected %t, got %t", tc.expected, result)
			}
		})
	}
}

func TestShouldRunError(t *testing.T) {
	t.Parallel()

	var (
		schedule *ScheduleExpression
		err      error
	)
	if schedule, err = NewSchedule("@daily"); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	op := &Operation{Schedule: *schedule}

	testErr := errors.New("test error")
	runner := &testLastRunner{err: testErr}
	now := time.Date(2021, 10, 6, 19, 10, 38, 0, time.Local)
	if result, err := op.ShouldRun(runner, now); result != false {
		t.Errorf("unexpected result: %t", result)
	} else if err != testErr {
		t.Errorf("expected %v, got %v", testErr, err)
	}
}

type testHandler struct {
	chunks []Chunk
	err    error
	done   chan bool
}

func (h *testHandler) Handler(chunks chan Chunk) {
	var chunk Chunk
	for !chunk.Done && chunk.Error == nil {
		chunk = <-chunks
		h.chunks = append(h.chunks, chunk)
	}
	h.done <- true
}
func (h *testHandler) Wait() error {
	<-h.done

	return h.err
}
func newTestHandler() *testHandler {
	done := make(chan bool, 1)

	return &testHandler{done: done}
}

func TestRun(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	if actualPath, err := filepath.EvalSymlinks(tmpDir); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if actualPath != tmpDir {
		tmpDir = actualPath
	}

	op := &Operation{
		Command: []string{"bash", "-c", "echo $FOO; pwd; echo logging >&2"},
		Cwd:     tmpDir,
		Env:     []string{"FOO=barbaz"},
	}
	expectedData := fmt.Sprintf("barbaz\n%s\n", tmpDir)
	expectedLogs := []string{"logging"}

	handler := newTestHandler()
	logger, logs := newTestLogFunction()

	if err := op.Run(handler, logger); err != nil {
		t.Errorf("unexpected error: %s", err)
	}
	if len(handler.chunks) != 1 {
		t.Errorf("expected 1 chunk, got %d", len(handler.chunks))
	}

	if chunk := handler.chunks[0]; chunk.Error != nil {
		t.Errorf("unexpected error: %s", chunk.Error)
	} else if string(chunk.Data) != expectedData {
		t.Errorf("expected %q, got %q", expectedData, chunk.Data)
	}

	if len(*logs) != len(expectedLogs) {
		t.Errorf("expected %d logs, got %d", len(expectedLogs), len(*logs))
	}
	for i, log := range *logs {
		if log != expectedLogs[i] {
			t.Errorf("expected %q, got %q", expectedLogs[i], log)
		}
	}
}

func TestRunLongOutput(t *testing.T) {
	t.Parallel()

	extraSize := 256 << 10
	op := &Operation{
		Command: []string{"bash", "-c", fmt.Sprintf("yes | head -c %d", CHUNK_SIZE+extraSize)},
	}

	handler := newTestHandler()
	logger, _ := newTestLogFunction()

	if err := op.Run(handler, logger); err != nil {
		t.Errorf("unexpected error: %s", err)
	}
	if len(handler.chunks) != 2 {
		t.Errorf("expected 2 chunks, got %d", len(handler.chunks))
	}

	if chunk := handler.chunks[0]; chunk.Error != nil {
		t.Errorf("unexpected error: %s", chunk.Error)
	} else if len(chunk.Data) != CHUNK_SIZE {
		t.Errorf("expected %d bytes, got %d", CHUNK_SIZE, len(chunk.Data))
	} else if chunk.Done {
		t.Errorf("expected first chunk to not be last one")
	}

	if chunk := handler.chunks[1]; chunk.Error != nil {
		t.Errorf("unexpected error: %s", chunk.Error)
	} else if len(chunk.Data) != extraSize {
		t.Errorf("expected %d bytes, got %d", extraSize, len(chunk.Data))
	} else if !chunk.Done {
		t.Errorf("expected last chunk to be last one")
	}
}

func TestRunProcessSpawnError(t *testing.T) {
	t.Parallel()

	op := &Operation{
		Command: []string{"this-cmd-does-not-exist"},
	}

	handler := newTestHandler()
	logger, logs := newTestLogFunction()

	err := op.Run(handler, logger)
	if err == nil {
		t.Error("expected error, got none")
	}

	if len(handler.chunks) != 1 {
		t.Errorf("expected 1 chunk, got %d", len(handler.chunks))
	}
	if chunk := handler.chunks[0]; chunk.Error != err {
		t.Errorf("expected %q, got %q", err, chunk.Error)
	} else if len(chunk.Data) > 0 {
		t.Errorf("expected no data, got %q", chunk.Data)
	}

	if len(*logs) != 1 {
		t.Errorf("expected 1 log, got %d", len(*logs))
	} else if (*logs)[0] != err.Error() {
		t.Errorf("expected %q, got %q", err.Error(), (*logs)[0])
	}
}

func TestRunProcessExecutionError(t *testing.T) {
	t.Parallel()

	op := &Operation{
		Command: []string{"bash", "-c", "echo foo bar; exit 1"},
	}

	handler := newTestHandler()
	logger, logs := newTestLogFunction()

	err := op.Run(handler, logger)
	if err == nil {
		t.Error("expected error, got none")
	}

	if len(handler.chunks) != 1 {
		t.Errorf("expected 1 chunk, got %d", len(handler.chunks))
	}
	if chunk := handler.chunks[0]; chunk.Error != err {
		t.Errorf("expected %q, got %q", err, chunk.Error)
	} else if string(chunk.Data) != "foo bar\n" {
		t.Errorf("expected %q, got %q", "foo bar\n", chunk.Data)
	}

	if len(*logs) != 1 {
		t.Errorf("expected 1 log, got %d", len(*logs))
	} else if (*logs)[0] != err.Error() {
		t.Errorf("expected %q, got %q", err.Error(), (*logs)[0])
	}
}

func TestRunProcessHandlerError(t *testing.T) {
	t.Parallel()

	op := &Operation{
		Command: []string{"bash", "-c", "echo foo bar"},
	}

	testErr := errors.New("test error")
	handler := newTestHandler()
	handler.err = testErr
	logger, logs := newTestLogFunction()

	err := op.Run(handler, logger)
	if err != testErr {
		t.Errorf("expected %q, got %q", testErr, err)
	}

	if len(handler.chunks) != 1 {
		t.Errorf("expected 1 chunk, got %d", len(handler.chunks))
	}
	if chunk := handler.chunks[0]; chunk.Error != nil {
		t.Errorf("unexpected error: %s", chunk.Error)
	} else if string(chunk.Data) != "foo bar\n" {
		t.Errorf("expected %q, got %q", "foo bar\n", chunk.Data)
	}

	if len(*logs) != 1 {
		t.Errorf("expected 1 log, got %d", len(*logs))
	} else if (*logs)[0] != err.Error() {
		t.Errorf("expected %q, got %q", err.Error(), (*logs)[0])
	}
}
