package backup

import (
	"bytes"
	"errors"
	"fmt"
	"log"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/chialab/streamlined-backup/config"
	"github.com/chialab/streamlined-backup/handler"
	"github.com/chialab/streamlined-backup/utils"
)

func newTestLogger() (*log.Logger, func() []string) {
	var buf bytes.Buffer
	logger := log.New(&buf, "", 0)

	lines := func() []string {
		lines := strings.Split(buf.String(), "\n")
		if lines[len(lines)-1] == "" {
			lines = lines[:len(lines)-1]
		}

		return lines
	}

	return logger, lines
}

type testHandler struct {
	lastRun    time.Time
	chunks     []utils.Chunk
	lastRunErr error
	initErr    error
	err        error
}

func (r *testHandler) LastRun() (time.Time, error) {
	return r.lastRun, r.lastRunErr
}

func (h *testHandler) Handler(chunks <-chan utils.Chunk, now time.Time) (func() error, error) {
	if h.initErr != nil {
		return nil, h.initErr
	}

	done := make(chan bool, 1)
	go func() {
		for chunk := range chunks {
			h.chunks = append(h.chunks, chunk)
		}

		done <- true
	}()

	return func() error {
		<-done

		return h.err
	}, nil
}

func TestNewTasks(t *testing.T) {
	t.Parallel()

	cfg := config.Task{
		Command: []string{"echo", "foo bar"},
		Env:     []string{"FOO=bar"},
		Destination: config.Destination{
			Type: "s3",
		},
	}

	task, err := NewTask("foo", cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if task.name != "foo" {
		t.Errorf("expected foo, got %s", task.name)
	}
	if !reflect.DeepEqual(task.command, []string{"echo", "foo bar"}) {
		t.Errorf("expected task command 'echo foo bar', got %v", task.command)
	}
	if !reflect.DeepEqual(task.env, []string{"FOO=bar"}) {
		t.Errorf("expected task env 'FOO=bar', got %v", task.env)
	}
	if _, ok := task.handler.(*handler.S3Handler); !ok {
		t.Errorf("expected S3Handler, got %T", task.handler)
	}
	if task.logger.Prefix() != "[foo] " {
		t.Errorf("expected log prefix '[foo] ', got %s", task.logger.Prefix())
	}
}

func TestNewTasksError(t *testing.T) {
	t.Parallel()

	cfg := config.Task{
		Command: []string{"echo", "bar foo"},
		Env:     []string{"BAR=foo"},
	}

	if tasks, err := NewTask("bar", cfg); err == nil {
		t.Fatalf("expected error, got %v", tasks)
	} else if !errors.Is(err, handler.ErrUnknownDestination) {
		t.Fatalf("expected ErrUnknownDestination, got %v", err)
	}
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
		"never_run": {
			expected: true,
			lastRun:  time.Time{},
			schedule: "@weekly",
		},
	}
	now := time.Date(2021, 10, 6, 19, 10, 38, 0, time.Local)
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			var (
				schedule *utils.ScheduleExpression
				err      error
			)
			if schedule, err = utils.NewSchedule(tc.schedule); err != nil {
				t.Fatalf("unexpected error: %s", err)
			}
			handler := &testHandler{lastRun: tc.lastRun}
			task := &Task{schedule: *schedule, handler: handler}

			if result, err := task.shouldRun(now); err != nil {
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
		schedule *utils.ScheduleExpression
		err      error
	)
	if schedule, err = utils.NewSchedule("@daily"); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	testErr := errors.New("test error")
	handler := &testHandler{lastRunErr: testErr}
	task := &Task{schedule: *schedule, handler: handler}

	now := time.Date(2021, 10, 6, 19, 10, 38, 0, time.Local)
	if result, err := task.shouldRun(now); result != false {
		t.Errorf("unexpected result: %t", result)
	} else if err != testErr {
		t.Errorf("expected %v, got %v", testErr, err)
	}
}

func TestRun(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	if actualPath, err := filepath.EvalSymlinks(tmpDir); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if actualPath != tmpDir {
		tmpDir = actualPath
	}

	handler := &testHandler{}
	logger, lines := newTestLogger()
	task := &Task{
		command: []string{"bash", "-c", "echo $FOO; pwd; echo logging >&2"},
		cwd:     tmpDir,
		env:     []string{"FOO=barbaz"},
		handler: handler,
		logger:  logger,
	}
	expectedData := fmt.Sprintf("barbaz\n%s\n", tmpDir)
	expectedResultLogs := []string{"logging"}

	if res := task.Run(time.Now()); res.Status != StatusSuccess {
		t.Errorf("unexpected error: %+v", res)
	} else if !reflect.DeepEqual(res.Logs, expectedResultLogs) {
		t.Errorf("expected %q, got %q", expectedResultLogs, res.Logs)
	}
	if len(handler.chunks) != 1 {
		t.Errorf("expected 1 chunk, got %d", len(handler.chunks))
	}

	if chunk := handler.chunks[0]; chunk.Error != nil {
		t.Errorf("unexpected error: %s", chunk.Error)
	} else if string(chunk.Data) != expectedData {
		t.Errorf("expected %q, got %q", expectedData, chunk.Data)
	}

	expectedLogs := []string{"logging", "DONE"}
	if logs := lines(); !reflect.DeepEqual(logs, expectedLogs) {
		t.Errorf("expected %q, got %q", expectedLogs, logs)
	}
}

func TestRunLongOutput(t *testing.T) {
	t.Parallel()

	extraSize := 256 << 10
	handler := &testHandler{}
	logger, lines := newTestLogger()
	task := &Task{
		command: []string{"bash", "-c", fmt.Sprintf("yes | head -c %d", CHUNK_SIZE+extraSize)},
		handler: handler,
		logger:  logger,
	}

	if res := task.Run(time.Now()); res.Status != StatusSuccess {
		t.Errorf("unexpected error: %+v", res)
	}
	if len(handler.chunks) != 2 {
		t.Errorf("expected 2 chunks, got %d", len(handler.chunks))
	}

	if chunk := handler.chunks[0]; chunk.Error != nil {
		t.Errorf("unexpected error: %s", chunk.Error)
	} else if len(chunk.Data) != CHUNK_SIZE {
		t.Errorf("expected %d bytes, got %d", CHUNK_SIZE, len(chunk.Data))
	}

	if chunk := handler.chunks[1]; chunk.Error != nil {
		t.Errorf("unexpected error: %s", chunk.Error)
	} else if len(chunk.Data) != extraSize {
		t.Errorf("expected %d bytes, got %d", extraSize, len(chunk.Data))
	}
	expectedLogs := []string{"DONE"}
	if logs := lines(); !reflect.DeepEqual(logs, expectedLogs) {
		t.Errorf("expected %q, got %q", expectedLogs, logs)
	}
}

func TestRunSkipped(t *testing.T) {
	t.Parallel()

	lastRun := time.Date(2021, 10, 12, 10, 30, 38, 0, time.Local)
	handler := &testHandler{lastRun: lastRun}
	schedule, err := utils.NewSchedule("0,30 * * * *")
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	logger, lines := newTestLogger()
	task := &Task{
		schedule: *schedule,
		command:  []string{"echo", "hello world"},
		handler:  handler,
		logger:   logger,
	}

	now := time.Date(2021, 10, 12, 10, 59, 38, 0, time.Local)
	if res := task.Run(now); res.Status != StatusSkipped {
		t.Errorf("unexpected result: %+v", res)
	}

	if len(handler.chunks) != 0 {
		t.Errorf("expected 0 chunks, got %d", len(handler.chunks))
	}

	expectedLogs := []string{"SKIPPED"}
	if logs := lines(); !reflect.DeepEqual(logs, expectedLogs) {
		t.Errorf("expected %q, got %q", expectedLogs, logs)
	}
}

func TestRunHandlerInitError(t *testing.T) {
	t.Parallel()

	initErr := errors.New("init error")
	handler := &testHandler{initErr: initErr}
	logger, lines := newTestLogger()
	task := &Task{
		command: []string{"echo", "hello world"},
		handler: handler,
		logger:  logger,
	}

	if res := task.Run(time.Now()); res.Status != StatusFailure {
		t.Errorf("unexpected result: %+v", res)
	} else if res.Error != initErr {
		t.Errorf("expected %v, got %v", initErr, res.Error)
	}

	if len(handler.chunks) != 0 {
		t.Errorf("expected 0 chunks, got %d", len(handler.chunks))
	}

	expectedLogs := []string{"ERROR (Initialization failed): init error"}
	if logs := lines(); !reflect.DeepEqual(logs, expectedLogs) {
		t.Errorf("expected %q, got %q", expectedLogs, logs)
	}
}

func TestRunLastRunError(t *testing.T) {
	t.Parallel()

	lastRunErr := errors.New("last run error")
	handler := &testHandler{lastRunErr: lastRunErr}
	logger, lines := newTestLogger()
	task := &Task{
		command: []string{"echo", "hello world"},
		handler: handler,
		logger:  logger,
	}

	if res := task.Run(time.Now()); res.Status != StatusFailure {
		t.Errorf("unexpected result: %+v", res)
	} else if res.Error != lastRunErr {
		t.Errorf("expected %v, got %v", lastRunErr, res.Error)
	}

	if len(handler.chunks) != 0 {
		t.Errorf("expected 0 chunks, got %d", len(handler.chunks))
	}

	expectedLogs := []string{"ERROR (Could not find last run): last run error"}
	if logs := lines(); !reflect.DeepEqual(logs, expectedLogs) {
		t.Errorf("expected %q, got %q", expectedLogs, logs)
	}
}

func TestRunProcessSpawnError(t *testing.T) {
	t.Parallel()

	handler := &testHandler{}
	logger, lines := newTestLogger()
	task := &Task{
		command: []string{"this-cmd-does-not-exist"},
		handler: handler,
		logger:  logger,
	}

	res := task.Run(time.Now())
	if res.Status != StatusFailure {
		t.Errorf("unexpected result: %+v", res)
	}

	if len(handler.chunks) != 1 {
		t.Errorf("expected 1 chunk, got %d", len(handler.chunks))
	}
	if chunk := handler.chunks[0]; !errors.Is(res.Error, chunk.Error) {
		t.Errorf("expected %q, got %q", res.Error, chunk.Error)
	} else if len(chunk.Data) > 0 {
		t.Errorf("expected no data, got %q", chunk.Data)
	}

	expectedLogs := []string{"ERROR (Command start): exec: \"this-cmd-does-not-exist\": executable file not found in $PATH"}
	if logs := lines(); !reflect.DeepEqual(logs, expectedLogs) {
		t.Errorf("expected %q, got %q", expectedLogs, logs)
	}
}

func TestRunProcessExecutionError(t *testing.T) {
	t.Parallel()

	handler := &testHandler{}
	logger, lines := newTestLogger()
	task := &Task{
		command: []string{"bash", "-c", "echo foo bar; exit 1"},
		handler: handler,
		logger:  logger,
	}

	res := task.Run(time.Now())
	if res.Status != StatusFailure {
		t.Errorf("unexpected result: %+v", res)
	}

	if len(handler.chunks) != 1 {
		t.Errorf("expected 1 chunk, got %d", len(handler.chunks))
	}
	if chunk := handler.chunks[0]; !errors.Is(res.Error, chunk.Error) {
		t.Errorf("expected %q, got %q", res.Error, chunk.Error)
	} else if string(chunk.Data) != "foo bar\n" {
		t.Errorf("expected %q, got %q", "foo bar\n", chunk.Data)
	}

	expectedLogs := []string{"ERROR (Command failed): exit status 1"}
	if logs := lines(); !reflect.DeepEqual(logs, expectedLogs) {
		t.Errorf("expected %q, got %q", expectedLogs, logs)
	}
}

func TestRunProcessHandlerError(t *testing.T) {
	t.Parallel()

	testErr := errors.New("test error")
	handler := &testHandler{}
	logger, lines := newTestLogger()
	task := &Task{
		command: []string{"bash", "-c", "echo foo bar"},
		handler: handler,
		logger:  logger,
	}

	handler.err = testErr

	if res := task.Run(time.Now()); res.Status != StatusFailure {
		t.Errorf("unexpected result: %+v", res)
	} else if !errors.Is(res.Error, testErr) {
		t.Errorf("expected %q, got %q", testErr, res.Error)
	}

	if len(handler.chunks) != 1 {
		t.Errorf("expected 1 chunk, got %d", len(handler.chunks))
	}
	if chunk := handler.chunks[0]; chunk.Error != nil {
		t.Errorf("unexpected error: %s", chunk.Error)
	} else if string(chunk.Data) != "foo bar\n" {
		t.Errorf("expected %q, got %q", "foo bar\n", chunk.Data)
	}

	expectedLogs := []string{"ERROR (Upload failed): test error"}
	if logs := lines(); !reflect.DeepEqual(logs, expectedLogs) {
		t.Errorf("expected %q, got %q", expectedLogs, logs)
	}
}

func TestRunAbortError(t *testing.T) {
	t.Parallel()

	abortErr := errors.New("test error")
	handler := &testHandler{}
	logger, lines := newTestLogger()
	task := &Task{
		command: []string{"bash", "-c", "echo foo bar; exit 1"},
		handler: handler,
		logger:  logger,
	}
	handler.err = abortErr

	if res := task.Run(time.Now()); res.Status != StatusFailure {
		t.Errorf("unexpected result: %+v", res)
	} else if !errors.Is(res.Error, abortErr) {
		t.Errorf("expected %q, got %q", abortErr, res.Error)
	}

	if len(handler.chunks) != 1 {
		t.Errorf("expected 1 chunk, got %d", len(handler.chunks))
	}
	if chunk := handler.chunks[0]; errors.Is(chunk.Error, abortErr) {
		t.Errorf("expected %q, got %q", abortErr, chunk.Error)
	} else if string(chunk.Data) != "foo bar\n" {
		t.Errorf("expected %q, got %q", "foo bar\n", chunk.Data)
	}

	expectedLogs := []string{"ERROR (Command failed): exit status 1", "ERROR (Upload abort failed): test error"}
	if logs := lines(); !reflect.DeepEqual(logs, expectedLogs) {
		t.Errorf("expected %q, got %q", expectedLogs, logs)
	}
}
