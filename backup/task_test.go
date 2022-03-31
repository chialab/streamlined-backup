package backup

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
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

var testChunkSize = 256 << 10 // 256 KiB

type testHandler struct {
	chunkSize  int
	lastRun    time.Time
	chunks     [][]byte
	lastRunErr error
	initErr    error
	err        error
}

func (h testHandler) size() int {
	if h.chunkSize == 0 {
		return testChunkSize
	}

	return h.chunkSize
}

func (r *testHandler) LastRun() (time.Time, error) {
	return r.lastRun, r.lastRunErr
}

func (h *testHandler) Handler(reader *io.PipeReader, now time.Time) (func() error, error) {
	if h.initErr != nil {
		return nil, h.initErr
	}

	done := make(chan error)
	go func() {
		defer close(done)

		for {
			chunk := make([]byte, h.size())
			bytes, err := io.ReadAtLeast(reader, chunk, h.size())
			if err == io.EOF {
				break
			} else if err != nil && err != io.ErrUnexpectedEOF {
				done <- err
				return
			} else if bytes == 0 {
				continue
			}
			h.chunks = append(h.chunks, chunk[:bytes])

			if err == io.EOF {
				break
			}
		}

		done <- nil
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
		Destinations: []config.Destination{
			{Type: "s3"},
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
	if len(task.destinations) != 1 {
		t.Errorf("expected 1 destination, got %d", len(task.destinations))
	}
	if task.logger.Prefix() != "[foo] " {
		t.Errorf("expected log prefix '[foo] ', got %s", task.logger.Prefix())
	}
}

func TestNewTasksInvalidDestination(t *testing.T) {
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

func TestNewTasksInvalidTimeout(t *testing.T) {
	t.Parallel()

	cfg := config.Task{
		Command: []string{"echo", "bar foo"},
		Env:     []string{"BAR=foo"},
		Timeout: "foo bar",
		Destinations: []config.Destination{
			{Type: "s3"},
		},
	}

	expectedErr := `time: invalid duration "foo bar"`
	if tasks, err := NewTask("bar", cfg); err == nil {
		t.Fatalf("expected error, got %v", tasks)
	} else if err.Error() != expectedErr {
		t.Fatalf("expected %s, got %s", expectedErr, err)
	}
}

func TestTaskAccessors(t *testing.T) {
	t.Parallel()

	t.Run("with_given_values", func(t *testing.T) {
		tmpDir := t.TempDir()

		task := &Task{
			name: "test",
			command: []string{
				"bash",
				"-c",
				"echo 'hello world' \"${PWD:-/tmp}\" | bzip2",
			},
			cwd:     tmpDir,
			timeout: 30 * time.Minute,
		}

		if name := task.Name(); name != "test" {
			t.Errorf("expected test, got %s", name)
		}
		escaped := "bash -c 'echo '\"'\"'hello world'\"'\"' \"${PWD:-/tmp}\" | bzip2'"
		if cmd := task.CommandString(); cmd != escaped {
			t.Errorf("expected %s, got %s", escaped, cmd)
		}
		if wd := task.ActualCwd(); wd != tmpDir {
			t.Errorf("expected %s, got %s", tmpDir, wd)
		}
		if timeout := task.Timeout(); timeout != 30*time.Minute {
			t.Errorf("expected 30m, got %s", timeout)
		}
	})

	t.Run("with_defaults", func(t *testing.T) {
		cwd, err := os.Getwd()
		if err != nil {
			t.Fatal(err)
		}

		task := &Task{
			name:    "test",
			command: []string{"echo", "hello world"},
		}

		if name := task.Name(); name != "test" {
			t.Errorf("expected %s, got %s", "test", name)
		}
		escaped := "echo 'hello world'"
		if cmd := task.CommandString(); cmd != escaped {
			t.Errorf("expected %s, got %s", escaped, cmd)
		}
		if wd := task.ActualCwd(); wd != cwd {
			t.Errorf("expected %s, got %s", cwd, wd)
		}
		if timeout := task.Timeout(); timeout != DEFAULT_TIMEOUT {
			t.Errorf("expected default timeout (%s), got %s", DEFAULT_TIMEOUT, timeout)
		}
	})
}

func TestRun(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	if actualPath, err := filepath.EvalSymlinks(tmpDir); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if actualPath != tmpDir {
		tmpDir = actualPath
	}

	h := &testHandler{}
	dest := handler.Destination{}
	dest.SetHandler(h)
	logger, lines := newTestLogger()
	task := &Task{
		command:      []string{"bash", "-c", "echo $FOO; pwd; echo logging >&2"},
		cwd:          tmpDir,
		env:          []string{"FOO=barbaz"},
		destinations: handler.Destinations{dest},
		logger:       logger,
	}
	expectedData := fmt.Sprintf("barbaz\n%s\n", tmpDir)
	expectedResultLogs := []string{"logging"}

	if res := task.Run(time.Now()); res.Status() != StatusSuccess {
		t.Errorf("unexpected error: %+v", res)
	} else if !reflect.DeepEqual(res.Logs(), expectedResultLogs) {
		t.Errorf("expected %q, got %q", expectedResultLogs, res.Logs())
	}
	if len(h.chunks) != 1 {
		t.Errorf("expected 1 chunk, got %d", len(h.chunks))
	}

	if chunk := h.chunks[0]; string(chunk) != expectedData {
		t.Errorf("expected %q, got %q", expectedData, chunk)
	}

	expectedLogs := []string{"logging", "DONE"}
	if logs := lines(); !reflect.DeepEqual(logs, expectedLogs) {
		t.Errorf("expected %q, got %q", expectedLogs, logs)
	}
}

func TestRunLongOutput(t *testing.T) {
	t.Parallel()

	extraSize := testChunkSize / 2
	h := &testHandler{}
	dest := handler.Destination{}
	dest.SetHandler(h)
	logger, lines := newTestLogger()
	task := &Task{
		command:      []string{"bash", "-c", fmt.Sprintf("yes | head -c %d", testChunkSize+extraSize)},
		destinations: handler.Destinations{dest},
		logger:       logger,
	}

	if res := task.Run(time.Now()); res.Status() != StatusSuccess {
		t.Errorf("unexpected error: %+v", res)
	}
	if len(h.chunks) != 2 {
		t.Errorf("expected 2 chunks, got %d", len(h.chunks))
	}

	if chunk := h.chunks[0]; len(chunk) < testChunkSize {
		t.Errorf("expected at least %d bytes, got %d", testChunkSize, len(chunk))
	}

	if chunk := h.chunks[1]; len(chunk) > extraSize {
		t.Errorf("expected at most %d bytes, got %d", extraSize, len(chunk))
	}
	expectedLogs := []string{"DONE"}
	if logs := lines(); !reflect.DeepEqual(logs, expectedLogs) {
		t.Errorf("expected %q, got %q", expectedLogs, logs)
	}
}

func TestRunSkipped(t *testing.T) {
	t.Parallel()

	lastRun := time.Date(2021, 10, 12, 10, 30, 38, 0, time.Local)
	h := &testHandler{lastRun: lastRun}
	schedule, err := utils.NewSchedule("0,30 * * * *")
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	dest := handler.Destination{}
	dest.SetSchedule(*schedule).SetHandler(h)
	logger, lines := newTestLogger()
	task := &Task{
		command:      []string{"echo", "hello world"},
		destinations: handler.Destinations{dest},
		logger:       logger,
	}

	now := time.Date(2021, 10, 12, 10, 59, 38, 0, time.Local)
	if res := task.Run(now); res.Status() != StatusSkipped {
		t.Errorf("unexpected result: %+v", res)
	}

	if len(h.chunks) != 0 {
		t.Errorf("expected 0 chunks, got %d", len(h.chunks))
	}

	expectedLogs := []string{"SKIPPED"}
	if logs := lines(); !reflect.DeepEqual(logs, expectedLogs) {
		t.Errorf("expected %q, got %q", expectedLogs, logs)
	}
}

func TestRunHandlerInitError(t *testing.T) {
	t.Parallel()

	initErr := errors.New("handler could not be initialized: init error")
	h := &testHandler{initErr: initErr}
	dest := handler.Destination{}
	dest.SetHandler(h)
	logger, lines := newTestLogger()
	task := &Task{
		command:      []string{"echo", "hello world"},
		destinations: handler.Destinations{dest},
		logger:       logger,
	}

	if res := task.Run(time.Now()); res.Status() != StatusFailed {
		t.Errorf("unexpected result: %+v", res)
	} else if !errors.Is(res.Error(), initErr) {
		t.Errorf("expected %v, got %v", initErr, res.Error())
	}

	if len(h.chunks) != 0 {
		t.Errorf("expected 0 chunks, got %d", len(h.chunks))
	}

	expectedLogs := []string{"ERROR (Initialization failed): handler could not be initialized: init error"}
	if logs := lines(); !reflect.DeepEqual(logs, expectedLogs) {
		t.Errorf("expected %q, got %q", expectedLogs, logs)
	}
}

func TestRunLastRunError(t *testing.T) {
	t.Parallel()

	lastRunErr := errors.New("last run error")
	h := &testHandler{lastRunErr: lastRunErr}
	dest := handler.Destination{}
	dest.SetHandler(h)
	logger, lines := newTestLogger()
	task := &Task{
		command:      []string{"echo", "hello world"},
		destinations: handler.Destinations{dest},
		logger:       logger,
	}

	if res := task.Run(time.Now()); res.Status() != StatusFailed {
		t.Errorf("unexpected result: %+v", res)
	} else if !errors.Is(res.Error(), lastRunErr) {
		t.Errorf("expected %v, got %v", lastRunErr, res.Error())
	}

	if len(h.chunks) != 0 {
		t.Errorf("expected 0 chunks, got %d", len(h.chunks))
	}

	expectedLogs := []string{"ERROR (Could not find last run): 1 error occurred:", "\t* last run error", ""}
	if logs := lines(); !reflect.DeepEqual(logs, expectedLogs) {
		t.Errorf("expected %q, got %q", expectedLogs, logs)
	}
}

func TestTaskRunner(t *testing.T) {
	t.Parallel()

	type testCase struct {
		handler  *testHandler
		command  []string
		status   Status
		errCodes []ErrorCode
		logs     []string
		chunks   []string
	}
	testCases := map[string]testCase{
		"ok": {
			handler:  &testHandler{},
			command:  []string{"echo", "foo bar"},
			status:   StatusSuccess,
			errCodes: nil,
			logs:     []string{"DONE"},
			chunks:   []string{"foo bar\n"},
		},
		"handler_init_error": {
			handler:  &testHandler{initErr: errors.New("test error")},
			command:  []string{"echo", "foo bar"},
			status:   StatusFailed,
			errCodes: []ErrorCode{HandlerError},
			logs:     []string{"ERROR (Initialization failed): test error"},
			chunks:   []string{},
		},
		"handler_upload_error": {
			handler:  &testHandler{err: errors.New("test error")},
			command:  []string{"echo", "foo bar"},
			status:   StatusFailed,
			errCodes: []ErrorCode{HandlerError},
			logs:     []string{"ERROR (Upload failed): test error"},
			chunks:   []string{"foo bar\n"},
		},
		"handler_abort_error": {
			handler:  &testHandler{err: errors.New("test error")},
			command:  []string{"false"},
			status:   StatusFailed,
			errCodes: []ErrorCode{CommandFailedError, HandlerError},
			logs:     []string{"ERROR (Command failed): exit status 1", "ERROR (Upload abort failed): test error"},
			chunks:   []string{},
		},
		"start_error": {
			handler:  &testHandler{},
			command:  []string{"this-cmd-does-not-exist"},
			status:   StatusFailed,
			errCodes: []ErrorCode{CommandStartError},
			logs:     []string{"ERROR (Command start): exec: \"this-cmd-does-not-exist\": executable file not found in $PATH"},
			chunks:   []string{},
		},
		"non_zero_exit_code": {
			handler:  &testHandler{chunkSize: 7},
			command:  []string{"bash", "-c", "echo output && echo error >&2 && exit 42"},
			status:   StatusFailed,
			errCodes: []ErrorCode{CommandFailedError},
			logs:     []string{"error", "ERROR (Command failed): exit status 42"},
			chunks:   []string{"output\n"},
		},
		"timeout": {
			handler:  &testHandler{},
			command:  []string{"bash", "-c", "sleep 1 && echo output && echo error >&2 && exit 42"},
			status:   StatusTimeout,
			errCodes: []ErrorCode{CommandTimeoutError},
			logs:     []string{"TIMEOUT (Command took more than 30ms)"},
			chunks:   []string{},
		},
	}
	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			logger, lines := newTestLogger()
			task := &Task{
				command: tc.command,
				timeout: 30 * time.Millisecond,
				logger:  logger,
			}

			result := task.runner(time.Now(), []handler.Handler{tc.handler})
			if result.Status() != tc.status {
				t.Errorf("expected status %+v, got %+v", tc.status, result.Status())
			}
			switch tc.status {
			case StatusSuccess:
				if result.Error() != nil {
					t.Errorf("unexpected error, got %+v", result.Error())
				}
			default:
				for _, code := range tc.errCodes {
					if err := result.Error(); !IsTaskError(err, code) {
						t.Errorf("expected error code %+v, got %+v", code, err)
					}
				}
			}

			if logs := lines(); !reflect.DeepEqual(logs, tc.logs) {
				t.Errorf("expected logs %q, got %q", tc.logs, logs)
			}

			chunks := []string{}
			for _, chunk := range tc.handler.chunks {
				chunks = append(chunks, string(chunk))
			}
			if !reflect.DeepEqual(chunks, tc.chunks) {
				t.Errorf("expected data %#v, got %#v", tc.chunks, chunks)
			}
		})
	}
}

func TestTaskExecCommand(t *testing.T) {
	t.Parallel()

	type testCase struct {
		command  []string
		errCodes []ErrorCode
		logs     []string
		stdout   string
		stderr   string
	}
	testCases := map[string]testCase{
		"ok": {
			command:  []string{"echo", "foo bar"},
			errCodes: nil,
			logs:     []string{},
			stdout:   "foo bar\n",
			stderr:   "",
		},
		"start_error": {
			command:  []string{"this-cmd-does-not-exist"},
			errCodes: []ErrorCode{CommandStartError},
			logs:     []string{"ERROR (Command start): exec: \"this-cmd-does-not-exist\": executable file not found in $PATH"},
			stdout:   "",
			stderr:   "",
		},
		"non_zero_exit_code": {
			command:  []string{"bash", "-c", "echo output && echo error >&2 && exit 42"},
			errCodes: []ErrorCode{CommandFailedError},
			logs:     []string{"ERROR (Command failed): exit status 42"},
			stdout:   "output\n",
			stderr:   "error\n",
		},
		"timeout": {
			command:  []string{"bash", "-c", "sleep 1 && echo output && echo error >&2 && exit 42"},
			errCodes: []ErrorCode{CommandTimeoutError},
			logs:     []string{"TIMEOUT (Command took more than 30ms)"},
			stdout:   "",
			stderr:   "",
		},
	}
	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			logger, lines := newTestLogger()
			task := &Task{
				command: tc.command,
				timeout: 30 * time.Millisecond,
				logger:  logger,
			}
			stdout := bytes.NewBuffer(nil)
			stderr := bytes.NewBuffer(nil)

			if err := task.execCommand(stdout, stderr); tc.errCodes == nil {
				if err != nil {
					t.Errorf("unexpected error, got %+v", err)
				}
			} else {
				for _, code := range tc.errCodes {
					if !IsTaskError(err, code) {
						t.Errorf("expected error code %+v, got %+v", code, err)
					}
				}
			}

			if logs := lines(); !reflect.DeepEqual(logs, tc.logs) {
				t.Errorf("expected logs %q, got %q", tc.logs, logs)
			}
			if data := stdout.String(); data != tc.stdout {
				t.Errorf("expected stdout %q, got %q", tc.stdout, data)
			}
			if data := stderr.String(); data != tc.stderr {
				t.Errorf("expected stderr %q, got %q", tc.stderr, data)
			}
		})
	}
}
