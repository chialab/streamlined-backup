package backup

import (
	"errors"
	"os"
	"reflect"
	"sort"
	"testing"
)

func TestResultConstructors(t *testing.T) {
	type testCase struct {
		ctor   func() Result
		status Status
		task   *Task
		err    error
		logs   []string
	}

	task := &Task{command: []string{"echo", "hello world"}}
	err := errors.New("test error")
	logs := []string{"test log 1", "test log 2"}
	testCases := map[string]testCase{
		"success": {
			ctor: func() Result {
				return NewResultSuccess(task, logs)
			},
			status: StatusSuccess,
			task:   task,
			err:    nil,
			logs:   logs,
		},
		"skipped": {
			ctor: func() Result {
				return NewResultSkipped(task)
			},
			status: StatusSkipped,
			task:   task,
			err:    nil,
			logs:   []string{},
		},
		"failed": {
			ctor: func() Result {
				return NewResultFailed(task, err, logs)
			},
			status: StatusFailed,
			task:   task,
			err:    err,
			logs:   logs,
		},
		"timed_out": {
			ctor: func() Result {
				return NewResultTimeout(task, logs)
			},
			status: StatusTimeout,
			task:   task,
			err:    nil,
			logs:   logs,
		},
	}
	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			result := tc.ctor()
			if result.status != tc.status {
				t.Errorf("expected status %s, got %s", tc.status, result.status)
			}
			if result.task != tc.task {
				t.Errorf("expected task %v, got %v", tc.task, result.task)
			}
			if result.err != tc.err {
				t.Errorf("expected err %v, got %v", tc.err, result.err)
			}
			if !reflect.DeepEqual(result.logs, tc.logs) {
				t.Errorf("expected logs %v, got %v", tc.logs, result.logs)
			}
		})
	}
}

func TestResultAccessors(t *testing.T) {
	t.Parallel()

	t.Run("without_task_definition", func(t *testing.T) {
		result := NewResultSkipped(nil)

		if status := result.Status(); status != StatusSkipped {
			t.Errorf("expected status %s, got %s", StatusSkipped, status)
		}
		if name := result.Name(); name != UNKNOWN_TASK {
			t.Errorf("expected %s, got %s", UNKNOWN_TASK, name)
		}
		if cmd := result.Command(); cmd != UNKNOWN_TASK {
			t.Errorf("expected %s, got %s", UNKNOWN_TASK, cmd)
		}
		if wd := result.ActualCwd(); wd != UNKNOWN_TASK {
			t.Errorf("expected %s, got %s", UNKNOWN_TASK, wd)
		}
		if err := result.Error(); err != nil {
			t.Errorf("unexpected error, got %+v", err)
		}
		if logs := result.Logs(); !reflect.DeepEqual(logs, []string{}) {
			t.Errorf("expected empty logs, got %+v", logs)
		}
	})

	t.Run("with_task_definition", func(t *testing.T) {
		tmpDir := t.TempDir()

		testErr := errors.New("test error")
		testLogs := []string{"test log 1", "test log 2"}
		result := NewResultFailed(&Task{
			name: "test",
			command: []string{
				"bash",
				"-c",
				"echo 'hello world' \"${PWD:-/tmp}\" | bzip2",
			},
			cwd: tmpDir,
		}, testErr, testLogs)

		if status := result.Status(); status != StatusFailed {
			t.Errorf("expected status %s, got %s", StatusFailed, status)
		}
		if name := result.Name(); name != "test" {
			t.Errorf("expected %s, got %s", "test", name)
		}
		escaped := "bash -c 'echo '\"'\"'hello world'\"'\"' \"${PWD:-/tmp}\" | bzip2'"
		if cmd := result.Command(); cmd != escaped {
			t.Errorf("expected %s, got %s", escaped, cmd)
		}
		if wd := result.ActualCwd(); wd != tmpDir {
			t.Errorf("expected %s, got %s", tmpDir, wd)
		}
		if err := result.Error(); err != testErr {
			t.Errorf("expected %+v, got %+v", testErr, err)
		}
		if logs := result.Logs(); !reflect.DeepEqual(logs, testLogs) {
			t.Errorf("expected %+v, got %+v", testLogs, logs)
		}
	})

	t.Run("with_current_directory", func(t *testing.T) {
		cwd, err := os.Getwd()
		if err != nil {
			t.Fatal(err)
		}

		testLogs := []string{"test log 1", "test log 2"}
		result := NewResultSuccess(&Task{
			name:    "test",
			command: []string{"echo", "hello world"},
		}, testLogs)

		if status := result.Status(); status != StatusSuccess {
			t.Errorf("expected status %s, got %s", StatusSuccess, status)
		}
		if name := result.Name(); name != "test" {
			t.Errorf("expected %s, got %s", "test", name)
		}
		escaped := "echo 'hello world'"
		if cmd := result.Command(); cmd != escaped {
			t.Errorf("expected %s, got %s", escaped, cmd)
		}
		if wd := result.ActualCwd(); wd != cwd {
			t.Errorf("expected %s, got %s", cwd, wd)
		}
		if err := result.Error(); err != nil {
			t.Errorf("unexpected error, got %+v", err)
		}
		if logs := result.Logs(); !reflect.DeepEqual(logs, testLogs) {
			t.Errorf("expected %+v, got %+v", testLogs, logs)
		}
	})
}

func TestResultsSort(t *testing.T) {
	t.Parallel()

	results := Results{
		{status: StatusFailed, task: &Task{name: "test e"}},
		{status: StatusSuccess, task: &Task{name: "test b"}},
		{status: StatusSkipped, task: &Task{name: "test d"}},
		{status: StatusTimeout, task: &Task{name: "test f"}},
		{status: StatusSuccess, task: &Task{name: "test c"}},
		{status: StatusFailed, task: &Task{name: "test a"}},
	}
	if results.Len() != 6 {
		t.Errorf("expected 6 results, got %d", results.Len())
	}
	if results.Less(0, 1) {
		t.Errorf("expected part 1 to be less than 0")
	}
	if !results.Less(2, 1) {
		t.Errorf("expected part 2 to be less than 1")
	}

	sort.Sort(results)

	expected := Results{
		{status: StatusSkipped, task: &Task{name: "test d"}},
		{status: StatusSuccess, task: &Task{name: "test b"}},
		{status: StatusSuccess, task: &Task{name: "test c"}},
		{status: StatusFailed, task: &Task{name: "test a"}},
		{status: StatusFailed, task: &Task{name: "test e"}},
		{status: StatusTimeout, task: &Task{name: "test f"}},
	}
	if !reflect.DeepEqual(results, expected) {
		t.Errorf("expected %v, got %v", expected, results)
	}
}
