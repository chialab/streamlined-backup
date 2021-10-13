package backup

import (
	"os"
	"reflect"
	"sort"
	"testing"
)

func TestResultAccessors(t *testing.T) {
	t.Parallel()

	t.Run("without_task_definition", func(t *testing.T) {
		result := NewResultSkipped(nil)

		if name := result.Name(); name != UNKNOWN_TASK {
			t.Errorf("expected %s, got %s", UNKNOWN_TASK, name)
		}
		if cmd := result.Command(); cmd != UNKNOWN_TASK {
			t.Errorf("expected %s, got %s", UNKNOWN_TASK, cmd)
		}
		if wd := result.ActualCwd(); wd != UNKNOWN_TASK {
			t.Errorf("expected %s, got %s", UNKNOWN_TASK, wd)
		}
	})

	t.Run("with_task_definition", func(t *testing.T) {
		tmpDir := t.TempDir()

		result := NewResultSkipped(&Task{
			name: "test",
			command: []string{
				"bash",
				"-c",
				"echo 'hello world' \"${PWD:-/tmp}\" | bzip2",
			},
			cwd: tmpDir,
		})

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
	})

	t.Run("with_current_directory", func(t *testing.T) {
		cwd, err := os.Getwd()
		if err != nil {
			t.Fatal(err)
		}

		result := NewResultSkipped(&Task{
			name:    "test",
			command: []string{"echo", "hello world"},
		})

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
	})
}

func TestResultsSort(t *testing.T) {
	t.Parallel()

	results := Results{
		{status: StatusFailed, task: &Task{name: "test e"}},
		{status: StatusSuccess, task: &Task{name: "test b"}},
		{status: StatusSkipped, task: &Task{name: "test d"}},
		{status: StatusSuccess, task: &Task{name: "test c"}},
		{status: StatusFailed, task: &Task{name: "test a"}},
	}
	if results.Len() != 5 {
		t.Errorf("expected 5 results, got %d", results.Len())
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
	}
	if !reflect.DeepEqual(results, expected) {
		t.Errorf("expected %v, got %v", expected, results)
	}
}
