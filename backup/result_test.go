package backup

import (
	"os"
	"reflect"
	"sort"
	"testing"
)

func TestResultAccessors(t *testing.T) {
	t.Parallel()

	cwd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}

	t.Run("without_task_definition", func(t *testing.T) {
		result := &Result{}

		if name := result.Name(); name != UNKNOWN_TASK {
			t.Errorf("expected %s, got %s", UNKNOWN_TASK, name)
		}
		if cmd := result.Command(); cmd != UNKNOWN_TASK {
			t.Errorf("expected %s, got %s", UNKNOWN_TASK, cmd)
		}
		if wd := result.ActualCwd(); wd != cwd {
			t.Errorf("expected %s, got %s", cwd, wd)
		}
	})

	t.Run("with_task_definition", func(t *testing.T) {
		tmpDir := t.TempDir()

		result := &Result{
			Task: &Task{
				Name: "test",
				Command: []string{
					"bash",
					"-c",
					"echo 'hello world' \"${PWD:-/tmp}\" | bzip2",
				},
				Cwd: tmpDir,
			},
		}

		if name := result.Name(); name != "test" {
			t.Errorf("expected %s, got %s", "test", name)
		}
		escaped := "bash -c 'echo '\"'\"'hello world'\"'\"' \"${PWD:-/tmp}\" | bzip2'"
		if cmd := result.Command(); cmd != escaped {
			t.Errorf("expected %s, got %s", escaped, cmd)
		}
		if cwd := result.ActualCwd(); cwd != tmpDir {
			t.Errorf("expected %s, got %s", tmpDir, cwd)
		}
	})

}

func TestResultsSort(t *testing.T) {
	t.Parallel()

	results := Results{
		{Status: StatusFailure, Task: &Task{Name: "test e"}},
		{Status: StatusSuccess, Task: &Task{Name: "test b"}},
		{Status: StatusSkipped, Task: &Task{Name: "test d"}},
		{Status: StatusSuccess, Task: &Task{Name: "test c"}},
		{Status: StatusFailure, Task: &Task{Name: "test a"}},
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
		{Status: StatusSkipped, Task: &Task{Name: "test d"}},
		{Status: StatusSuccess, Task: &Task{Name: "test b"}},
		{Status: StatusSuccess, Task: &Task{Name: "test c"}},
		{Status: StatusFailure, Task: &Task{Name: "test a"}},
		{Status: StatusFailure, Task: &Task{Name: "test e"}},
	}
	if !reflect.DeepEqual(results, expected) {
		t.Errorf("expected %v, got %v", expected, results)
	}
}
