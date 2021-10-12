package backup

import (
	"os"
	"reflect"
	"sort"
	"testing"
)

func TestOperationResultAccessors(t *testing.T) {
	cwd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}

	t.Run("without_operation", func(t *testing.T) {
		result := &Result{}

		if name := result.Name(); name != UNKNOWN_OPERATION {
			t.Errorf("expected %s, got %s", UNKNOWN_OPERATION, name)
		}
		if cmd := result.Command(); cmd != UNKNOWN_OPERATION {
			t.Errorf("expected %s, got %s", UNKNOWN_OPERATION, cmd)
		}
		if wd := result.ActualCwd(); wd != cwd {
			t.Errorf("expected %s, got %s", cwd, wd)
		}
	})

	t.Run("with_operation", func(t *testing.T) {
		tmpDir := t.TempDir()

		result := &Result{
			Operation: &Operation{
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

func TestOperationResultsSort(t *testing.T) {
	t.Parallel()

	results := OperationResults{
		{Status: StatusFailure, Operation: &Operation{Name: "test e"}},
		{Status: StatusSuccess, Operation: &Operation{Name: "test b"}},
		{Status: StatusSkipped, Operation: &Operation{Name: "test d"}},
		{Status: StatusSuccess, Operation: &Operation{Name: "test c"}},
		{Status: StatusFailure, Operation: &Operation{Name: "test a"}},
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

	expected := OperationResults{
		{Status: StatusSkipped, Operation: &Operation{Name: "test d"}},
		{Status: StatusSuccess, Operation: &Operation{Name: "test b"}},
		{Status: StatusSuccess, Operation: &Operation{Name: "test c"}},
		{Status: StatusFailure, Operation: &Operation{Name: "test a"}},
		{Status: StatusFailure, Operation: &Operation{Name: "test e"}},
	}
	if !reflect.DeepEqual(results, expected) {
		t.Errorf("expected %v, got %v", expected, results)
	}
}
