package backup

import (
	"reflect"
	"testing"
	"time"
)

type testTask struct {
	result Result
	delay  time.Duration
}

func (t testTask) Run(now time.Time) Result {
	time.Sleep(t.delay)

	return t.result
}

func TestRunTasks(t *testing.T) {
	tasks := TasksList{
		testTask{
			result: Result{Status: StatusSuccess, Logs: []string{"fourth to complete"}},
			delay:  time.Millisecond * 80,
		},
		testTask{
			result: Result{Status: StatusSuccess, Logs: []string{"first to complete"}},
			delay:  time.Millisecond * 30,
		},
		testTask{
			result: Result{Status: StatusSkipped, Logs: []string{"second to complete"}},
			delay:  time.Millisecond * 20,
		},
		testTask{
			result: Result{Status: StatusFailure, Logs: []string{"third to complete"}},
			delay:  time.Millisecond * 10,
		},
		testTask{
			result: Result{Status: StatusFailure, Logs: []string{"sixth to complete"}},
			delay:  time.Millisecond * 50,
		},
		testTask{
			result: Result{Status: StatusSuccess, Logs: []string{"fifth to complete"}},
			delay:  time.Millisecond * 20,
		},
	}
	results := tasks.Run(time.Now(), 2)
	expected := Results{
		Result{Status: StatusSuccess, Logs: []string{"first to complete"}},
		Result{Status: StatusSkipped, Logs: []string{"second to complete"}},
		Result{Status: StatusFailure, Logs: []string{"third to complete"}},
		Result{Status: StatusSuccess, Logs: []string{"fourth to complete"}},
		Result{Status: StatusSuccess, Logs: []string{"fifth to complete"}},
		Result{Status: StatusFailure, Logs: []string{"sixth to complete"}},
	}
	if !reflect.DeepEqual(results, expected) {
		t.Errorf("Expected %v, got %v", expected, results)
	}
}
