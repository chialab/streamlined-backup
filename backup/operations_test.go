package backup

import (
	"reflect"
	"testing"
	"time"
)

type testOperation struct {
	result Result
	delay  time.Duration
}

func (t testOperation) Run(now time.Time) Result {
	time.Sleep(t.delay)

	return t.result
}

func TestRunOperations(t *testing.T) {
	operations := Operations{
		testOperation{
			result: Result{Status: StatusSuccess, Logs: []string{"fourth to complete"}},
			delay:  time.Millisecond * 80,
		},
		testOperation{
			result: Result{Status: StatusSuccess, Logs: []string{"first to complete"}},
			delay:  time.Millisecond * 30,
		},
		testOperation{
			result: Result{Status: StatusSkipped, Logs: []string{"second to complete"}},
			delay:  time.Millisecond * 20,
		},
		testOperation{
			result: Result{Status: StatusFailure, Logs: []string{"third to complete"}},
			delay:  time.Millisecond * 10,
		},
		testOperation{
			result: Result{Status: StatusFailure, Logs: []string{"sixth to complete"}},
			delay:  time.Millisecond * 50,
		},
		testOperation{
			result: Result{Status: StatusSuccess, Logs: []string{"fifth to complete"}},
			delay:  time.Millisecond * 20,
		},
	}
	results := operations.Run(time.Now(), 2)
	expected := OperationResults{
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
