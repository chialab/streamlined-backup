package backup

import (
	"reflect"
	"testing"
	"time"
)

type testOperation struct {
	result OperationResult
	delay  time.Duration
}

func (t testOperation) Run(now time.Time) OperationResult {
	time.Sleep(t.delay)

	return t.result
}

func TestRunOperations(t *testing.T) {
	operations := Operations{
		testOperation{
			result: OperationResult{Status: StatusSuccess, Logs: []string{"fourth to complete"}},
			delay:  time.Millisecond * 70,
		},
		testOperation{
			result: OperationResult{Status: StatusSuccess, Logs: []string{"first to complete"}},
			delay:  time.Millisecond * 30,
		},
		testOperation{
			result: OperationResult{Status: StatusSkipped, Logs: []string{"second to complete"}},
			delay:  time.Millisecond * 20,
		},
		testOperation{
			result: OperationResult{Status: StatusFailure, Logs: []string{"third to complete"}},
			delay:  time.Millisecond * 10,
		},
		testOperation{
			result: OperationResult{Status: StatusFailure, Logs: []string{"sixth to complete"}},
			delay:  time.Millisecond * 40,
		},
		testOperation{
			result: OperationResult{Status: StatusSuccess, Logs: []string{"fifth to complete"}},
			delay:  time.Millisecond * 20,
		},
	}
	results := operations.Run(time.Now(), 2)
	expected := OperationResults{
		OperationResult{Status: StatusSuccess, Logs: []string{"first to complete"}},
		OperationResult{Status: StatusSkipped, Logs: []string{"second to complete"}},
		OperationResult{Status: StatusFailure, Logs: []string{"third to complete"}},
		OperationResult{Status: StatusSuccess, Logs: []string{"fourth to complete"}},
		OperationResult{Status: StatusSuccess, Logs: []string{"fifth to complete"}},
		OperationResult{Status: StatusFailure, Logs: []string{"sixth to complete"}},
	}
	if !reflect.DeepEqual(results, expected) {
		t.Errorf("Expected %v, got %v", expected, results)
	}
}
