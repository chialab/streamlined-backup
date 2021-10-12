package backup

import (
	"os"

	"github.com/alessio/shellescape"
)

type OperationStatus string

const (
	StatusSuccess OperationStatus = "success"
	StatusFailure OperationStatus = "failure"
	StatusSkipped OperationStatus = "skipped"
)

func (status OperationStatus) Priority() uint {
	switch status {
	case StatusSuccess:
		return 10
	case StatusFailure:
		return 20
	default:
		return 0
	}
}

const UNKNOWN_OPERATION = "(unknown)"

type Result struct {
	Status    OperationStatus
	Operation *Operation
	Logs      []string
	Error     error
}

func (r Result) Name() string {
	if r.Operation == nil {
		return UNKNOWN_OPERATION
	}

	return r.Operation.Name
}

func (r Result) Command() string {
	if r.Operation == nil || r.Operation.Command == nil {
		return UNKNOWN_OPERATION
	}

	return shellescape.QuoteCommand(r.Operation.Command)
}

func (r Result) ActualCwd() string {
	if r.Operation == nil || r.Operation.Cwd == "" {
		if cwd, err := os.Getwd(); err == nil {
			return cwd
		} else {
			return UNKNOWN_OPERATION
		}
	}

	return r.Operation.Cwd
}

type OperationResults []Result

func (o OperationResults) Len() int {
	return len(o)
}
func (o OperationResults) Swap(i, j int) {
	o[i], o[j] = o[j], o[i]
}
func (o OperationResults) Less(i, j int) bool {
	if o[i].Status != o[j].Status {
		return o[i].Status.Priority() < o[j].Status.Priority()
	}
	return o[i].Name() < o[j].Name()
}
