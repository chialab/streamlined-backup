package backup

import (
	"os"

	"github.com/alessio/shellescape"
)

type Status string

const (
	StatusSuccess Status = "success"
	StatusFailure Status = "failure"
	StatusSkipped Status = "skipped"
)

func (status Status) Priority() uint {
	switch status {
	case StatusSuccess:
		return 10
	case StatusFailure:
		return 20
	default:
		return 0
	}
}

const UNKNOWN_TASK = "(unknown)"

type Result struct {
	Status Status
	Task   *Task
	Logs   []string
	Error  error
}

func (r Result) Name() string {
	if r.Task == nil {
		return UNKNOWN_TASK
	}

	return r.Task.Name
}

func (r Result) Command() string {
	if r.Task == nil || r.Task.Command == nil {
		return UNKNOWN_TASK
	}

	return shellescape.QuoteCommand(r.Task.Command)
}

func (r Result) ActualCwd() string {
	if r.Task == nil || r.Task.Cwd == "" {
		if cwd, err := os.Getwd(); err == nil {
			return cwd
		} else {
			return UNKNOWN_TASK
		}
	}

	return r.Task.Cwd
}

type Results []Result

func (r Results) Len() int {
	return len(r)
}
func (r Results) Swap(i, j int) {
	r[i], r[j] = r[j], r[i]
}
func (r Results) Less(i, j int) bool {
	if r[i].Status != r[j].Status {
		return r[i].Status.Priority() < r[j].Status.Priority()
	}
	return r[i].Name() < r[j].Name()
}
