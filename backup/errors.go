package backup

import (
	"errors"
	"fmt"

	"github.com/hashicorp/go-multierror"
)

type ErrorCode int

const (
	HandlerError = iota
	CommandStartError
	CommandFailedError
	CommandTimeoutError
	CommandKillError
)

type TaskError struct {
	code     ErrorCode
	format   string
	previous error
}

func NewTaskError(code ErrorCode, format string, previous error) *TaskError {
	return &TaskError{
		code:     code,
		format:   format,
		previous: previous,
	}
}

func (e TaskError) Code() ErrorCode {
	return e.code
}

func (e TaskError) Error() string {
	if e.previous == nil {
		return e.format
	}

	return fmt.Sprintf(e.format, e.previous)
}

func (e TaskError) Unwrap() error {
	return e.previous
}

func IsTaskError(err error, code ...ErrorCode) bool {
	if err == nil {
		return false
	}

	check := func(taskErr *TaskError) bool {
		if len(code) == 0 {
			return true
		}

		for _, c := range code {
			if taskErr.Code() == c {
				return true
			}
		}

		return false
	}

	if taskErr, ok := err.(*TaskError); ok {
		return check(taskErr)
	} else if merr, ok := err.(*multierror.Error); ok {
		for _, err := range merr.Errors {
			if IsTaskError(err, code...) {
				return true
			}
		}

		return false
	} else if taskErr := new(TaskError); errors.As(err, &taskErr) {
		return check(taskErr)
	}

	return false
}
