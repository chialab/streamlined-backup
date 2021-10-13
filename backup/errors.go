package backup

import "fmt"

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
