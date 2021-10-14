package backup

import (
	"errors"
	"fmt"
	"testing"

	"github.com/hashicorp/go-multierror"
)

func TestTaskError(t *testing.T) {
	t.Parallel()

	type testCase struct {
		expected string
		code     ErrorCode
		format   string
		previous error
	}

	testCases := map[string]testCase{
		"full": {
			expected: "format: test error",
			code:     HandlerError,
			format:   "format: %s",
			previous: errors.New("test error"),
		},
		"no_previous": {
			expected: "format",
			code:     HandlerError,
			format:   "format",
			previous: nil,
		},
	}
	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			err := NewTaskError(tc.code, tc.format, tc.previous)

			if err.Code() != tc.code {
				t.Errorf("expected code %d, got %d", tc.code, err.Code())
			}
			if err.Error() != tc.expected {
				t.Errorf("expected error %s, got %s", tc.expected, err.Error())
			}
			if err.Unwrap() != tc.previous {
				t.Errorf("expected previous error %s, got %s", tc.previous, err.Unwrap())
			}
			if tc.previous != nil {
				if !errors.Is(err, tc.previous) {
					t.Errorf("expected previous error %s, got %s", tc.previous, err.Unwrap())
				}

				var previous error
				if !errors.As(err, &previous) {
					t.Errorf("expected previous error %s, got %s", tc.previous, err.Unwrap())
				}
			}
		})
	}
}

func TestIsTaskError(t *testing.T) {
	t.Parallel()

	type testCase struct {
		expected bool
		err      error
		codes    []ErrorCode
	}
	testCases := map[string]testCase{
		"nil": {
			err:      nil,
			expected: false,
			codes:    []ErrorCode{},
		},
		"TaskError": {
			err:      NewTaskError(CommandTimeoutError, "test error", nil),
			expected: true,
			codes:    []ErrorCode{CommandFailedError, CommandTimeoutError},
		},
		"TaskError_wrong_code": {
			err:      NewTaskError(HandlerError, "test error", nil),
			expected: false,
			codes:    []ErrorCode{CommandFailedError, CommandTimeoutError},
		},
		"TaskError_no_code_check": {
			err:      NewTaskError(HandlerError, "test error", nil),
			expected: true,
			codes:    []ErrorCode{},
		},
		"TaskError_wrapped": {
			err:      fmt.Errorf("wrapping: %w", NewTaskError(CommandTimeoutError, "test error", nil)),
			expected: true,
			codes:    []ErrorCode{CommandTimeoutError},
		},
		"MultiError": {
			err: &multierror.Error{
				Errors: []error{
					NewTaskError(CommandTimeoutError, "test error", nil),
					NewTaskError(CommandFailedError, "test error", nil),
				},
			},
			expected: true,
			codes:    []ErrorCode{CommandFailedError},
		},
		"MultiError_no_TaskError": {
			err: &multierror.Error{
				Errors: []error{
					errors.New("test error"),
					errors.New("test error"),
				},
			},
			expected: false,
			codes:    []ErrorCode{},
		},
		"other": {
			err:      errors.New("test error"),
			expected: false,
			codes:    []ErrorCode{},
		},
	}
	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			if actual := IsTaskError(tc.err, tc.codes...); actual != tc.expected {
				t.Errorf("expected %t, got %t", tc.expected, actual)
			}
		})
	}
}
