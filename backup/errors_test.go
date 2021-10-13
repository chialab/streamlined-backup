package backup

import (
	"errors"
	"testing"
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
