package backup

import (
	"errors"
	"testing"
)

func TestToError(t *testing.T) {
	t.Parallel()

	type testCase struct {
		expected string
		val      interface{}
	}
	testCases := map[string]testCase{
		"nil": {
			expected: "<nil>",
			val:      nil,
		},
		"string": {
			expected: "test",
			val:      "test",
		},
		"error": {
			expected: "test",
			val:      errors.New("test"),
		},
		"list": {
			expected: "[foo bar]",
			val:      []string{"foo", "bar"},
		},
	}
	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			if actual := ToError(tc.val); actual.Error() != tc.expected {
				t.Errorf("expected %q, got %q", tc.expected, actual)
			}
		})
	}
}
