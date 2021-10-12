package backup

import (
	"testing"
	"time"
)

func TestKey(t *testing.T) {
	t.Parallel()

	type testCase struct {
		expected, prefix, suffix string
		timestamp                time.Time
	}
	cases := map[string]testCase{
		"prefix_suffix": {
			expected:  "foo/20211008131625-bar.sql",
			prefix:    "foo/",
			suffix:    "-bar.sql",
			timestamp: time.Date(2021, 10, 8, 13, 16, 25, 0, time.Local),
		},
		"empty_prefix": {
			expected:  "20211008131625.sql",
			prefix:    "",
			suffix:    ".sql",
			timestamp: time.Date(2021, 10, 8, 13, 16, 25, 0, time.Local),
		},
	}
	for name, testCase := range cases {
		t.Run(name, func(t *testing.T) {
			dest := &Destination{
				Prefix: testCase.prefix,
				Suffix: testCase.suffix,
			}
			actual := dest.Key(testCase.timestamp)
			if testCase.expected != actual {
				t.Errorf("expected %s, got %s", testCase.expected, actual)
			}
		})
	}
}

func TestParseTimestamp(t *testing.T) {
	t.Parallel()

	type testCase struct {
		expected            time.Time
		prefix, suffix, key string
	}
	cases := map[string]testCase{
		"prefix_suffix": {
			expected: time.Date(2021, 10, 8, 13, 16, 25, 0, time.Local),
			prefix:   "foo/",
			suffix:   "-bar.sql",
			key:      "foo/20211008131625-bar.sql",
		},
		"empty_prefix": {
			expected: time.Date(2021, 10, 8, 13, 16, 25, 0, time.Local),
			prefix:   "",
			suffix:   ".sql",
			key:      "20211008131625.sql",
		},
	}
	for name, testCase := range cases {
		t.Run(name, func(t *testing.T) {
			dest := &Destination{
				Prefix: testCase.prefix,
				Suffix: testCase.suffix,
			}

			if actual, err := dest.ParseTimestamp(testCase.key); err != nil {
				t.Errorf("unexpected error: %s", err)
			} else if !actual.Equal(testCase.expected) {
				t.Errorf("expected %s, got %s", testCase.expected, actual)
			}
		})
	}

	type errorCase struct {
		prefix, suffix, key string
	}
	errorCases := map[string]errorCase{
		"invalid_prefix": {
			prefix: "foo/",
			suffix: "-bar.sql",
			key:    "bar/20211008131625-bar.sql",
		},
		"invalid_suffix": {
			prefix: "",
			suffix: ".sql",
			key:    "20211008131625.tar.gz",
		},
		"invalid_timestamp": {
			prefix: "",
			suffix: ".sql",
			key:    "invalid.sql",
		},
	}
	for name, testCase := range errorCases {
		t.Run(name, func(t *testing.T) {
			dest := &Destination{
				Prefix: testCase.prefix,
				Suffix: testCase.suffix,
			}

			if actual, err := dest.ParseTimestamp(testCase.key); err == nil {
				t.Errorf("expected error, got %s", actual)
			} else if !actual.IsZero() {
				t.Errorf("expected zero time, got %s", actual)
			}
		})
	}
}
