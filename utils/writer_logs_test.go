package utils

import (
	"bytes"
	"log"
	"reflect"
	"strings"
	"testing"
)

func newTestLogger() (*log.Logger, func() []string) {
	var buf bytes.Buffer
	logger := log.New(&buf, "", 0)

	lines := func() []string {
		lines := strings.Split(buf.String(), "\n")
		if lines[len(lines)-1] == "" {
			lines = lines[:len(lines)-1]
		}

		return lines
	}

	return logger, lines
}

func TestLogWriter(t *testing.T) {
	t.Parallel()

	type testCase struct {
		useUnderlyingLogger bool
		steps               []struct {
			input    string
			expected []string
		}
		final []string
	}

	testCases := map[string]testCase{
		"no_underlying_logger": {
			useUnderlyingLogger: false,
			steps: []struct {
				input    string
				expected []string
			}{
				{input: "foo", expected: []string{}},
				{input: "bar\n", expected: []string{"foobar"}},
				{input: "baz\nbazinga\ngo lang go", expected: []string{"foobar", "baz", "bazinga"}},
			},
			final: []string{"foobar", "baz", "bazinga", "go lang go"},
		},
		"with_underlying_logger": {
			useUnderlyingLogger: true,
			steps: []struct {
				input    string
				expected []string
			}{
				{input: "foo", expected: []string{}},
				{input: "bar\n", expected: []string{"foobar"}},
				{input: "baz\nbazinga\ngo lang go", expected: []string{"foobar", "baz", "bazinga"}},
			},
			final: []string{"foobar", "baz", "bazinga", "go lang go"},
		},
		"with_underlying_logger_empty_final_line": {
			useUnderlyingLogger: true,
			steps: []struct {
				input    string
				expected []string
			}{
				{input: "foo", expected: []string{}},
				{input: "bar\n", expected: []string{"foobar"}},
				{input: "baz\nbazinga\n", expected: []string{"foobar", "baz", "bazinga"}},
			},
			final: []string{"foobar", "baz", "bazinga"},
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			logger, lines := newTestLogger()

			writer := NewLogWriter(nil)
			if tc.useUnderlyingLogger {
				writer = NewLogWriter(logger)
			}

			for _, step := range tc.steps {
				if b, err := writer.Write([]byte(step.input)); err != nil {
					t.Fatalf("unexpected error: %s", err)
				} else if b != len(step.input) {
					t.Errorf("expected %d bytes written, got %d", len(step.input), b)
				}

				if lines := writer.Lines(); !reflect.DeepEqual(step.expected, lines) {
					t.Errorf("expected %#v, got %#v", step.expected, lines)
				}
				if logs := lines(); tc.useUnderlyingLogger {
					if !reflect.DeepEqual(step.expected, logs) {
						t.Errorf("expected %#v, got %#v", step.expected, logs)
					}
				}
			}

			writer.Close()
			if lines := writer.Lines(); !reflect.DeepEqual(tc.final, lines) {
				t.Errorf("expected %#v, got %#v", tc.final, lines)
			}
			if logs := lines(); tc.useUnderlyingLogger {
				if !reflect.DeepEqual(tc.final, logs) {
					t.Errorf("expected %#v, got %#v", tc.final, logs)
				}
			}
		})
	}
}
