package backup

import (
	"strings"
	"testing"
)

func newTestLogFunction() (logFunction, *[]string) {
	var logs []string
	return func(msg string) {
		logs = append(logs, msg)
	}, &logs
}

func TestLogWriter(t *testing.T) {
	t.Parallel()

	fn, logs := newTestLogFunction()
	writer := NewLogWriter(fn)

	writer.Write([]byte("foo"))
	if len(*logs) != 0 {
		t.Error("logs should be empty")
	}

	writer.Write([]byte("bar\n"))
	if len(*logs) != 1 {
		t.Error("logs should have 1 element")
	} else if firstLog := (*logs)[0]; firstLog != "foobar" {
		t.Errorf("first log entry should be 'foobar', got %s", firstLog)
	}

	writer.Write([]byte("baz\nbazinga\ngo lang go"))
	if len(*logs) != 3 {
		t.Error("logs should have 3 elements")
	} else if logs := strings.Join(*logs, "|"); logs != "foobar|baz|bazinga" {
		t.Errorf("logs should be 'foobar|baz|bazinga', got %s", logs)
	}

	writer.Close()

	if len(*logs) != 4 {
		t.Error("logs should have 4 elements")
	} else if logs := strings.Join(*logs, "|"); logs != "foobar|baz|bazinga|go lang go" {
		t.Errorf("logs should be 'foobar|baz|bazinga|go lang go', got %s", logs)
	}
}
