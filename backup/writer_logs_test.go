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

	if b, err := writer.Write([]byte("foo")); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if b != 3 {
		t.Errorf("expected 3 bytes written, got %d", b)
	}
	if len(*logs) != 0 {
		t.Error("logs should be empty")
	}

	if b, err := writer.Write([]byte("bar\n")); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if b != 4 {
		t.Errorf("expected 4 bytes written, got %d", b)
	}
	if len(*logs) != 1 {
		t.Error("logs should have 1 element")
	} else if firstLog := (*logs)[0]; firstLog != "foobar" {
		t.Errorf("first log entry should be 'foobar', got %s", firstLog)
	}

	if b, err := writer.Write([]byte("baz\nbazinga\ngo lang go")); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if b != 22 {
		t.Errorf("expected 22 bytes written, got %d", b)
	}
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
