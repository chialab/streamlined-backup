package backup

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync"
	"testing"

	"github.com/hashicorp/go-multierror"
)

func TestSlackFormat(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()
	testCases := map[string]struct {
		input    *OperationResult
		expected map[string]interface{}
	}{
		"skipped": {
			input:    &OperationResult{Status: StatusSkipped},
			expected: nil,
		},
		"success": {
			input: &OperationResult{Status: StatusSuccess, Operation: &Operation{Name: "foo"}},
			expected: map[string]interface{}{
				"type": "section",
				"text": map[string]string{
					"type": "mrkdwn",
					"text": ":white_checkmark: Backup operation `foo` completed successfully.",
				},
			},
		},
		"failure": {
			input: &OperationResult{
				Status:    StatusFailure,
				Error:     fmt.Errorf("test error"),
				Logs:      []string{"test log 1", "test log 2", ""},
				Operation: &Operation{Name: "bar", Command: []string{"echo", "foo bar"}, Cwd: tmpDir},
			},
			expected: map[string]interface{}{
				"type": "section",
				"text": map[string]string{
					"type": "mrkdwn",
					"text": ":rotating_light: *Error running backup operation `bar`!* @channel",
				},
				"fields": []map[string]string{
					{
						"type": "mrkdwn",
						"text": "*Command:*\n```\necho 'foo bar'\n```",
					},
					{
						"type": "mrkdwn",
						"text": fmt.Sprintf("*Working directory:*\n```\n%s\n```", tmpDir),
					},
					{
						"type": "mrkdwn",
						"text": "*Error:*\n```\ntest error\n```",
					},
					{
						"type": "mrkdwn",
						"text": "*Log lines (written to stderr):*\n```\ntest log 1\ntest log 2\n```",
					},
				},
			},
		},
	}
	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			actual := new(SlackNotifier).Format(tc.input)
			if !reflect.DeepEqual(actual, tc.expected) {
				t.Errorf("expected %v, got %v", tc.expected, actual)
			}
		})
	}
}

func TestSlackNotify(t *testing.T) {
	results := []OperationResult{
		{Status: StatusSkipped},
		{Status: StatusSuccess, Operation: &Operation{Name: "foo"}},
	}

	requests := []*http.Request{}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests = append(requests, r)

		w.WriteHeader(http.StatusOK)
		if _, err := w.Write([]byte(`{"ok": true}`)); err != nil {
			t.Fatal(err)
		}
	}))
	defer ts.Close()

	notifier := NewSlackNotifier(ts.URL)

	if err := notifier.Notify(results...); err != nil {
		t.Errorf("expected no error, got %v", err)
	}
	if len(requests) != 1 {
		t.Errorf("expected 1 request, got %d", len(requests))
	}
	for _, req := range requests {
		if req.Method != "POST" {
			t.Errorf("expected POST request, got %s", req.Method)
		}
		if req.Header["Content-Type"][0] != "application/json" {
			t.Errorf("expected Content-Type: application/json, got %s", req.Header["Content-Type"][0])
		}
	}
}

func TestSlackNotifyError(t *testing.T) {
	results := []OperationResult{
		{Status: StatusSkipped},
		{Status: StatusSuccess, Operation: &Operation{Name: "foo"}},
	}

	mutex := &sync.Mutex{}
	requests := []*http.Request{}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mutex.Lock()
		defer mutex.Unlock()
		requests = append(requests, r)

		switch r.URL.Path {
		case "/foo":
			w.WriteHeader(http.StatusOK)
			if _, err := w.Write([]byte(`{"ok": true}`)); err != nil {
				t.Fatal(err)
			}
		case "/bar":
			w.WriteHeader(http.StatusBadRequest)
			if _, err := w.Write([]byte(`{"ok": false}`)); err != nil {
				t.Fatal(err)
			}
		}
	}))
	defer ts.Close()

	notifier := NewSlackNotifier(ts.URL+"/foo", ts.URL+"/bar", "wrong-protocol"+ts.URL)

	if err := notifier.Notify(results...); err == nil {
		t.Errorf("expected no error, got %v", err)
	} else if merr, ok := err.(*multierror.Error); !ok {
		t.Errorf("expected multierror, got %T", err)
	} else if len(merr.Errors) != 2 {
		t.Errorf("expected 2 errors, got %d", len(merr.Errors))
	}
	if len(requests) != 2 {
		t.Errorf("expected 1 request, got %d", len(requests))
	}
	for _, req := range requests {
		if req.Method != "POST" {
			t.Errorf("expected POST request, got %s", req.Method)
		}
		if req.Header["Content-Type"][0] != "application/json" {
			t.Errorf("expected Content-Type: application/json, got %s", req.Header["Content-Type"][0])
		}
	}
}