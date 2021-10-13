package notifier

import (
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync"
	"testing"

	"github.com/chialab/streamlined-backup/backup"
	"github.com/hashicorp/go-multierror"
)

func TestSlackFormat(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	testCases := map[string]struct {
		input    *backup.Result
		expected map[string]interface{}
	}{
		"skipped": {
			input:    &backup.Result{Status: backup.StatusSkipped},
			expected: nil,
		},
		"success": {
			input: &backup.Result{Status: backup.StatusSuccess, Task: &backup.Task{Name: "foo"}},
			expected: map[string]interface{}{
				"type": "section",
				"text": map[string]string{
					"type": "mrkdwn",
					"text": ":white_check_mark: Backup task `foo` completed successfully.",
				},
			},
		},
		"failure": {
			input: &backup.Result{
				Status: backup.StatusFailure,
				Error:  fmt.Errorf("test error"),
				Logs:   []string{"test log 1", "test log 2", ""},
				Task:   &backup.Task{Name: "bar", Command: []string{"echo", "foo bar"}, Cwd: tmpDir},
			},
			expected: map[string]interface{}{
				"type": "section",
				"text": map[string]string{
					"type": "mrkdwn",
					"text": ":rotating_light: *Error running backup task `bar`!* @channel",
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
	t.Parallel()

	results := []backup.Result{
		{Status: backup.StatusSkipped},
		{Status: backup.StatusSuccess, Task: &backup.Task{Name: "foo"}},
	}
	expectedBody := fmt.Sprintf(
		`{"blocks":[{"text":{"text":":white_check_mark: Backup task %s completed successfully.","type":"mrkdwn"},"type":"section"}]}`+"\n",
		"`foo`",
	)

	requests := []struct {
		method      string
		contentType string
		body        string
	}{}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if body, err := ioutil.ReadAll(r.Body); err != nil {
			panic(err)
		} else {
			requests = append(requests, struct {
				method      string
				contentType string
				body        string
			}{
				method:      r.Method,
				contentType: r.Header.Get("Content-Type"),
				body:        string(body),
			})
		}

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
		if req.method != "POST" {
			t.Errorf("expected POST request, got %s", req.method)
		}
		if req.contentType != "application/json" {
			t.Errorf("expected Content-Type: application/json, got %s", req.contentType)
		}
		if req.body != expectedBody {
			t.Errorf("expected body %s, got %s", expectedBody, req.body)
		}
	}
}

func TestSlackNotifyEmpty(t *testing.T) {
	t.Parallel()

	results := []backup.Result{
		{Status: backup.StatusSkipped},
		{Status: backup.StatusSkipped},
	}

	requests := []struct {
		method      string
		contentType string
		body        string
	}{}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if body, err := ioutil.ReadAll(r.Body); err != nil {
			panic(err)
		} else {
			requests = append(requests, struct {
				method      string
				contentType string
				body        string
			}{
				method:      r.Method,
				contentType: r.Header.Get("Content-Type"),
				body:        string(body),
			})
		}

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
	if len(requests) != 0 {
		t.Errorf("expected 0 requests, got %d", len(requests))
	}
}

func TestSlackNotifyError(t *testing.T) {
	t.Parallel()

	results := []backup.Result{
		{Status: backup.StatusSkipped},
		{Status: backup.StatusSuccess, Task: &backup.Task{Name: "foo"}},
	}
	expectedBody := fmt.Sprintf(
		`{"blocks":[{"text":{"text":":white_check_mark: Backup task %s completed successfully.","type":"mrkdwn"},"type":"section"}]}`+"\n",
		"`foo`",
	)

	mutex := &sync.Mutex{}
	requests := []struct {
		method      string
		contentType string
		body        string
	}{}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mutex.Lock()
		defer mutex.Unlock()

		if body, err := ioutil.ReadAll(r.Body); err != nil {
			panic(err)
		} else {
			requests = append(requests, struct {
				method      string
				contentType string
				body        string
			}{
				method:      r.Method,
				contentType: r.Header.Get("Content-Type"),
				body:        string(body),
			})
		}

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
		if req.method != "POST" {
			t.Errorf("expected POST request, got %s", req.method)
		}
		if req.contentType != "application/json" {
			t.Errorf("expected Content-Type: application/json, got %s", req.contentType)
		}
		if req.body != expectedBody {
			t.Errorf("expected body %s, got %s", expectedBody, req.body)
		}
	}
}

func TestSlackError(t *testing.T) {
	t.Parallel()

	err := errors.New("test error")
	expectedBody := fmt.Sprintf(
		`{"blocks":[{"text":{"text":":rotating_light: *Error running backup task!* @channel\n%s","type":"mrkdwn"},"type":"section"}]}`+"\n",
		"```\\ntest error\\n```",
	)

	requests := []struct {
		method      string
		contentType string
		body        string
	}{}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if body, err := ioutil.ReadAll(r.Body); err != nil {
			panic(err)
		} else {
			requests = append(requests, struct {
				method      string
				contentType string
				body        string
			}{
				method:      r.Method,
				contentType: r.Header.Get("Content-Type"),
				body:        string(body),
			})
		}

		w.WriteHeader(http.StatusOK)
		if _, err := w.Write([]byte(`{"ok": true}`)); err != nil {
			t.Fatal(err)
		}
	}))
	defer ts.Close()

	notifier := NewSlackNotifier(ts.URL)

	if err := notifier.Error(err); err != nil {
		t.Errorf("expected no error, got %v", err)
	}
	if len(requests) != 1 {
		t.Errorf("expected 1 request, got %d", len(requests))
	}
	for _, req := range requests {
		if req.method != "POST" {
			t.Errorf("expected POST request, got %s", req.method)
		}
		if req.contentType != "application/json" {
			t.Errorf("expected Content-Type: application/json, got %s", req.contentType)
		}
		if req.body != expectedBody {
			t.Errorf("expected body %s, got %s", expectedBody, req.body)
		}
	}
}
