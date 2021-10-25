package main

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path"
	"reflect"
	"strconv"
	"testing"

	"github.com/chialab/streamlined-backup/backup"
	"github.com/chialab/streamlined-backup/config"
	"github.com/chialab/streamlined-backup/handler"
	"github.com/hashicorp/go-multierror"
)

func TestListOfStrings(t *testing.T) {
	t.Parallel()

	l := listOfStrings{"foo", "bar"}
	if err := l.Set("baz"); err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if len(l) != 3 {
		t.Errorf("Unexpected length: %d", len(l))
	} else if l[0] != "foo" || l[1] != "bar" || l[2] != "baz" {
		t.Errorf("Unexpected list: %v", l)
	}
	if l.String() != "foo,bar,baz" {
		t.Errorf("Unexpected list: %v", l)
	}
}

func TestParseOptions(t *testing.T) {
	t.Parallel()

	args := []string{"-parallel=42", "-config=foo.json", "-slack-webhook=http://example.org", "-slack-webhook=http://example.com", "-pid-file=pid.txt"}
	if opts, err := parseOptions("foo", args); err != nil {
		t.Errorf("unexpected error: %#v", err)
	} else if *opts.parallel != 42 {
		t.Errorf("expected 42, got %#v", *opts.parallel)
	} else if *opts.config != "foo.json" {
		t.Errorf("expected foo.json, got %#v", *opts.config)
	} else if expected := (&listOfStrings{"http://example.org", "http://example.com"}); !reflect.DeepEqual(opts.slackWebhooks, expected) {
		t.Errorf("expected %#v, got %#v", expected, opts.slackWebhooks)
	} else if *opts.pidFile != "pid.txt" {
		t.Errorf("expected pid.txt, got %#v", *opts.pidFile)
	}
}

func TestParseOptionsErr(t *testing.T) {
	t.Parallel()

	args := []string{"-parallel=foo", "-config=foo.json", "-slack-webhook=http://example.org", "-slack-webhook=http://example.com", "-pid-file=pid.txt"}
	if _, err := parseOptions("foo", args); err == nil {
		t.Errorf("expected error, got nil")
	} else if err.Error() != `invalid value "foo" for flag -parallel: parse error` {
		t.Errorf("expected error, got %#v", err)
	}
}

func TestWithNotifier(t *testing.T) {
	t.Parallel()

	requests := 0
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++

		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	opts := &cliOptions{
		slackWebhooks: &listOfStrings{ts.URL},
	}

	invocations := 0
	withNotifier(opts, func(o *cliOptions) backup.Results {
		invocations++

		return backup.Results{backup.NewResultSuccess(&backup.Task{}, []string{})}
	})

	if invocations != 1 {
		t.Errorf("expected callback to be invoked once, got %d", invocations)
	}
	if requests != 1 {
		t.Errorf("expected Slack webhook to be called once, got %d", requests)
	}
}

func TestWithNotifierCallbackPanic(t *testing.T) {
	t.Parallel()

	requests := 0
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++

		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	opts := &cliOptions{
		slackWebhooks: &listOfStrings{ts.URL},
	}

	invocations := 0
	defer func() {
		if invocations != 1 {
			t.Errorf("expected callback to be invoked once, got %d", invocations)
		}
		if requests != 1 {
			t.Errorf("expected Slack webhook to be called once, got %d", requests)
		}
		if panicked := recover(); panicked == nil {
			t.Errorf("expected panic, got nil")
		} else if expected := errors.New("test error"); !reflect.DeepEqual(expected, panicked) {
			t.Errorf("expected %#v, got %#v", expected, panicked)
		}
	}()

	withNotifier(opts, func(o *cliOptions) backup.Results {
		invocations++

		panic("test error")
	})
	t.Fatal("expected panic")
}

func TestWithNotifierNotifyPanic(t *testing.T) {
	t.Parallel()

	requests := 0
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++

		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer ts.Close()

	opts := &cliOptions{
		slackWebhooks: &listOfStrings{ts.URL},
	}

	invocations := 0
	defer func() {
		if invocations != 1 {
			t.Errorf("expected callback to be invoked once, got %d", invocations)
		}
		if requests != 2 {
			t.Errorf("expected Slack webhook to be called twice, got %d", requests)
		}

		expectedErr := fmt.Errorf("error sending notification to %s: %d %s", ts.URL, http.StatusUnauthorized, http.StatusText(http.StatusUnauthorized))
		if panicked := recover(); panicked == nil {
			t.Errorf("expected panic, got nil")
		} else if merr, ok := panicked.(*multierror.Error); !ok {
			t.Errorf("expected *multierror.Error, got %#v", panicked)
		} else if merr.Errors[0].Error() != expectedErr.Error() {
			t.Errorf("expected %#v, got %#v", expectedErr, merr.Errors[0])
		} else if merr.Errors[1].Error() != expectedErr.Error() {
			t.Errorf("expected %#v, got %#v", expectedErr, merr.Errors[1])
		}
	}()

	withNotifier(opts, func(o *cliOptions) backup.Results {
		invocations++

		return backup.Results{backup.NewResultSuccess(&backup.Task{}, []string{})}
	})
	t.Fatal("expected panic")
}

func TestRunInvalidConfigFile(t *testing.T) {
	t.Parallel()

	configFile := "foo.xml"
	opts := &cliOptions{config: &configFile}

	defer func() {
		if panicked := recover(); panicked == nil {
			t.Errorf("expected panic, got nil")
		} else if panicked != config.ErrUnsupportedConfigFile {
			t.Errorf("expected %#v, got %#v", config.ErrUnsupportedConfigFile, panicked)
		}
	}()

	run(opts)
	t.Fatal("expected panic")
}

func TestRunInvalidConfig(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	configFile := path.Join(tmpDir, "foo.json")
	data := `{"foo": {"timeout": "5s", "destination": {"type": "unknown"}}}`
	if err := os.WriteFile(configFile, []byte(data), 0644); err != nil {
		t.Fatalf("unepected error: %s", err)
	}

	opts := &cliOptions{config: &configFile}

	defer func() {
		if panicked := recover(); panicked == nil {
			t.Errorf("expected panic, got nil")
		} else if panicked != handler.ErrUnknownDestination {
			t.Errorf("expected %#v, got %#v", handler.ErrUnknownDestination, panicked)
		}
	}()

	run(opts)
	t.Fatal("expected panic")
}

func TestRunPidRunning(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	configFile := path.Join(tmpDir, "foo.json")
	data := `{"foo": {"destination": {"type": "s3"}}}`
	if err := os.WriteFile(configFile, []byte(data), 0644); err != nil {
		t.Fatalf("unepected error: %s", err)
	}

	pidFile := path.Join(tmpDir, "foo.pid")
	cmd := exec.Command("tail", "-f", "/dev/null")
	if err := cmd.Start(); err != nil {
		t.Fatalf("unexpected error: %#v", err)
	}
	if err := os.WriteFile(pidFile, []byte(strconv.Itoa(cmd.Process.Pid)), 0644); err != nil {
		t.Fatalf("unexpected error: %#v", err)
	}
	defer func() {
		if err := cmd.Process.Signal(os.Kill); err != nil {
			t.Fatalf("unexpected error: %#v", err)
		}
		var exitErr *exec.ExitError
		if err := cmd.Wait(); err != nil && !errors.As(err, &exitErr) {
			t.Fatalf("unexpected error: %#v", err)
		}
	}()

	opts := &cliOptions{config: &configFile, pidFile: &pidFile}

	results := run(opts)
	if len(results) != 0 {
		t.Errorf("expected 0 results, got %d", len(results))
	}
}

func TestRun(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	configFile := path.Join(tmpDir, "foo.json")
	if err := os.WriteFile(configFile, []byte(`{}`), 0644); err != nil {
		t.Fatalf("unepected error: %s", err)
	}

	pidFile := path.Join(tmpDir, "foo.pid")

	parallel := uint(1)
	opts := &cliOptions{config: &configFile, pidFile: &pidFile, parallel: &parallel}

	results := run(opts)
	if len(results) != 0 {
		t.Errorf("expected 0 results, got %d", len(results))
	}
}
