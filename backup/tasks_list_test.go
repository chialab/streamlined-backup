package backup

import (
	"errors"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/chialab/streamlined-backup/config"
	"github.com/chialab/streamlined-backup/handler"
)

type testTask struct {
	result Result
	delay  time.Duration
}

func (t testTask) Run(now time.Time) Result {
	time.Sleep(t.delay)

	return t.result
}

func TestNewTasksList(t *testing.T) {
	t.Parallel()

	cfg := map[string]config.Task{
		"foo": {
			Command: []string{"echo", "foo bar"},
			Env:     []string{"FOO=bar"},
			Destination: config.Destination{
				Type: "s3",
			},
		},
		"bar": {
			Command: []string{"echo", "bar foo"},
			Env:     []string{"BAR=foo"},
			Destination: config.Destination{
				Type: "s3",
			},
		},
	}

	tasks, err := NewTasksList(cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(tasks) != 2 {
		t.Fatalf("expected 2 tasks, got %d", len(tasks))
	}
	names := []string{}
	for _, taskIf := range tasks {
		task, ok := taskIf.(*Task)
		if !ok {
			t.Fatalf("expected Task, got %T", taskIf)
		}

		names = append(names, task.Name)
		switch task.Name {
		case "foo":
			if !reflect.DeepEqual(task.Command, []string{"echo", "foo bar"}) {
				t.Errorf("expected task command 'echo foo bar', got %v", task.Command)
			}
			if !reflect.DeepEqual(task.Env, []string{"FOO=bar"}) {
				t.Errorf("expected task env 'FOO=bar', got %v", task.Env)
			}
			if _, ok := task.handler.(*handler.S3Handler); !ok {
				t.Errorf("expected S3Handler, got %T", task.handler)
			}
			if task.logger.Prefix() != "[foo] " {
				t.Errorf("expected log prefix '[foo] ', got %s", task.logger.Prefix())
			}
		case "bar":
			if !reflect.DeepEqual(task.Command, []string{"echo", "bar foo"}) {
				t.Errorf("expected task command 'echo bar foo', got %v", task.Command)
			}
			if !reflect.DeepEqual(task.Env, []string{"BAR=foo"}) {
				t.Errorf("expected task env 'BAR=foo', got %v", task.Env)
			}
			if _, ok := task.handler.(*handler.S3Handler); !ok {
				t.Errorf("expected S3Handler, got %T", task.handler)
			}
			if task.logger.Prefix() != "[bar] " {
				t.Errorf("expected log prefix '[bar] ', got %s", task.logger.Prefix())
			}
		}
	}
	sort.Strings(names) // Order isn't relevant

	if !reflect.DeepEqual(names, []string{"bar", "foo"}) {
		t.Errorf("expected tasks names 'bar', 'foo', got %v", names)
	}
}

func TestNewTasksListError(t *testing.T) {
	t.Parallel()

	cfg := map[string]config.Task{
		"foo": {
			Command: []string{"echo", "foo bar"},
			Env:     []string{"FOO=bar"},
			Destination: config.Destination{
				Type: "s3",
			},
		},
		"bar": {
			Command: []string{"echo", "bar foo"},
			Env:     []string{"BAR=foo"},
		},
	}

	if tasks, err := NewTasksList(cfg); err == nil {
		t.Fatalf("expected error, got %v", tasks)
	} else if !errors.Is(err, handler.ErrUnknownDestination) {
		t.Fatalf("expected ErrUnknownDestination, got %v", err)
	}
}

func TestRunTasks(t *testing.T) {
	t.Parallel()

	tasks := TasksList{
		testTask{
			result: Result{Status: StatusSuccess, Logs: []string{"fourth to complete"}},
			delay:  time.Millisecond * 80,
		},
		testTask{
			result: Result{Status: StatusSuccess, Logs: []string{"first to complete"}},
			delay:  time.Millisecond * 30,
		},
		testTask{
			result: Result{Status: StatusSkipped, Logs: []string{"second to complete"}},
			delay:  time.Millisecond * 20,
		},
		testTask{
			result: Result{Status: StatusFailure, Logs: []string{"third to complete"}},
			delay:  time.Millisecond * 10,
		},
		testTask{
			result: Result{Status: StatusFailure, Logs: []string{"sixth to complete"}},
			delay:  time.Millisecond * 50,
		},
		testTask{
			result: Result{Status: StatusSuccess, Logs: []string{"fifth to complete"}},
			delay:  time.Millisecond * 20,
		},
	}
	results := tasks.Run(time.Now(), 2)
	expected := Results{
		Result{Status: StatusSuccess, Logs: []string{"first to complete"}},
		Result{Status: StatusSkipped, Logs: []string{"second to complete"}},
		Result{Status: StatusFailure, Logs: []string{"third to complete"}},
		Result{Status: StatusSuccess, Logs: []string{"fourth to complete"}},
		Result{Status: StatusSuccess, Logs: []string{"fifth to complete"}},
		Result{Status: StatusFailure, Logs: []string{"sixth to complete"}},
	}
	if !reflect.DeepEqual(results, expected) {
		t.Errorf("Expected %v, got %v", expected, results)
	}
}
