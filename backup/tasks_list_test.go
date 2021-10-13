package backup

import (
	"errors"
	"math"
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/chialab/streamlined-backup/config"
	"github.com/chialab/streamlined-backup/handler"
)

func newConcurrenceCounter() *concurrenceCounter {
	return &concurrenceCounter{
		mutex: &sync.Mutex{},
		ch:    make(chan bool, math.MaxInt16),
	}
}

type concurrenceCounter struct {
	max   uint
	mutex *sync.Mutex
	ch    chan bool
}

func (c *concurrenceCounter) Start() {
	c.ch <- true
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if current := uint(len(c.ch)); current > c.max {
		c.max = current
	}
}
func (c *concurrenceCounter) Done() {
	<-c.ch
}
func (c concurrenceCounter) Max() uint {
	return c.max
}

type testTask struct {
	result      Result
	delay       time.Duration
	concurrence *concurrenceCounter
}

func (t testTask) Run(now time.Time) Result {
	t.concurrence.Start()
	defer t.concurrence.Done()
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

		names = append(names, task.name)
		switch task.name {
		case "foo":
			if !reflect.DeepEqual(task.command, []string{"echo", "foo bar"}) {
				t.Errorf("expected task command 'echo foo bar', got %v", task.command)
			}
			if !reflect.DeepEqual(task.env, []string{"FOO=bar"}) {
				t.Errorf("expected task env 'FOO=bar', got %v", task.env)
			}
			if _, ok := task.handler.(*handler.S3Handler); !ok {
				t.Errorf("expected S3Handler, got %T", task.handler)
			}
			if task.logger.Prefix() != "[foo] " {
				t.Errorf("expected log prefix '[foo] ', got %s", task.logger.Prefix())
			}
		case "bar":
			if !reflect.DeepEqual(task.command, []string{"echo", "bar foo"}) {
				t.Errorf("expected task command 'echo bar foo', got %v", task.command)
			}
			if !reflect.DeepEqual(task.env, []string{"BAR=foo"}) {
				t.Errorf("expected task env 'BAR=foo', got %v", task.env)
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

	meter := newConcurrenceCounter()
	tasks := TasksList{
		testTask{
			result:      Result{Status: StatusSuccess, Logs: []string{"success"}},
			delay:       time.Millisecond * 10,
			concurrence: meter,
		},
		testTask{
			result:      Result{Status: StatusSkipped, Logs: []string{"skipped"}},
			delay:       time.Millisecond * 10,
			concurrence: meter,
		},
		testTask{
			result:      Result{Status: StatusFailed, Logs: []string{"failed"}},
			delay:       time.Millisecond * 10,
			concurrence: meter,
		},
		testTask{
			result:      Result{Status: StatusSuccess, Logs: []string{"success"}},
			delay:       time.Millisecond * 10,
			concurrence: meter,
		},
		testTask{
			result:      Result{Status: StatusSuccess, Logs: []string{"success"}},
			delay:       time.Millisecond * 10,
			concurrence: meter,
		},
		testTask{
			result:      Result{Status: StatusFailed, Logs: []string{"failed"}},
			delay:       time.Millisecond * 10,
			concurrence: meter,
		},
	}
	results := tasks.Run(time.Now(), 2)
	if max := meter.Max(); max != 2 {
		t.Errorf("expected 2 concurrent tasks, got %d", max)
	}

	count := map[Status]int{}
	for _, result := range results {
		count[result.Status]++
	}
	if count[StatusSuccess] != 3 {
		t.Errorf("expected 3 success tasks, got %d", count[StatusSuccess])
	}
	if count[StatusFailed] != 2 {
		t.Errorf("expected 2 failed tasks, got %d", count[StatusFailed])
	}
	if count[StatusSkipped] != 1 {
		t.Errorf("expected 1 skipped task, got %d", count[StatusSkipped])
	}
}
