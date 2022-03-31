package handler

import (
	"errors"
	"io"
	"reflect"
	"testing"
	"time"

	"github.com/chialab/streamlined-backup/utils"
)

type testHandler struct {
	lastRun    time.Time
	lastRunErr error
}

func (r *testHandler) LastRun() (time.Time, error) {
	return r.lastRun, r.lastRunErr
}

func (r testHandler) Handler(*io.PipeReader, time.Time) (func() error, error) {
	panic("unepected call")
}

func TestDestinationShouldRun(t *testing.T) {
	t.Parallel()

	type testCase struct {
		expected bool
		lastRun  time.Time
		schedule string
	}
	cases := map[string]testCase{
		"yes": {
			expected: true,
			lastRun:  time.Date(2021, 10, 3, 19, 10, 38, 0, time.Local),
			schedule: "0 10 * * *",
		},
		"no": {
			expected: false,
			lastRun:  time.Date(2021, 10, 3, 19, 10, 38, 0, time.Local),
			schedule: "@weekly",
		},
		"never_run": {
			expected: true,
			lastRun:  time.Time{},
			schedule: "@weekly",
		},
	}
	now := time.Date(2021, 10, 6, 19, 10, 38, 0, time.Local)
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			var (
				schedule *utils.ScheduleExpression
				err      error
			)
			if schedule, err = utils.NewSchedule(tc.schedule); err != nil {
				t.Fatalf("unexpected error: %s", err)
			}
			handler := &testHandler{lastRun: tc.lastRun}
			destination := &Destination{
				schedule: *schedule,
				handler:  handler,
			}

			if result, err := destination.ShouldRun(now); err != nil {
				t.Errorf("unexpected error: %s", err)
			} else if result != tc.expected {
				t.Errorf("expected %t, got %t", tc.expected, result)
			}
		})
	}
}

func TestDestinationShouldRunError(t *testing.T) {
	t.Parallel()

	var (
		schedule *utils.ScheduleExpression
		err      error
	)
	if schedule, err = utils.NewSchedule("@daily"); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	testErr := errors.New("test error")
	handler := &testHandler{lastRunErr: testErr}
	destination := &Destination{
		schedule: *schedule,
		handler:  handler,
	}

	now := time.Date(2021, 10, 6, 19, 10, 38, 0, time.Local)
	if result, err := destination.ShouldRun(now); result != false {
		t.Errorf("unexpected result: %t", result)
	} else if err != testErr {
		t.Errorf("expected %v, got %v", testErr, err)
	}
}

func TestDestinationsGetHandlers(t *testing.T) {
	t.Parallel()

	config := []struct {
		schedule string
		lastRun  time.Time
	}{
		{schedule: "0 10 * * *", lastRun: time.Date(2021, 10, 3, 19, 10, 38, 0, time.Local)},
		{schedule: "@weekly", lastRun: time.Date(2021, 10, 3, 19, 10, 38, 0, time.Local)},
		{schedule: "@weekly", lastRun: time.Time{}},
	}

	destinations := make(Destinations, 3)
	for i, tc := range config {
		if schedule, err := utils.NewSchedule(tc.schedule); err != nil {
			t.Fatalf("unexpected error: %s", err)
		} else {
			handler := &testHandler{lastRun: tc.lastRun}
			destinations[i] = Destination{
				schedule: *schedule,
				handler:  handler,
			}
		}
	}

	now := time.Date(2021, 10, 6, 19, 10, 38, 0, time.Local)
	if result, err := destinations.GetHandlers(now); err != nil {
		t.Errorf("unexpected error: %s", err)
	} else if len(result) != 2 {
		t.Errorf("expected 2 destinations, got %d", len(result))
	} else if !reflect.DeepEqual(result, []Handler{destinations[0].handler, destinations[2].handler}) && !reflect.DeepEqual(result, []Handler{destinations[2].handler, destinations[0].handler}) {
		t.Errorf("expected %#v, got %#v", []Handler{destinations[0].handler, destinations[2].handler}, result)
	}
}

func TestDestinationsGetHandlersErrors(t *testing.T) {
	t.Parallel()

	config := []struct {
		schedule string
		lastRun  time.Time
	}{
		{schedule: "0 10 * * *", lastRun: time.Date(2021, 10, 3, 19, 10, 38, 0, time.Local)},
		{schedule: "@weekly", lastRun: time.Date(2021, 10, 3, 19, 10, 38, 0, time.Local)},
		{schedule: "@weekly", lastRun: time.Time{}},
	}

	destinations := make(Destinations, 3)
	for i, tc := range config {
		if schedule, err := utils.NewSchedule(tc.schedule); err != nil {
			t.Fatalf("unexpected error: %s", err)
		} else {
			handler := &testHandler{lastRun: tc.lastRun}
			destinations[i] = Destination{
				schedule: *schedule,
				handler:  handler,
			}
		}
	}

	now := time.Date(2021, 10, 6, 19, 10, 38, 0, time.Local)
	if result, err := destinations.GetHandlers(now); err != nil {
		t.Errorf("unexpected error: %s", err)
	} else if len(result) != 2 {
		t.Errorf("expected 2 destinations, got %d", len(result))
	} else if !reflect.DeepEqual(result, []Handler{destinations[0].handler, destinations[2].handler}) && !reflect.DeepEqual(result, []Handler{destinations[2].handler, destinations[0].handler}) {
		t.Errorf("expected %#v, got %#v", []Handler{destinations[0].handler, destinations[2].handler}, result)
	}
}
