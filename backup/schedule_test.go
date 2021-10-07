package backup

import (
	"testing"
	"time"
)

func TestScheduleUnmarshal(t *testing.T) {
	t.Parallel()

	ok := map[string]string{
		"every_minute":           "* * * * *",
		"every_hour_minute_42":   "42 * * * *",
		"every_2hours_minute_17": "17 */2 * * *",
		"tuesdays_fridays_noon":  "0 12 * * 2,5",
		"weekly":                 "@weekly",
	}
	for name, expr := range ok {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			s := ScheduleExpression{}
			if err := s.UnmarshalText([]byte(expr)); err != nil {
				t.Fatalf("unexpected error: %s", err)
			}
			if *s.expression != expr {
				t.Fatalf("expected \"%s\", got \"%s\"", expr, s.schedule)
			}
			if s.schedule == nil {
				t.Fatal("expected non-nil schedule")
			}
			if s.String() != expr {
				t.Fatalf("expected \"%s\", got \"%s\"", expr, s.String())
			}
		})
	}
}
func TestScheduleUnmarshalFail(t *testing.T) {
	t.Parallel()

	s := ScheduleExpression{}
	err := s.UnmarshalText([]byte("invalid"))
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if s.expression != nil || s.schedule != nil {
		t.Fatalf("expected nil expression and schedule, got %s and %v", *s.expression, s.schedule)
	}
}

func TestScheduleNext(t *testing.T) {
	t.Parallel()

	type testCase struct {
		expected time.Time
		schedule string
		start    time.Time
	}
	cases := map[string]testCase{
		"weekly": {
			expected: time.Date(2021, time.October, 3, 0, 0, 0, 0, time.Local),
			schedule: "@weekly",
			start:    time.Date(2021, time.October, 1, 0, 0, 0, 0, time.Local),
		},
		"every_15_minutes": {
			expected: time.Date(2021, time.October, 6, 22, 45, 0, 0, time.Local),
			schedule: "*/15 * * * *",
			start:    time.Date(2021, time.October, 6, 22, 33, 50, 0, time.Local),
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			s := ScheduleExpression{}
			if err := s.UnmarshalText([]byte(tc.schedule)); err != nil {
				t.Fatalf("unexpected error: %s", err)
			}
			if next := s.Next(tc.start); !next.Equal(tc.expected) {
				t.Fatalf("expected %s, got %s (%v)", tc.expected, next, s.schedule)
			}
		})
	}
}

func TestScheduleNextNoTime(t *testing.T) {
	t.Parallel()

	s := ScheduleExpression{}
	if err := s.UnmarshalText([]byte("* * * * *")); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	now := time.Now()
	if next := s.Next(time.Time{}); next.Sub(now).Truncate(time.Second).Seconds() > 0 {
		t.Fatalf("expected %s, got %s (%v)", now, next, s.schedule)
	}
}

func TestNewSchedule(t *testing.T) {
	t.Parallel()

	if s, err := NewSchedule("@weekly"); err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if s.String() != "@weekly" {
		t.Fatalf("expected \"@weekly\", got \"%s\"", s.String())
	}
}
