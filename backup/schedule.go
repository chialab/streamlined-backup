package backup

import (
	"time"

	"github.com/robfig/cron/v3"
)

type ScheduleExpression struct {
	schedule   cron.Schedule
	expression *string
}

func (s *ScheduleExpression) String() string {
	if s.expression == nil {
		return ""
	}

	return *s.expression
}

func (s *ScheduleExpression) UnmarshalText(text []byte) error {
	expr := string(text)
	if schedule, err := cron.ParseStandard(expr); err != nil {
		return err
	} else {
		s.expression = &expr
		s.schedule = schedule

		return nil
	}
}

func (s *ScheduleExpression) Next(t time.Time) time.Time {
	if t.IsZero() {
		return time.Now()
	}

	return s.schedule.Next(t)
}

func NewSchedule(expression string) (*ScheduleExpression, error) {
	schedule := &ScheduleExpression{}
	if err := schedule.UnmarshalText([]byte(expression)); err != nil {
		return nil, err
	}

	return schedule, nil
}
