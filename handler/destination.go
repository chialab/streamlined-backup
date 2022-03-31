package handler

import (
	"sync"
	"time"

	"github.com/chialab/streamlined-backup/config"
	"github.com/chialab/streamlined-backup/utils"
	"github.com/hashicorp/go-multierror"
)

func NewDestination(destination config.Destination) (Destination, error) {
	if handler, err := NewHandler(destination); err != nil {
		return Destination{}, err
	} else {
		return Destination{
			schedule: destination.Schedule,
			handler:  handler,
		}, nil
	}
}

type Destination struct {
	schedule utils.ScheduleExpression
	handler  Handler
}

func (d *Destination) SetSchedule(schedule utils.ScheduleExpression) *Destination {
	d.schedule = schedule

	return d
}

func (d *Destination) SetHandler(handler Handler) *Destination {
	d.handler = handler

	return d
}

func (d Destination) ShouldRun(now time.Time) (bool, error) {
	lastRun, err := d.handler.LastRun()
	if err != nil {
		return false, err
	}

	if lastRun.IsZero() {
		return true, nil
	}

	return d.schedule.Next(lastRun).Before(now), nil
}

type Destinations []Destination

func (d Destinations) GetHandlers(now time.Time) ([]Handler, error) {
	var merr *multierror.Error
	filtered := []Handler{}
	mutex := &sync.Mutex{}
	wg := &sync.WaitGroup{}

	for _, dest := range d {
		wg.Add(1)
		go func(dest Destination) {
			defer wg.Done()
			shouldRun, err := dest.ShouldRun(now)

			mutex.Lock()
			defer mutex.Unlock()
			if err != nil {
				merr = multierror.Append(merr, err)
			} else if shouldRun {
				filtered = append(filtered, dest.handler)
			}
		}(dest)
	}

	wg.Wait()
	if merr != nil {
		return nil, merr
	}

	return filtered, nil
}
