package backup

import (
	"fmt"
	"os/exec"
	"time"

	"github.com/hashicorp/go-multierror"
)

type Operation struct {
	Schedule    ScheduleExpression
	Command     []string
	Cwd         string   `toml:"cwd,omitempty"`
	Env         []string `toml:"env,omitempty"`
	Destination Destination
}

type OperationStatus string

const (
	StatusSuccess OperationStatus = "success"
	StatusFailure OperationStatus = "failure"
	StatusSkipped OperationStatus = "skipped"
)

type OperationResult struct {
	Name      string
	Status    OperationStatus
	Operation *Operation
	Logs      []string
	Error     error
}

func (o *Operation) ShouldRun(lastRunner LastRunner, now time.Time) (bool, error) {
	lastRun, err := lastRunner.LastRun(o.Destination)
	if err != nil {
		return false, err
	}

	if lastRun.IsZero() {
		return true, nil
	}

	return o.Schedule.Next(lastRun).Before(now), nil
}

func (o *Operation) Run(handler Handler, log logFunction) error {
	logsWriter := NewLogWriter(log)
	writer := NewChunkWriter(CHUNK_SIZE)
	go handler.Handler(writer.Chunks)
	defer func() {
		logsWriter.Close()
	}()

	cmd := exec.Command(o.Command[0], o.Command[1:]...)
	cmd.Dir = o.Cwd
	cmd.Env = o.Env
	cmd.Stdout = writer
	cmd.Stderr = logsWriter

	if err := cmd.Start(); err != nil {
		log(fmt.Sprint(err))
		writer.Abort(err)
		if handlerErr := handler.Wait(); handlerErr != nil {
			return multierror.Append(err, handlerErr)
		}

		return err
	}

	if err := cmd.Wait(); err != nil {
		log(fmt.Sprint(err))
		writer.Abort(err)
		if handlerErr := handler.Wait(); handlerErr != nil {
			return multierror.Append(err, handlerErr)
		}

		return err
	}

	writer.Close()

	if err := handler.Wait(); err != nil {
		log(fmt.Sprint(err))

		return err
	}

	return nil
}
