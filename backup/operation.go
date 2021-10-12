package backup

import (
	"log"
	"os/exec"
	"time"

	"github.com/hashicorp/go-multierror"
)

type Operation struct {
	Name     string
	Schedule ScheduleExpression
	Command  []string
	Cwd      string   `toml:"cwd,omitempty"`
	Env      []string `toml:"env,omitempty"`
	handler  Handler
	logger   *log.Logger
}

type OperationInterface interface {
	Run(now time.Time) (result OperationResult)
}

func (o Operation) ShouldRun(now time.Time) (bool, error) {
	lastRun, err := o.handler.LastRun()
	if err != nil {
		return false, err
	}

	if lastRun.IsZero() {
		return true, nil
	}

	return o.Schedule.Next(lastRun).Before(now), nil
}

func (o Operation) Run(now time.Time) (result OperationResult) {
	result = OperationResult{Operation: &o}
	if run, err := o.ShouldRun(now); err != nil {
		o.logger.Printf("ERROR (Could not find last run): %s", err)
		result.Status = StatusFailure
		result.Error = err

		return
	} else if !run {
		o.logger.Print("SKIPPED")
		result.Status = StatusSkipped

		return
	}

	logsWriter := &LogWriter{logger: o.logger}
	defer logsWriter.Close()

	writer := NewChunkWriter(CHUNK_SIZE)
	wait, initErr := o.handler.Handler(writer.Chunks, now)
	if initErr != nil {
		o.logger.Printf("ERROR (Initialization failed): %s", initErr)
		result.Status = StatusFailure
		result.Error = initErr

		return
	}
	defer func() {
		if panicked := recover(); panicked != nil {
			panicErr := ToError(panicked)

			result.Status = StatusFailure
			result.Error = multierror.Append(result.Error, panicErr)
			if writer != nil {
				writer.Abort(panicErr)
				if err := wait(); err != nil {
					o.logger.Printf("ERROR (Upload abort failed): %s", err)
					result.Error = multierror.Append(result.Error, err)
				}
			}
		}
	}()

	cmd := exec.Command(o.Command[0], o.Command[1:]...)
	cmd.Dir = o.Cwd
	cmd.Env = o.Env
	cmd.Stdout = writer
	cmd.Stderr = logsWriter

	if err := cmd.Start(); err != nil {
		o.logger.Printf("ERROR (Command start): %s", err)
		panic(err)
	}

	if err := cmd.Wait(); err != nil {
		o.logger.Printf("ERROR (Command failed): %s", err)
		panic(err)
	}

	writer.Close()
	writer = nil

	if err := wait(); err != nil {
		o.logger.Printf("ERROR (Upload failed): %s", err)
		panic(err)
	}

	o.logger.Print("DONE")
	result.Status = StatusSuccess

	return
}
