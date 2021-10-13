package backup

import (
	"log"
	"os/exec"
	"time"

	"github.com/chialab/streamlined-backup/handler"
	"github.com/chialab/streamlined-backup/utils"
	"github.com/hashicorp/go-multierror"
)

const CHUNK_SIZE = 10 << 20

type Task struct {
	Name     string
	Schedule utils.ScheduleExpression
	Command  []string
	Cwd      string   `toml:"cwd,omitempty"`
	Env      []string `toml:"env,omitempty"`
	handler  handler.Handler
	logger   *log.Logger
}

type TaskInterface interface {
	Run(now time.Time) (result Result)
}

func (t Task) ShouldRun(now time.Time) (bool, error) {
	lastRun, err := t.handler.LastRun()
	if err != nil {
		return false, err
	}

	if lastRun.IsZero() {
		return true, nil
	}

	return t.Schedule.Next(lastRun).Before(now), nil
}

func (t Task) Run(now time.Time) (result Result) {
	result = Result{Task: &t}
	if run, err := t.ShouldRun(now); err != nil {
		t.logger.Printf("ERROR (Could not find last run): %s", err)
		result.Status = StatusFailure
		result.Error = err

		return
	} else if !run {
		t.logger.Print("SKIPPED")
		result.Status = StatusSkipped

		return
	}

	logsWriter := utils.NewLogWriter(t.logger)
	defer func() {
		logsWriter.Close()
		result.Logs = logsWriter.Lines()
	}()

	writer := utils.NewChunkWriter(CHUNK_SIZE)
	wait, initErr := t.handler.Handler(writer.Chunks, now)
	if initErr != nil {
		t.logger.Printf("ERROR (Initialization failed): %s", initErr)
		result.Status = StatusFailure
		result.Error = initErr

		return
	}
	defer func() {
		if panicked := recover(); panicked != nil {
			panicErr := utils.ToError(panicked)

			result.Status = StatusFailure
			result.Error = multierror.Append(result.Error, panicErr)
			if writer != nil {
				writer.Abort(panicErr)
				if err := wait(); err != nil {
					t.logger.Printf("ERROR (Upload abort failed): %s", err)
					result.Error = multierror.Append(result.Error, err)
				}
			}
		}
	}()

	cmd := exec.Command(t.Command[0], t.Command[1:]...)
	cmd.Dir = t.Cwd
	cmd.Env = t.Env
	cmd.Stdout = writer
	cmd.Stderr = logsWriter

	if err := cmd.Start(); err != nil {
		t.logger.Printf("ERROR (Command start): %s", err)
		panic(err)
	}

	if err := cmd.Wait(); err != nil {
		t.logger.Printf("ERROR (Command failed): %s", err)
		panic(err)
	}

	writer.Close()
	writer = nil

	if err := wait(); err != nil {
		t.logger.Printf("ERROR (Upload failed): %s", err)
		panic(err)
	}

	t.logger.Print("DONE")
	result.Status = StatusSuccess

	return
}
