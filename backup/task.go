package backup

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	"time"

	"github.com/alessio/shellescape"
	"github.com/chialab/streamlined-backup/config"
	"github.com/chialab/streamlined-backup/handler"
	"github.com/chialab/streamlined-backup/utils"
	"github.com/hashicorp/go-multierror"
)

const CHUNK_SIZE = 10 << 20

const CHUNK_BUFFER = 8

type Task struct {
	name     string
	schedule utils.ScheduleExpression
	command  []string
	cwd      string
	env      []string
	handler  handler.Handler
	logger   *log.Logger
}

func NewTask(name string, def config.Task) (*Task, error) {
	logger := log.New(os.Stderr, fmt.Sprintf("[%s] ", name), log.LstdFlags|log.Lmsgprefix)
	handler, err := handler.NewHandler(def.Destination)
	if err != nil {
		return nil, err
	}

	return &Task{
		name:     name,
		schedule: def.Schedule,
		command:  def.Command,
		cwd:      def.Cwd,
		env:      def.Env,
		handler:  handler,
		logger:   logger,
	}, nil
}

type TaskInterface interface {
	Run(now time.Time) (result Result)
}

func (t Task) Name() string {
	return t.name
}

func (t Task) CommandString() string {
	return shellescape.QuoteCommand(t.command)
}

func (t Task) ActualCwd() string {
	if t.cwd != "" {
		return t.cwd
	}

	if cwd, err := os.Getwd(); err == nil {
		return cwd
	} else {
		return ""
	}
}

func (t Task) shouldRun(now time.Time) (bool, error) {
	lastRun, err := t.handler.LastRun()
	if err != nil {
		return false, err
	}

	if lastRun.IsZero() {
		return true, nil
	}

	return t.schedule.Next(lastRun).Before(now), nil
}

func (t Task) Run(now time.Time) Result {
	if run, err := t.shouldRun(now); err != nil {
		t.logger.Printf("ERROR (Could not find last run): %s", err)

		return Result{
			Task:   &t,
			Status: StatusFailure,
			Error:  err,
		}
	} else if !run {
		t.logger.Print("SKIPPED")

		return Result{
			Task:   &t,
			Status: StatusSkipped,
		}
	}

	return t.runner(now)
}

func (t Task) runner(now time.Time) (result Result) {
	result = Result{Task: &t}

	logsWriter := utils.NewLogWriter(t.logger)
	defer func() {
		logsWriter.Close()
		result.Logs = logsWriter.Lines()
	}()

	writer := utils.NewChunkWriter(CHUNK_SIZE, CHUNK_BUFFER)
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

	cmd := exec.Command(t.command[0], t.command[1:]...)
	cmd.Dir = t.cwd
	cmd.Env = t.env
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
