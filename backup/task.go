package backup

import (
	"fmt"
	"io"
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

const DEFAULT_TIMEOUT = time.Minute * 10

type Task struct {
	name         string
	command      []string
	cwd          string
	env          []string
	timeout      time.Duration
	destinations handler.Destinations
	logger       *log.Logger
}

func NewTask(name string, def config.Task) (*Task, error) {
	logger := log.New(os.Stderr, fmt.Sprintf("[%s] ", name), log.LstdFlags|log.Lmsgprefix)

	if len(def.Destinations) == 0 {
		return nil, handler.ErrUnknownDestination
	}

	destinations := make(handler.Destinations, len(def.Destinations))
	for i, dest := range def.Destinations {
		if destination, err := handler.NewDestination(dest); err != nil {
			return nil, err
		} else {
			destinations[i] = destination
		}
	}

	var timeout time.Duration
	var err error
	if def.Timeout != "" {
		timeout, err = time.ParseDuration(def.Timeout)
		if err != nil {
			return nil, err
		}
	}

	return &Task{
		name:         name,
		command:      def.Command,
		cwd:          def.Cwd,
		env:          def.Env,
		timeout:      timeout,
		destinations: destinations,
		logger:       logger,
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

func (t Task) Timeout() time.Duration {
	if t.timeout == 0 {
		return DEFAULT_TIMEOUT
	}

	return t.timeout
}

func (t Task) Run(now time.Time) Result {
	if handlers, err := t.destinations.GetHandlers(now); err != nil {
		t.logger.Printf("ERROR (Could not find last run): %s", err)

		return NewResultFailed(&t, err, []string{})
	} else if len(handlers) == 0 {
		t.logger.Print("SKIPPED")

		return NewResultSkipped(&t)
	} else {
		return t.runner(now, handlers)
	}
}

func (t Task) runner(now time.Time, handlers []handler.Handler) (result Result) {
	result = Result{task: &t}

	logsWriter := utils.NewLogWriter(t.logger)
	defer func() {
		logsWriter.Close()
		result.logs = logsWriter.Lines()
	}()

	reader, writer := io.Pipe()
	wait, initErr := handlers[0].Handler(reader, now)
	if initErr != nil {
		t.logger.Printf("ERROR (Initialization failed): %s", initErr)
		result.status = StatusFailed
		result.err = NewTaskError(HandlerError, "handler could not be initialized: %s", initErr)

		return
	}
	defer func() {
		if panicked := recover(); panicked != nil {
			panicErr := utils.ToError(panicked)

			result.status = StatusFailed
			if IsTaskError(panicErr, CommandTimeoutError) {
				result.status = StatusTimeout
			}

			result.err = panicErr
			if writer != nil {
				if err := writer.CloseWithError(panicErr); err != nil {
					t.logger.Printf("ERROR (Abort failed): %s", err)
					result.err = multierror.Append(result.err, err)
				}
				if err := wait(); err != nil {
					t.logger.Printf("ERROR (Upload abort failed): %s", err)
					result.err = multierror.Append(result.err, NewTaskError(HandlerError, "handler could not abort artifact upload: %s", err))
				}
			}
		}
	}()

	if err := t.execCommand(writer, logsWriter); err != nil {
		panic(err)
	}

	writer.Close()
	writer = nil

	if err := wait(); err != nil {
		t.logger.Printf("ERROR (Upload failed): %s", err)
		panic(NewTaskError(HandlerError, "handler could not complete artifact upload: %s", err))
	}

	t.logger.Print("DONE")
	result.status = StatusSuccess

	return
}

func (t Task) execCommand(stdout io.Writer, stderr io.Writer) error {
	cmd := exec.Command(t.command[0], t.command[1:]...)
	cmd.Dir = t.cwd
	cmd.Env = t.env
	cmd.Stdout = stdout
	cmd.Stderr = stderr

	if err := cmd.Start(); err != nil {
		t.logger.Printf("ERROR (Command start): %s", err)

		return NewTaskError(CommandStartError, "command could not be started: %s", err)
	}

	res := make(chan error)
	go func() {
		defer close(res)
		res <- cmd.Wait()
	}()

	select {
	case err := <-res:
		if err == nil {
			return nil
		}

		t.logger.Printf("ERROR (Command failed): %s", err)

		return NewTaskError(CommandFailedError, "command failed: %s", err)
	case <-time.After(t.Timeout()):
		t.logger.Printf("TIMEOUT (Command took more than %s)", t.Timeout())
		var err error
		err = NewTaskError(CommandTimeoutError, fmt.Sprintf("command timed out after %s", t.Timeout()), nil)
		if killErr := cmd.Process.Kill(); killErr != nil {
			t.logger.Printf("ERROR (Command kill): %s", killErr)
			err = multierror.Append(err, NewTaskError(CommandKillError, "command could not be killed: %s", killErr))
		} else if waitErr := <-res; waitErr != nil {
			err = multierror.Append(err, waitErr)
		}

		return err
	}
}
