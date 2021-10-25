package main

import (
	"flag"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/chialab/streamlined-backup/backup"
	"github.com/chialab/streamlined-backup/config"
	"github.com/chialab/streamlined-backup/notifier"
	"github.com/chialab/streamlined-backup/utils"
	"github.com/hashicorp/go-multierror"
)

const PARALLEL_TASKS = 2

type listOfStrings []string

func (list *listOfStrings) Set(value string) error {
	*list = append(*list, value)

	return nil
}
func (list listOfStrings) String() string {
	return strings.Join(list, ",")
}

type cliOptions struct {
	config        *string
	pidFile       *string
	parallel      *uint
	slackWebhooks *listOfStrings
}

func parseOptions(name string, arguments []string) (*cliOptions, error) {
	opts := &cliOptions{slackWebhooks: new(listOfStrings)}
	flags := flag.NewFlagSet(name, flag.ContinueOnError)

	flags.Var(opts.slackWebhooks, "slack-webhook", "Slack webhook URL (can be specified multiple times).")
	opts.config = flags.String("config", "", "Path to configuration file (TOML/JSON).")
	opts.pidFile = flags.String("pid-file", "/var/run/streamlined-backup.pid", "Path to PID file.")
	opts.parallel = flags.Uint("parallel", PARALLEL_TASKS, "Number of tasks to run in parallel.")
	if err := flags.Parse(arguments); err != nil {
		return nil, err
	}

	return opts, nil
}

func withNotifier(opts *cliOptions, callback func(*cliOptions) backup.Results) {
	slack := notifier.NewSlackNotifier(*opts.slackWebhooks...)
	defer func() {
		if panicked := recover(); panicked != nil {
			err := utils.ToError(panicked)
			if notifyErr := slack.Error(err); notifyErr != nil {
				panic(multierror.Append(err, notifyErr))
			}

			panic(err)
		}
	}()

	results := callback(opts)
	if err := slack.Notify(results...); err != nil {
		panic(err)
	}
}

func run(opts *cliOptions) backup.Results {
	tasksDfn, err := config.LoadConfiguration(*opts.config)
	if err != nil {
		panic(err)
	}

	tasks, err := backup.NewTasksList(tasksDfn)
	if err != nil {
		panic(err)
	}

	pid := utils.NewPidFile(*opts.pidFile)
	if err := pid.Acquire(); err == utils.ErrPidFileExists {
		return backup.Results{}
	} else if err != nil {
		panic(err)
	}
	defer pid.MustRelease()

	now := time.Now()
	results := tasks.Run(now, *opts.parallel)
	sort.Sort(results)

	return results
}

func main() {
	if opts, err := parseOptions(os.Args[0], os.Args[1:]); err == flag.ErrHelp {
		os.Exit(0)
	} else if err != nil {
		os.Exit(2)
	} else {
		withNotifier(opts, run)
	}
}
