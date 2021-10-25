package main

import (
	"flag"
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
func (list *listOfStrings) String() string {
	return strings.Join(*list, ",")
}

func main() {
	var slackWebhooks listOfStrings
	flag.Var(&slackWebhooks, "slack-webhook", "Slack webhook URL (can be specified multiple times).")
	cfgFilePath := flag.String("config", "", "Path to configuration file (TOML/JSON).")
	pidFilePath := flag.String("pid", "/var/run/streamlined-backup.pid", "Path to PID file.")
	parallel := flag.Uint("parallel", PARALLEL_TASKS, "Number of tasks to run in parallel.")
	flag.Parse()

	slack := notifier.NewSlackNotifier(slackWebhooks...)
	defer func() {
		if panicked := recover(); panicked != nil {
			err := utils.ToError(panicked)
			if notifyErr := slack.Error(err); notifyErr != nil {
				panic(multierror.Append(err, notifyErr))
			}

			panic(err)
		}
	}()

	tasksDfn, err := config.LoadConfiguration(*cfgFilePath)
	if err != nil {
		panic(err)
	}

	tasks, err := backup.NewTasksList(tasksDfn)
	if err != nil {
		panic(err)
	}

	pid := utils.NewPidFile(*pidFilePath)
	if err := pid.Acquire(); err != nil {
		panic(err)
	}
	defer pid.MustRelease()

	now := time.Now()
	results := tasks.Run(now, *parallel)
	sort.Sort(results)
	if err := slack.Notify(results...); err != nil {
		panic(err)
	}
}
