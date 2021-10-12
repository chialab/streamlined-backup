package main

import (
	"flag"
	"sort"
	"strings"
	"time"

	"github.com/chialab/streamlined-backup/backup"
	"github.com/chialab/streamlined-backup/notifier"
	"github.com/hashicorp/go-multierror"
)

const PARALLEL_OPERATIONS = 2

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
	config := flag.String("config", "", "Path to configuration file (TOML).")
	parallel := flag.Uint("parallel", PARALLEL_OPERATIONS, "Number of parallel operations.")
	flag.Parse()

	slack := notifier.NewSlackNotifier(slackWebhooks...)
	defer func() {
		if panicked := recover(); panicked != nil {
			err := backup.ToError(panicked)
			if notifyErr := slack.Error(err); notifyErr != nil {
				panic(multierror.Append(err, notifyErr))
			}

			panic(err)
		}
	}()

	Operations, err := backup.LoadConfiguration(*config)
	if err != nil {
		panic(err)
	}

	now := time.Now()
	results := Operations.Run(now, *parallel)
	sort.Sort(results)
	if err := slack.Notify(results...); err != nil {
		panic(err)
	}
}
