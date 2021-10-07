package backup

import (
	"flag"
	"strings"

	"github.com/BurntSushi/toml"
)

type listOfStrings []string

func (list *listOfStrings) Set(value string) error {
	*list = append(*list, value)

	return nil
}

func (list *listOfStrings) String() string {
	return strings.Join(*list, ",")
}

func MustLoadConfiguration() (*Notifications, *Operations) {
	var SlackWebhooks listOfStrings
	flag.Var(&SlackWebhooks, "slack-webhook", "Slack webhook URL (can be specified multiple times).")
	ConfigurationFile := flag.String("config", "", "Path to configuration file (TOML).")
	flag.Parse()

	Notifications := NewNotifications(SlackWebhooks)

	if *ConfigurationFile == "" {
		panic("No configuration file specified.")
	}
	Configuration, err := parse(*ConfigurationFile)
	if err != nil {
		panic(err)
	}

	return Notifications, Configuration
}

func parse(path string) (*Operations, error) {
	var config Operations
	_, err := toml.DecodeFile(path, &config)

	return &config, err
}
