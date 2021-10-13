package config

import (
	"github.com/BurntSushi/toml"
	"github.com/chialab/streamlined-backup/utils"
)

type Task struct {
	Schedule    utils.ScheduleExpression
	Command     []string
	Cwd         string
	Env         []string
	Timeout     string
	Destination Destination
}

func LoadConfiguration(path string) (map[string]Task, error) {
	var config map[string]Task
	if _, err := toml.DecodeFile(path, &config); err != nil {
		return nil, err
	}

	return config, nil
}
