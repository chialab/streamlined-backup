package config

import (
	"github.com/BurntSushi/toml"
	"github.com/chialab/streamlined-backup/utils"
)

type Task struct {
	Schedule    utils.ScheduleExpression `json:"schedule" toml:"schedule"`
	Command     []string                 `json:"command" toml:"command"`
	Cwd         string                   `json:"cwd" toml:"cwd"`
	Env         []string                 `json:"env" toml:"env"`
	Timeout     string                   `json:"timeout" toml:"timeout"`
	Destination Destination              `json:"destination" toml:"destination"`
}

func LoadConfiguration(path string) (map[string]Task, error) {
	var config map[string]Task
	if _, err := toml.DecodeFile(path, &config); err != nil {
		return nil, err
	}

	return config, nil
}
