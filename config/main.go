package config

import (
	"encoding/json"
	"errors"
	"os"
	"strings"

	"github.com/BurntSushi/toml"
	"github.com/chialab/streamlined-backup/utils"
)

var ErrUnsupportedConfigFile = errors.New("unsupported config file")

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
	switch {
	case strings.HasSuffix(path, ".toml"):
		if _, err := toml.DecodeFile(path, &config); err != nil {
			return nil, err
		}
		return config, nil
	case strings.HasSuffix(path, ".json"):
		if data, err := os.ReadFile(path); err != nil {
			return nil, err
		} else if err := json.Unmarshal(data, &config); err != nil {
			return nil, err
		}

		return config, nil
	}

	return nil, ErrUnsupportedConfigFile
}
