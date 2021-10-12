package backup

import (
	"fmt"
	"log"
	"os"

	"github.com/BurntSushi/toml"
)

type operationConfig struct {
	Schedule    ScheduleExpression
	Command     []string
	Cwd         string   `toml:"cwd,omitempty"`
	Env         []string `toml:"env,omitempty"`
	Destination Destination
}

func LoadConfiguration(path string) (*Operations, error) {
	var config map[string]operationConfig
	if _, err := toml.DecodeFile(path, &config); err != nil {
		return nil, err
	}

	operations := Operations{}
	for name, operation := range config {
		handler, err := NewHandler(operation.Destination)
		if err != nil {
			return nil, err
		}

		logger := log.New(os.Stderr, fmt.Sprintf("[%s]", name), log.LstdFlags)
		operations = append(operations, Operation{
			Name:     name,
			Schedule: operation.Schedule,
			Command:  operation.Command,
			Cwd:      operation.Cwd,
			Env:      operation.Env,
			handler:  handler,
			logger:   logger,
		})
	}

	return &operations, nil
}
