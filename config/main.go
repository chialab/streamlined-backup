package config

import (
	"github.com/BurntSushi/toml"
	"github.com/chialab/streamlined-backup/utils"
)

type Operation struct {
	Schedule    utils.ScheduleExpression
	Command     []string
	Cwd         string
	Env         []string
	Destination Destination
}

func LoadConfiguration(path string) (map[string]Operation, error) {
	var config map[string]Operation
	if _, err := toml.DecodeFile(path, &config); err != nil {
		return nil, err
	}

	return config, nil

	// operations := Operations{}
	// for name, operation := range config {
	// 	handler, err := NewHandler(operation.Destination)
	// 	if err != nil {
	// 		return nil, err
	// 	}

	// 	logger := log.New(os.Stderr, fmt.Sprintf("[%s] ", name), log.LstdFlags|log.Lmsgprefix)
	// 	operations = append(operations, Operation{
	// 		Name:     name,
	// 		Schedule: operation.Schedule,
	// 		Command:  operation.Command,
	// 		Cwd:      operation.Cwd,
	// 		Env:      operation.Env,
	// 		handler:  handler,
	// 		logger:   logger,
	// 	})
	// }

	// return &operations, nil
}
