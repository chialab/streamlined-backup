package backup

import (
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/chialab/streamlined-backup/config"
	"github.com/chialab/streamlined-backup/handler"
)

type TasksList []TaskInterface

func NewTasksList(tasks map[string]config.Task) (*TasksList, error) {
	list := TasksList{}
	for name, taskDfn := range tasks {
		handler, err := handler.NewHandler(taskDfn.Destination)
		if err != nil {
			return nil, err
		}

		logger := log.New(os.Stderr, fmt.Sprintf("[%s] ", name), log.LstdFlags|log.Lmsgprefix)
		list = append(list, Task{
			Name:     name,
			Schedule: taskDfn.Schedule,
			Command:  taskDfn.Command,
			Cwd:      taskDfn.Cwd,
			Env:      taskDfn.Env,
			handler:  handler,
			logger:   logger,
		})
	}

	return &list, nil
}

func (t TasksList) Run(now time.Time, parallel uint) Results {
	pool := make(chan bool, parallel)
	results := Results{}
	mutex := &sync.Mutex{}
	for _, task := range t {
		pool <- true
		go func(task TaskInterface) {
			defer func() { <-pool }()

			result := task.Run(now)

			mutex.Lock()
			defer mutex.Unlock()
			results = append(results, result)
		}(task)
	}
	for i := 0; i < cap(pool); i++ {
		pool <- true
	}

	return results
}
