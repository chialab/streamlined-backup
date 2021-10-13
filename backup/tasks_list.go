package backup

import (
	"sync"
	"time"

	"github.com/chialab/streamlined-backup/config"
)

type TasksList []TaskInterface

func NewTasksList(tasks map[string]config.Task) (TasksList, error) {
	list := TasksList{}
	for name, taskDfn := range tasks {
		if task, err := NewTask(name, taskDfn); err != nil {
			return nil, err
		} else {
			list = append(list, task)
		}
	}

	return list, nil
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
