package backup

import (
	"sync"
	"time"
)

type TasksList []TaskInterface

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
