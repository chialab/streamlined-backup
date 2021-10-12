package backup

import (
	"sync"
	"time"
)

type Operations []OperationInterface

func (o Operations) Run(now time.Time, parallel uint) OperationResults {
	pool := make(chan bool, parallel)
	results := OperationResults{}
	mutex := &sync.Mutex{}
	for _, op := range o {
		pool <- true
		go func(op OperationInterface) {
			defer func() { <-pool }()

			result := op.Run(now)

			mutex.Lock()
			defer mutex.Unlock()
			results = append(results, result)
		}(op)
	}
	for i := 0; i < cap(pool); i++ {
		pool <- true
	}

	return results
}
