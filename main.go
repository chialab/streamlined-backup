package main

import (
	"log"
	"time"

	"github.com/chialab/streamlined-backup/backup"
)

const PARALLEL_OPERATIONS = 2

func main() {
	Notifications, Operations := backup.MustLoadConfiguration()
	now := time.Now()

	pool := make(chan bool, PARALLEL_OPERATIONS)
	successes := make([]backup.OperationResult, 0)
	for name, op := range *Operations {
		pool <- true
		go func(name string, op backup.Operation) {
			defer func() { <-pool }()

			result := run(name, op, now)
			switch result.Status {
			case backup.StatusSuccess:
				successes = append(successes, *result)
			case backup.StatusFailure:
				if err := Notifications.Notify(*result); err != nil {
					log.Printf("[%s] FAIL: %s", name, err)
				}
			}
		}(name, op)
	}
	for i := 0; i < cap(pool); i++ {
		pool <- true
	}

	if len(successes) > 0 {
		if err := Notifications.Notify(successes...); err != nil {
			log.Printf("FAIL: %s", err)
		}
	}
}

func run(name string, op backup.Operation, now time.Time) *backup.OperationResult {
	handler, err := backup.NewHandler(op.Destination, now)
	if err != nil {
		log.Printf("[%s] FAIL: %s", name, err)

		return &backup.OperationResult{Name: name, Error: err, Status: backup.StatusFailure, Operation: &op}
	}

	if shouldRun, err := op.ShouldRun(handler, now); err != nil {
		log.Printf("[%s] FAIL: %s", name, err)

		return &backup.OperationResult{Name: name, Error: err, Status: backup.StatusFailure, Operation: &op}
	} else if !shouldRun {
		log.Printf("[%s] SKIP", name)

		return &backup.OperationResult{Name: name, Status: backup.StatusSkipped, Operation: &op}
	}

	logLines := make([]string, 0)
	err = op.Run(handler, func(msg string) {
		logLines = append(logLines, msg)
	})
	if err != nil {
		for _, line := range logLines {
			log.Printf("[%s] STDERR: %s", name, line)
		}
		log.Printf("[%s] FAIL: %s", name, err)

		return &backup.OperationResult{Name: name, Error: err, Status: backup.StatusFailure, Operation: &op, Logs: logLines}
	}

	log.Printf("[%s] SUCCESS", name)

	return &backup.OperationResult{Name: name, Status: backup.StatusSuccess, Operation: &op, Logs: logLines}
}
