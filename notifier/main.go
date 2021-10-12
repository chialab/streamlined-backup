package notifier

import "github.com/chialab/streamlined-backup/backup"

type Notifier interface {
	Notify(...backup.OperationResult) error
	Error(interface{}) error
}
