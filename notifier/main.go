package notifier

import "github.com/chialab/streamlined-backup/backup"

type Notifier interface {
	Notify(...backup.Result) error
	Error(interface{}) error
}
