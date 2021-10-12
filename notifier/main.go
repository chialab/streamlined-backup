package notifier

import (
	"bytes"
	"encoding/json"

	"github.com/chialab/streamlined-backup/backup"
)

type Notifier interface {
	Notify(...backup.Result) error
	Error(interface{}) error
}

func MustToJSON(val interface{}) []byte {
	buf := bytes.NewBuffer(nil)
	if err := json.NewEncoder(buf).Encode(val); err != nil {
		panic(err)
	}

	return buf.Bytes()
}
