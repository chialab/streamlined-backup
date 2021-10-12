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

func ToJSON(val interface{}) ([]byte, error) {
	buf := bytes.NewBuffer(nil)
	if err := json.NewEncoder(buf).Encode(val); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}
