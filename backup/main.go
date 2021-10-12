package backup

import (
	"fmt"
	"time"
)

const CHUNK_SIZE = 10 << 20

type Handler interface {
	Handler(<-chan Chunk, time.Time) (func() error, error)
	LastRun() (time.Time, error)
}

type Notifier interface {
	Notify(...OperationResult) error
	Error(interface{}) error
}

func ToError(val interface{}) error {
	if err, ok := val.(error); ok {
		return err
	}

	return fmt.Errorf("%+v", val)
}
