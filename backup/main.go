package backup

import (
	"time"

	"github.com/chialab/streamlined-backup/utils"
)

const CHUNK_SIZE = 10 << 20

type Handler interface {
	Handler(<-chan utils.Chunk, time.Time) (func() error, error)
	LastRun() (time.Time, error)
}
