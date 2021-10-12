package handler

import (
	"errors"
	"time"

	"github.com/chialab/streamlined-backup/config"
	"github.com/chialab/streamlined-backup/utils"
)

type Handler interface {
	Handler(<-chan utils.Chunk, time.Time) (func() error, error)
	LastRun() (time.Time, error)
}

var ErrUnknownDestination = errors.New("unknown destination type")

func NewHandler(destination config.Destination) (Handler, error) {
	switch destination.Type {
	case config.S3Destination:
		return newS3Handler(destination.S3), nil
	}

	return nil, ErrUnknownDestination
}
