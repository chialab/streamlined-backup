package handler

import (
	"errors"
	"io"
	"time"

	"github.com/chialab/streamlined-backup/config"
)

type Handler interface {
	Handler(*io.PipeReader, time.Time) (func() error, error)
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
