package backup

import (
	"errors"
	"time"
)

type Uploader interface {
	Handler
	LastRunner
}

func NewHandler(destination Destination, timestamp time.Time) (Uploader, error) {
	switch destination.Type {
	case S3Destination:
		return newS3Handler(destination, timestamp), nil
	}

	return nil, errors.New("unknown destination type")
}
