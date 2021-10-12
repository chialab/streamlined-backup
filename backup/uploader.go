package backup

import (
	"errors"
)

var ErrUnknownDestination = errors.New("unknown destination type")

func NewHandler(destination Destination) (Handler, error) {
	switch destination.Type {
	case S3Destination:
		return newS3Handler(destination.S3), nil
	}

	return nil, ErrUnknownDestination
}
