package backup

import (
	"time"
)

const CHUNK_SIZE = 10 << 20

type Handler interface {
	Handler(chunks chan Chunk)

	Wait() error
}

type LastRunner interface {
	LastRun(Destination) (time.Time, error)
}

type Operations map[string]Operation

type DestinationType string

const (
	S3Destination DestinationType = "s3"
)

type Destination struct {
	Type   DestinationType
	Bucket string
	Prefix string
	Suffix string
	Region string
}
