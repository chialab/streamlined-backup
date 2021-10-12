package backup

import (
	"fmt"
	"strings"
	"time"
)

const timeFormat = "20060102150405"

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

func (d Destination) Key(timestamp time.Time) string {
	return fmt.Sprintf("%s%s%s", d.Prefix, timestamp.Format(timeFormat), d.Suffix)
}

func (d Destination) ParseTimestamp(key string) (time.Time, error) {
	if !strings.HasPrefix(key, d.Prefix) || !strings.HasSuffix(key, d.Suffix) {
		return time.Time{}, fmt.Errorf("key %s does not match prefix %s and suffix %s", key, d.Prefix, d.Suffix)
	}

	ts := strings.TrimSuffix(strings.TrimPrefix(key, d.Prefix), d.Suffix)
	if timestamp, err := time.ParseInLocation(timeFormat, ts, time.Local); err != nil {
		return time.Time{}, err
	} else {
		return timestamp, nil
	}
}
