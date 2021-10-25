package config

import (
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/client"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

const S3_TIME_FORMAT = "20060102150405"

type DestinationType string

const (
	S3Destination DestinationType = "s3"
)

type Destination struct {
	Type DestinationType
	S3   S3DestinationDefinition
}

type S3DestinationDefinition struct {
	Bucket      string
	Prefix      string
	Suffix      string
	Region      string
	Credentials *struct {
		AccessKeyId     string
		SecretAccessKey string
		SessionToken    string
	}
	Profile *string
}

func (d S3DestinationDefinition) credentials() *credentials.Credentials {
	if d.Credentials != nil {
		return credentials.NewStaticCredentials(d.Credentials.AccessKeyId, d.Credentials.SecretAccessKey, d.Credentials.SessionToken)
	} else if d.Profile != nil {
		return credentials.NewSharedCredentials("", *d.Profile)
	}

	return nil
}

func (d S3DestinationDefinition) Client() *s3.S3 {
	session := session.Must(session.NewSession(&aws.Config{
		Region:      aws.String(d.Region),
		Credentials: d.credentials(),
	}))
	client := s3.New(session, &aws.Config{
		Retryer: &client.DefaultRetryer{NumMaxRetries: 3},
	})

	return client
}

func (d S3DestinationDefinition) Key(timestamp time.Time) string {
	return fmt.Sprintf("%s%s%s", d.Prefix, timestamp.Format(S3_TIME_FORMAT), d.Suffix)
}

func (d S3DestinationDefinition) ParseTimestamp(key string) (time.Time, error) {
	if !strings.HasPrefix(key, d.Prefix) || !strings.HasSuffix(key, d.Suffix) {
		return time.Time{}, fmt.Errorf("key %s does not match prefix %s and suffix %s", key, d.Prefix, d.Suffix)
	}

	ts := strings.TrimSuffix(strings.TrimPrefix(key, d.Prefix), d.Suffix)
	if timestamp, err := time.ParseInLocation(S3_TIME_FORMAT, ts, time.Local); err != nil {
		return time.Time{}, err
	} else {
		return timestamp, nil
	}
}
