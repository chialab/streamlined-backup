package backup

import (
	"crypto/md5"
	"encoding/base64"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/client"
	"github.com/aws/aws-sdk-go/aws/session"
	S3 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/hashicorp/go-multierror"
)

const timeFormat = "20060102150405"

type s3UploadedPart struct {
	PartNumber int64
	Error      error
	ETag       string
}

type s3UploadedParts []s3UploadedPart

func (p s3UploadedParts) Len() int {
	return len(p)
}
func (p s3UploadedParts) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
}
func (p s3UploadedParts) Less(i, j int) bool {
	return p[i].PartNumber < p[j].PartNumber
}

type S3Handler struct {
	bucket      string
	path        string
	initialized bool
	client      *S3.S3
	completion  chan error
}

func newS3Handler(destination Destination, timestamp time.Time) *S3Handler {
	session := session.Must(session.NewSession())
	client := S3.New(session, &aws.Config{
		Retryer: &client.DefaultRetryer{NumMaxRetries: 3},
		Region:  aws.String(destination.Region),
	})

	return &S3Handler{
		bucket:     destination.Bucket,
		path:       fmt.Sprintf("%s%s%s", destination.Prefix, timestamp.Format(timeFormat), destination.Suffix),
		client:     client,
		completion: make(chan error, 1),
	}
}

func (s3 *S3Handler) Handler(chunks chan Chunk) {
	uploadId, err := s3.initMultipartUpload()
	if err != nil {
		s3.completion <- err

		return
	}

	parts := make(s3UploadedParts, 0)
	uploads := make([]chan s3UploadedPart, 0)

	var chunk Chunk
	for !chunk.Done {
		chunk = <-chunks
		if chunk.Error != nil {
			break
		}
		if chunk.Done && len(chunk.Data) == 0 {
			continue
		}

		ch := make(chan s3UploadedPart, 1)
		uploads = append(uploads, ch)
		partNumber := int64(len(uploads))
		go func(ch chan s3UploadedPart, partNumber int64, chunk Chunk) {
			ch <- s3.uploadPart(uploadId, partNumber, chunk)
		}(ch, partNumber, chunk)
	}

	s3.waitUntilUploadsComplete(uploads, &parts)
	if chunk.Error != nil {
		if abortErr := s3.abortMultipartUpload(uploadId); err != nil {
			s3.completion <- multierror.Append(chunk.Error, abortErr)
		} else {
			s3.completion <- chunk.Error
		}
	} else if err := s3.completeMultipartUpload(uploadId, parts); err != nil {
		if abortErr := s3.abortMultipartUpload(uploadId); err != nil {
			s3.completion <- multierror.Append(err, abortErr)
		} else {
			s3.completion <- err
		}
	} else {
		s3.completion <- nil
	}
}

func (s3 *S3Handler) Wait() error {
	if err := <-s3.completion; err != nil {
		return err
	}

	return nil
}

func (s3 *S3Handler) initMultipartUpload() (string, error) {
	if s3.initialized {
		return "", errors.New("already initialized")
	}

	result, err := s3.client.CreateMultipartUpload(&S3.CreateMultipartUploadInput{
		Bucket: aws.String(s3.bucket),
		Key:    aws.String(s3.path),
	})
	if err != nil {
		return "", err
	}

	s3.initialized = true

	return *result.UploadId, nil
}

func (s3 *S3Handler) uploadPart(uploadId string, partNumber int64, chunk Chunk) s3UploadedPart {
	h := md5.New()
	h.Write(chunk.Data)
	hash := base64.StdEncoding.EncodeToString(h.Sum(nil))

	result, err := s3.client.UploadPart(&S3.UploadPartInput{
		Bucket:        aws.String(s3.bucket),
		Key:           aws.String(s3.path),
		UploadId:      aws.String(uploadId),
		Body:          chunk.NewReader(),
		PartNumber:    aws.Int64(partNumber),
		ContentLength: aws.Int64(int64(len(chunk.Data))),
		ContentMD5:    aws.String(hash),
	})
	if err != nil {
		return s3UploadedPart{Error: err, PartNumber: partNumber}
	}

	return s3UploadedPart{PartNumber: partNumber, ETag: *result.ETag}
}

func (s3 *S3Handler) waitUntilUploadsComplete(uploads []chan s3UploadedPart, parts *s3UploadedParts) {
	for _, upload := range uploads {
		*parts = append(*parts, <-upload)
	}
}

func (s3 *S3Handler) abortMultipartUpload(uploadId string) error {
	_, err := s3.client.AbortMultipartUpload(&S3.AbortMultipartUploadInput{
		Bucket:   aws.String(s3.bucket),
		Key:      aws.String(s3.path),
		UploadId: aws.String(uploadId),
	})

	if err != nil {
		return err
	}

	s3.initialized = false

	return nil
}

func (s3 *S3Handler) completeMultipartUpload(uploadId string, uploadedParts s3UploadedParts) error {
	parts := make([]*S3.CompletedPart, 0)
	sort.Sort(uploadedParts)
	for _, part := range uploadedParts {
		if part.Error != nil {
			return part.Error
		}

		parts = append(parts, &S3.CompletedPart{
			PartNumber: aws.Int64(part.PartNumber),
			ETag:       aws.String(part.ETag),
		})
	}

	_, err := s3.client.CompleteMultipartUpload(&S3.CompleteMultipartUploadInput{
		Bucket:          aws.String(s3.bucket),
		Key:             aws.String(s3.path),
		UploadId:        aws.String(uploadId),
		MultipartUpload: &S3.CompletedMultipartUpload{Parts: parts},
	})

	if err != nil {
		return err
	}

	s3.initialized = false

	return nil
}

func (s3 *S3Handler) LastRun(destination Destination) (time.Time, error) {
	var lastKey *string
	var lastRun time.Time
	for {
		result, err := s3.client.ListObjects(&S3.ListObjectsInput{
			Bucket: aws.String(destination.Bucket),
			Prefix: aws.String(destination.Prefix),
			Marker: lastKey,
		})
		if err != nil {
			return time.Time{}, err
		}

		if len(result.Contents) == 0 {
			break
		}

		for _, object := range result.Contents {
			if !strings.HasPrefix(*object.Key, destination.Prefix) || !strings.HasSuffix(*object.Key, destination.Suffix) {
				continue
			}

			run, err := time.Parse(
				timeFormat,
				strings.TrimSuffix(strings.TrimPrefix(*object.Key, destination.Prefix), destination.Suffix),
			)
			if err != nil {
				continue
			}
			if run.After(lastRun) {
				lastRun = run
			}
		}

		lastKey = result.Contents[len(result.Contents)-1].Key
	}

	return lastRun, nil
}
