package backup

import (
	"crypto/md5"
	"encoding/base64"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/client"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/chialab/streamlined-backup/utils"
	"github.com/hashicorp/go-multierror"
)

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

type s3MultipartUpload struct {
	UploadId string
	Bucket   string
	Key      string
	Parts    chan s3UploadedPart
	Error    error
}

const timeFormat = "20060102150405"

type S3DestinationDefinition struct {
	Bucket string
	Prefix string
	Suffix string
	Region string
}

func (d S3DestinationDefinition) Key(timestamp time.Time) string {
	return fmt.Sprintf("%s%s%s", d.Prefix, timestamp.Format(timeFormat), d.Suffix)
}

func (d S3DestinationDefinition) ParseTimestamp(key string) (time.Time, error) {
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

func newS3Handler(destination S3DestinationDefinition) *S3Handler {
	session := session.Must(session.NewSession())
	client := s3.New(session, &aws.Config{
		Retryer: &client.DefaultRetryer{NumMaxRetries: 3},
		Region:  aws.String(destination.Region),
	})

	return &S3Handler{
		client:      client,
		destination: destination,
	}
}

type S3Handler struct {
	client      s3iface.S3API
	destination S3DestinationDefinition
}

func (h S3Handler) Handler(chunks <-chan utils.Chunk, timestamp time.Time) (func() error, error) {
	upload, err := h.initMultipartUpload(timestamp)
	if err != nil {
		return nil, err
	}

	go func() {
		wg := sync.WaitGroup{}
		partNumber := int64(1)
		for chunk := range chunks {
			if chunk.Error != nil {
				upload.Error = chunk.Error

				break
			}

			wg.Add(1)
			go func(partNumber int64, chunk utils.Chunk) {
				defer wg.Done()

				upload.Parts <- h.uploadPart(upload, partNumber, chunk)
			}(partNumber, chunk)
			partNumber++
		}
		wg.Wait()
		close(upload.Parts)
	}()

	return func() (err error) {
		// Wait for all pending uploads to finish
		parts := make(s3UploadedParts, 0)
		for part := range upload.Parts {
			parts = append(parts, part)
		}

		defer func() {
			// Abort the upload if any error occurred
			if panic := recover(); panic != nil {
				var multiErr *multierror.Error
				if panicErr, ok := panic.(error); ok {
					multiErr = multierror.Append(multiErr, panicErr)
				}
				if abortErr := h.abortMultipartUpload(upload); abortErr != nil {
					multiErr = multierror.Append(multiErr, abortErr)
				}

				err = multiErr.ErrorOrNil()
			}
		}()

		if upload.Error != nil {
			panic(upload.Error)
		} else if err := h.completeMultipartUpload(upload, parts); err != nil {
			panic(err)
		}

		return nil
	}, nil
}

func (h S3Handler) initMultipartUpload(timestamp time.Time) (*s3MultipartUpload, error) {
	key := h.destination.Key(timestamp)
	input := &s3.CreateMultipartUploadInput{
		Bucket: aws.String(h.destination.Bucket),
		Key:    aws.String(key),
	}
	if result, err := h.client.CreateMultipartUpload(input); err != nil {
		return nil, err
	} else {
		return &s3MultipartUpload{
			UploadId: *result.UploadId,
			Bucket:   h.destination.Bucket,
			Key:      key,
			Parts:    make(chan s3UploadedPart),
		}, nil
	}
}

func (h S3Handler) uploadPart(upload *s3MultipartUpload, partNumber int64, chunk utils.Chunk) s3UploadedPart {
	hash := md5.New()
	hash.Write(chunk.Data)
	md5sum := base64.StdEncoding.EncodeToString(hash.Sum(nil))

	input := &s3.UploadPartInput{
		Bucket:        aws.String(upload.Bucket),
		Key:           aws.String(upload.Key),
		UploadId:      aws.String(upload.UploadId),
		Body:          chunk.NewReader(),
		PartNumber:    aws.Int64(partNumber),
		ContentLength: aws.Int64(int64(len(chunk.Data))),
		ContentMD5:    aws.String(md5sum),
	}
	if result, err := h.client.UploadPart(input); err != nil {
		return s3UploadedPart{Error: err, PartNumber: partNumber}
	} else {
		return s3UploadedPart{PartNumber: partNumber, ETag: *result.ETag}
	}

}

func (h S3Handler) abortMultipartUpload(upload *s3MultipartUpload) error {
	input := &s3.AbortMultipartUploadInput{
		Bucket:   aws.String(upload.Bucket),
		Key:      aws.String(upload.Key),
		UploadId: aws.String(upload.UploadId),
	}
	if _, err := h.client.AbortMultipartUpload(input); err != nil {
		return err
	}

	return nil
}

func (h S3Handler) completeMultipartUpload(upload *s3MultipartUpload, uploadedParts s3UploadedParts) error {
	parts := make([]*s3.CompletedPart, 0)
	sort.Sort(uploadedParts)
	for _, part := range uploadedParts {
		if part.Error != nil {
			return part.Error
		}

		parts = append(parts, &s3.CompletedPart{
			PartNumber: aws.Int64(part.PartNumber),
			ETag:       aws.String(part.ETag),
		})
	}

	input := &s3.CompleteMultipartUploadInput{
		Bucket:          aws.String(upload.Bucket),
		Key:             aws.String(upload.Key),
		UploadId:        aws.String(upload.UploadId),
		MultipartUpload: &s3.CompletedMultipartUpload{Parts: parts},
	}
	if _, err := h.client.CompleteMultipartUpload(input); err != nil {
		return err
	}

	return nil
}

func (h S3Handler) LastRun() (time.Time, error) {
	var marker *string
	var lastRun time.Time
	for {
		result, err := h.client.ListObjects(&s3.ListObjectsInput{
			Bucket: aws.String(h.destination.Bucket),
			Prefix: aws.String(h.destination.Prefix),
			Marker: marker,
		})
		if err != nil {
			return time.Time{}, err
		}

		for _, object := range result.Contents {
			if !strings.HasPrefix(*object.Key, h.destination.Prefix) || !strings.HasSuffix(*object.Key, h.destination.Suffix) {
				continue
			}

			if run, err := h.destination.ParseTimestamp(*object.Key); err != nil {
				continue
			} else if run.After(lastRun) {
				lastRun = run
			}
		}

		marker = result.NextMarker
		if marker == nil {
			break
		}
	}

	return lastRun, nil
}
