package handler

import (
	"bytes"
	"crypto/md5"
	"encoding/base64"
	"io"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/chialab/streamlined-backup/config"
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

const s3ChunkMinSize = 5 << 20  // 5 MiB
const s3ChunkMaxSize = 32 << 20 // 32 MiB

func newS3Handler(destination config.S3DestinationDefinition) *S3Handler {
	return &S3Handler{
		client:      destination.Client(),
		destination: destination,
	}
}

type S3Handler struct {
	client      s3iface.S3API
	destination config.S3DestinationDefinition
}

func (h S3Handler) Handler(reader *io.PipeReader, timestamp time.Time) (func() error, error) {
	upload, err := h.initMultipartUpload(timestamp)
	if err != nil {
		return nil, err
	}

	go func() {
		defer reader.Close()
		wg := sync.WaitGroup{}
		partNumber := int64(1)
		for {
			buf := make([]byte, s3ChunkMaxSize)
			bytes, err := io.ReadAtLeast(reader, buf, s3ChunkMinSize)
			if err == io.EOF {
				break
			} else if err != nil && err != io.ErrUnexpectedEOF {
				upload.Error = err
				break
			} else if bytes == 0 {
				continue
			}
			buf = buf[:bytes]

			wg.Add(1)
			go func(partNumber int64, chunk []byte) {
				defer wg.Done()

				upload.Parts <- h.uploadPart(upload, partNumber, chunk)
			}(partNumber, buf)
			partNumber++

			if err == io.ErrUnexpectedEOF {
				break
			}
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

func (h S3Handler) uploadPart(upload *s3MultipartUpload, partNumber int64, chunk []byte) s3UploadedPart {
	hash := md5.New()
	hash.Write(chunk)
	md5sum := base64.StdEncoding.EncodeToString(hash.Sum(nil))

	input := &s3.UploadPartInput{
		Bucket:        aws.String(upload.Bucket),
		Key:           aws.String(upload.Key),
		UploadId:      aws.String(upload.UploadId),
		Body:          bytes.NewReader(chunk),
		PartNumber:    aws.Int64(partNumber),
		ContentLength: aws.Int64(int64(len(chunk))),
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
