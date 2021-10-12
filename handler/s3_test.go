package handler

import (
	"errors"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/chialab/streamlined-backup/config"
	"github.com/chialab/streamlined-backup/utils"
	"github.com/hashicorp/go-multierror"
)

func TestS3UploadedParts(t *testing.T) {
	t.Parallel()

	parts := s3UploadedParts{
		{PartNumber: int64(42), ETag: "bar"},
		{PartNumber: int64(100), ETag: "foo"},
		{PartNumber: int64(10), ETag: "baz"},
	}
	if parts.Len() != 3 {
		t.Errorf("expected 3 parts, got %d", parts.Len())
	}
	if !parts.Less(0, 1) {
		t.Errorf("expected part 0 to be less than 1")
	}
	if parts.Less(0, 2) {
		t.Errorf("expected part 2 to be less than 0")
	}
	parts.Swap(1, 2)
	if parts[1].PartNumber != int64(10) {
		t.Errorf("expected part 1 to be 10, got %d", parts[1].PartNumber)
	} else if parts[2].PartNumber != int64(100) {
		t.Errorf("expected part 2 to be 100, got %d", parts[2].PartNumber)
	}
}

type mockedClientS3Upload struct {
	s3iface.S3API
	uploading     *sync.Mutex
	UploadedParts s3UploadedParts
	objects       map[string][]byte
	CalledApis    struct {
		CreateMultipartUpload   uint32
		UploadPart              uint32
		CompleteMultipartUpload uint32
		AbortMultipartUpload    uint32
	}
}

func (c *mockedClientS3Upload) CreateMultipartUpload(input *s3.CreateMultipartUploadInput) (*s3.CreateMultipartUploadOutput, error) {
	atomic.AddUint32(&c.CalledApis.CreateMultipartUpload, 1)
	if input.Bucket == nil || *input.Bucket != "example-bucket" {
		return nil, awserr.New(s3.ErrCodeNoSuchBucket, "", nil)
	}

	c.UploadedParts = s3UploadedParts{}
	c.uploading = new(sync.Mutex)

	return &s3.CreateMultipartUploadOutput{
		Bucket:   input.Bucket,
		Key:      input.Key,
		UploadId: aws.String("upload-id"),
	}, nil
}

func (c *mockedClientS3Upload) UploadPart(input *s3.UploadPartInput) (*s3.UploadPartOutput, error) {
	atomic.AddUint32(&c.CalledApis.UploadPart, 1)
	if input.Bucket == nil || *input.Bucket != "example-bucket" {
		return nil, awserr.New(s3.ErrCodeNoSuchBucket, "", nil)
	}
	if input.UploadId == nil || *input.UploadId != "upload-id" {
		return nil, awserr.New(s3.ErrCodeNoSuchUpload, "", nil)
	}

	body := make([]byte, *input.ContentLength)
	if size, err := input.Body.Read(body); err != nil {
		return nil, err
	} else if size != int(*input.ContentLength) {
		return nil, errors.New("unexpected read size")
	}
	if string(body) == "error" {
		return nil, errors.New("test error")
	} else if sleep, err := time.ParseDuration(string(body)); err == nil {
		time.Sleep(sleep)
	}

	etag := string(body)
	part := &s3UploadedPart{
		PartNumber: *input.PartNumber,
		ETag:       etag,
	}

	c.uploading.Lock()
	defer c.uploading.Unlock()
	c.UploadedParts = append(c.UploadedParts, *part)

	return &s3.UploadPartOutput{ETag: &etag}, nil
}

func (c *mockedClientS3Upload) CompleteMultipartUpload(input *s3.CompleteMultipartUploadInput) (*s3.CompleteMultipartUploadOutput, error) {
	atomic.AddUint32(&c.CalledApis.CompleteMultipartUpload, 1)
	if input.Bucket == nil || *input.Bucket != "example-bucket" {
		return nil, awserr.New(s3.ErrCodeNoSuchBucket, "", nil)
	}
	if input.UploadId == nil || *input.UploadId != "upload-id" {
		return nil, awserr.New(s3.ErrCodeNoSuchUpload, "", nil)
	}

	sort.Sort(c.UploadedParts)
	if len(c.UploadedParts) != len(input.MultipartUpload.Parts) {
		return nil, errors.New("number of parts does not match")
	}

	body := []byte{}
	for i, part := range input.MultipartUpload.Parts {
		stored := c.UploadedParts[i]
		if part.ETag == nil || *part.ETag != stored.ETag {
			return nil, errors.New("part ETags do not match")
		}
		if part.PartNumber == nil || *part.PartNumber != stored.PartNumber {
			return nil, errors.New("part numbers do not match")
		}
		body = append(body, []byte(stored.ETag)...)
	}

	if input.Key != nil && strings.HasPrefix(*input.Key, "complete-error/") {
		return nil, awserr.New(s3.ErrCodeNoSuchKey, "", nil)
	}
	c.objects[*input.Key] = body

	return &s3.CompleteMultipartUploadOutput{}, nil
}

func (c *mockedClientS3Upload) AbortMultipartUpload(input *s3.AbortMultipartUploadInput) (*s3.AbortMultipartUploadOutput, error) {
	atomic.AddUint32(&c.CalledApis.AbortMultipartUpload, 1)
	if input.Bucket == nil || *input.Bucket != "example-bucket" {
		return nil, awserr.New(s3.ErrCodeNoSuchBucket, "", nil)
	}
	if input.UploadId == nil || *input.UploadId != "upload-id" {
		return nil, awserr.New(s3.ErrCodeNoSuchUpload, "", nil)
	}

	if input.Key != nil && strings.HasPrefix(*input.Key, "abort-error/") {
		return nil, awserr.New(s3.ErrCodeNoSuchKey, "", nil)
	}

	return &s3.AbortMultipartUploadOutput{}, nil
}

func TestS3Handler(t *testing.T) {
	t.Parallel()

	client := &mockedClientS3Upload{
		objects: make(map[string][]byte),
	}
	dest := config.S3DestinationDefinition{
		Bucket: "example-bucket",
		Prefix: "foo/",
	}
	handler := &S3Handler{client: client, destination: dest}

	chunks := make(chan utils.Chunk)
	now := time.Date(2021, 10, 8, 18, 9, 17, 0, time.Local)
	wait, initErr := handler.Handler(chunks, now)
	if initErr != nil {
		t.Fatalf("unexpected error: %s", initErr)
	}
	chunks <- utils.Chunk{Data: []byte("10ms")}
	chunks <- utils.Chunk{Data: []byte("5ms")}
	chunks <- utils.Chunk{Data: []byte("1ms")}
	close(chunks)

	if err := wait(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	key := "foo/20211008180917"
	if client.objects[key] == nil {
		t.Errorf("expected object %s, got nil", key)
	} else if string(client.objects[key]) != "10ms5ms1ms" {
		t.Errorf("expected object %s to be 10ms5ms1ms, got %s", key, string(client.objects[key]))
	}

	if client.CalledApis.CreateMultipartUpload != 1 {
		t.Errorf("expected CreateMultipartUpload to be called once, got %d", client.CalledApis.CreateMultipartUpload)
	}
	if client.CalledApis.UploadPart != 3 {
		t.Errorf("expected UploadPart to be called 3 times, got %d", client.CalledApis.UploadPart)
	}
	if client.CalledApis.CompleteMultipartUpload != 1 {
		t.Errorf("expected CompleteMultipartUpload to be called once, got %d", client.CalledApis.CompleteMultipartUpload)
	}
	if client.CalledApis.AbortMultipartUpload != 0 {
		t.Errorf("expected AbortMultipartUpload to be called 0 times, got %d", client.CalledApis.AbortMultipartUpload)
	}
}

func TestS3HandlerInitError(t *testing.T) {
	t.Parallel()

	client := &mockedClientS3Upload{
		objects: make(map[string][]byte),
	}
	dest := config.S3DestinationDefinition{
		Bucket: "wrong-bucket",
		Prefix: "foo/",
	}
	handler := &S3Handler{client: client, destination: dest}

	chunks := make(chan utils.Chunk)
	now := time.Date(2021, 10, 8, 18, 9, 17, 0, time.Local)

	if wait, initErr := handler.Handler(chunks, now); initErr == nil {
		t.Error("expected error, got nil")
	} else if wait != nil {
		t.Error("expected nil wait, got non-nil")
	}

	if client.CalledApis.CreateMultipartUpload != 1 {
		t.Errorf("expected CreateMultipartUpload to be called once, got %d", client.CalledApis.CreateMultipartUpload)
	}
	if client.CalledApis.UploadPart != 0 {
		t.Errorf("expected UploadPart to be called 0 times, got %d", client.CalledApis.UploadPart)
	}
	if client.CalledApis.CompleteMultipartUpload != 0 {
		t.Errorf("expected CompleteMultipartUpload to be called 0 times, got %d", client.CalledApis.CompleteMultipartUpload)
	}
	if client.CalledApis.AbortMultipartUpload != 0 {
		t.Errorf("expected AbortMultipartUpload to be called 0 times, got %d", client.CalledApis.AbortMultipartUpload)
	}
}

func TestS3HandlerUploadError(t *testing.T) {
	t.Parallel()

	client := &mockedClientS3Upload{
		objects: make(map[string][]byte),
	}
	dest := config.S3DestinationDefinition{
		Bucket: "example-bucket",
		Prefix: "foo/",
	}
	handler := &S3Handler{client: client, destination: dest}

	chunks := make(chan utils.Chunk)
	now := time.Date(2021, 10, 8, 18, 9, 17, 0, time.Local)
	wait, initErr := handler.Handler(chunks, now)
	if initErr != nil {
		t.Fatalf("unexpected error: %s", initErr)
	}
	chunks <- utils.Chunk{Data: []byte("10ms")}
	chunks <- utils.Chunk{Data: []byte("error")}
	chunks <- utils.Chunk{Data: []byte("1ms")}
	close(chunks)

	if err := wait(); err == nil {
		t.Error("expected error, got nil")
	}
	key := "foo/20211008180917"
	if client.objects[key] != nil {
		t.Errorf("expected object %s to be nil, got %s", key, string(client.objects[key]))
	}

	if client.CalledApis.CreateMultipartUpload != 1 {
		t.Errorf("expected CreateMultipartUpload to be called once, got %d", client.CalledApis.CreateMultipartUpload)
	}
	if client.CalledApis.UploadPart != 3 {
		t.Errorf("expected UploadPart to be called 3 times, got %d", client.CalledApis.UploadPart)
	}
	if client.CalledApis.CompleteMultipartUpload != 0 {
		t.Errorf("expected CompleteMultipartUpload to be called 0 times, got %d", client.CalledApis.AbortMultipartUpload)
	}
	if client.CalledApis.AbortMultipartUpload != 1 {
		t.Errorf("expected AbortMultipartUpload to be called once, got %d", client.CalledApis.CompleteMultipartUpload)
	}
}

func TestS3HandlerChunkError(t *testing.T) {
	t.Parallel()

	client := &mockedClientS3Upload{
		objects: make(map[string][]byte),
	}
	dest := config.S3DestinationDefinition{
		Bucket: "example-bucket",
		Prefix: "foo/",
	}
	handler := &S3Handler{client: client, destination: dest}

	chunks := make(chan utils.Chunk)
	now := time.Date(2021, 10, 8, 18, 9, 17, 0, time.Local)
	wait, initErr := handler.Handler(chunks, now)
	if initErr != nil {
		t.Fatalf("unexpected error: %s", initErr)
	}
	chunks <- utils.Chunk{Data: []byte("10ms")}
	chunks <- utils.Chunk{Data: []byte("5ms"), Error: errors.New("test error")}
	close(chunks)

	if err := wait(); err == nil {
		t.Error("expected error, got nil")
	}
	key := "foo/20211008180917"
	if client.objects[key] != nil {
		t.Errorf("expected object %s to be nil, got %s", key, string(client.objects[key]))
	}

	if client.CalledApis.CreateMultipartUpload != 1 {
		t.Errorf("expected CreateMultipartUpload to be called once, got %d", client.CalledApis.CreateMultipartUpload)
	}
	if client.CalledApis.UploadPart != 1 {
		t.Errorf("expected UploadPart to be called once, got %d", client.CalledApis.UploadPart)
	}
	if client.CalledApis.CompleteMultipartUpload != 0 {
		t.Errorf("expected CompleteMultipartUpload to be called 0 times, got %d", client.CalledApis.AbortMultipartUpload)
	}
	if client.CalledApis.AbortMultipartUpload != 1 {
		t.Errorf("expected AbortMultipartUpload to be called once, got %d", client.CalledApis.CompleteMultipartUpload)
	}
}

func TestS3HandlerCompleteError(t *testing.T) {
	t.Parallel()

	client := &mockedClientS3Upload{
		objects: make(map[string][]byte),
	}
	dest := config.S3DestinationDefinition{
		Bucket: "example-bucket",
		Prefix: "complete-error/",
	}
	handler := &S3Handler{client: client, destination: dest}

	chunks := make(chan utils.Chunk)
	now := time.Date(2021, 10, 8, 18, 9, 17, 0, time.Local)
	wait, initErr := handler.Handler(chunks, now)
	if initErr != nil {
		t.Fatalf("unexpected error: %s", initErr)
	}
	chunks <- utils.Chunk{Data: []byte("10ms")}
	chunks <- utils.Chunk{Data: []byte("5ms")}
	chunks <- utils.Chunk{Data: []byte("5ms")}
	close(chunks)

	if err := wait(); err == nil {
		t.Error("expected error, got nil")
	}
	key := "complete-error/20211008180917"
	if client.objects[key] != nil {
		t.Errorf("expected object %s to be nil, got %s", key, string(client.objects[key]))
	}

	if client.CalledApis.CreateMultipartUpload != 1 {
		t.Errorf("expected CreateMultipartUpload to be called once, got %d", client.CalledApis.CreateMultipartUpload)
	}
	if client.CalledApis.UploadPart != 3 {
		t.Errorf("expected UploadPart to be called 3 times, got %d", client.CalledApis.UploadPart)
	}
	if client.CalledApis.CompleteMultipartUpload != 1 {
		t.Errorf("expected CompleteMultipartUpload to be called once, got %d", client.CalledApis.AbortMultipartUpload)
	}
	if client.CalledApis.AbortMultipartUpload != 1 {
		t.Errorf("expected AbortMultipartUpload to be called once, got %d", client.CalledApis.CompleteMultipartUpload)
	}
}

func TestS3HandlerAbortError(t *testing.T) {
	t.Parallel()

	client := &mockedClientS3Upload{
		objects: make(map[string][]byte),
	}
	dest := config.S3DestinationDefinition{
		Bucket: "example-bucket",
		Prefix: "abort-error/",
	}
	handler := &S3Handler{client: client, destination: dest}

	chunks := make(chan utils.Chunk)
	now := time.Date(2021, 10, 8, 18, 9, 17, 0, time.Local)
	wait, initErr := handler.Handler(chunks, now)
	if initErr != nil {
		t.Fatalf("unexpected error: %s", initErr)
	}
	chunks <- utils.Chunk{Data: []byte("10ms")}
	chunks <- utils.Chunk{Data: []byte("5ms"), Error: errors.New("test error")}
	close(chunks)

	if err := wait(); err == nil {
		t.Error("expected error, got nil")
	} else if mErr, ok := err.(*multierror.Error); !ok {
		t.Errorf("expected multierror, got %T", err)
	} else if len(mErr.Errors) != 2 {
		t.Errorf("expected 2 errors, got %d", len(mErr.Errors))
	}
	key := "abort-error/20211008180917"
	if client.objects[key] != nil {
		t.Errorf("expected object %s to be nil, got %s", key, string(client.objects[key]))
	}

	if client.CalledApis.CreateMultipartUpload != 1 {
		t.Errorf("expected CreateMultipartUpload to be called once, got %d", client.CalledApis.CreateMultipartUpload)
	}
	if client.CalledApis.UploadPart != 1 {
		t.Errorf("expected UploadPart to be called once, got %d", client.CalledApis.UploadPart)
	}
	if client.CalledApis.CompleteMultipartUpload != 0 {
		t.Errorf("expected CompleteMultipartUpload to be called 0 times, got %d", client.CalledApis.AbortMultipartUpload)
	}
	if client.CalledApis.AbortMultipartUpload != 1 {
		t.Errorf("expected AbortMultipartUpload to be called once, got %d", client.CalledApis.CompleteMultipartUpload)
	}
}

type mockedClientS3LastRun struct {
	s3iface.S3API
	CalledApis struct {
		ListObjects uint32
	}
}

func (c *mockedClientS3LastRun) ListObjects(req *s3.ListObjectsInput) (*s3.ListObjectsOutput, error) {
	atomic.AddUint32(&c.CalledApis.ListObjects, 1)
	if req.Bucket == nil || *req.Bucket != "example-bucket" {
		return nil, awserr.New(s3.ErrCodeNoSuchBucket, "", nil)
	}
	if req.Prefix == nil || *req.Prefix != "foo/" {
		return &s3.ListObjectsOutput{}, nil
	}

	pages := map[string]*s3.ListObjectsOutput{
		"": {
			NextMarker: aws.String("page 2"),
			Contents: []*s3.Object{
				{Key: aws.String("foo/20200819093000-barbaz.tgz")},
				{Key: aws.String("foo/bar/20200818093000-barbaz.tgz")},
				{Key: aws.String("foo/20200817093000-bar.sql")},
				{Key: aws.String("foo/20200819093000-bar.sql")},
				{Key: aws.String("foo/20200816093000-bar.sql")},
			},
		},
		"page 2": {
			Contents: []*s3.Object{
				{Key: aws.String("foo/20210819093000-barbaz.tgz")},
				{Key: aws.String("foo/invaliddate-bar.sql")},
				{Key: aws.String("foo/20210816093000-bar.sql")},
				{Key: aws.String("foo/20210817093000-bar.sql")},
				{Key: aws.String("foo/20210815093000-bar.sql")},
			},
		},
	}

	marker := req.Marker
	if marker == nil {
		return pages[""], nil
	} else if pages[*marker] == nil {
		return nil, errors.New("invalid marker")
	}

	return pages[*marker], nil
}

func TestS3LastRun(t *testing.T) {
	t.Parallel()

	bucket, prefix, suffix := "example-bucket", "foo/", "-bar.sql"
	expectedLastRun := time.Date(2021, 8, 17, 9, 30, 0, 0, time.Local)

	dest := &config.S3DestinationDefinition{
		Region: "us-east-1",
		Bucket: bucket,
		Prefix: prefix,
		Suffix: suffix,
	}
	s3Client := &mockedClientS3LastRun{}
	s3Handler := &S3Handler{
		client:      s3Client,
		destination: *dest,
	}
	if lastRun, err := s3Handler.LastRun(); err != nil {
		t.Errorf("expected no error, got %s", err)
	} else if !expectedLastRun.Equal(lastRun) {
		t.Errorf("expected %s, got %s", expectedLastRun, lastRun)
	}
	if s3Client.CalledApis.ListObjects != 2 {
		t.Errorf("expected ListObjects to be called twice, got %d", s3Client.CalledApis.ListObjects)
	}
}

func TestS3LastRunEmpty(t *testing.T) {
	t.Parallel()

	bucket, prefix, suffix := "example-bucket", "bar/", "-bar.sql"
	expectedLastRun := time.Time{}

	dest := &config.S3DestinationDefinition{
		Region: "us-east-1",
		Bucket: bucket,
		Prefix: prefix,
		Suffix: suffix,
	}
	s3Client := &mockedClientS3LastRun{}
	s3Handler := &S3Handler{
		client:      s3Client,
		destination: *dest,
	}
	if lastRun, err := s3Handler.LastRun(); err != nil {
		t.Errorf("expected no error, got %s", err)
	} else if !expectedLastRun.Equal(lastRun) {
		t.Errorf("expected %s, got %s", expectedLastRun, lastRun)
	}
	if s3Client.CalledApis.ListObjects != 1 {
		t.Errorf("expected ListObjects to be called once, got %d", s3Client.CalledApis.ListObjects)
	}
}

func TestS3LastRunError(t *testing.T) {
	t.Parallel()

	bucket, prefix, suffix := "wrong-bucket", "foo/", "-bar.sql"

	dest := &config.S3DestinationDefinition{
		Region: "us-east-1",
		Bucket: bucket,
		Prefix: prefix,
		Suffix: suffix,
	}
	s3Client := &mockedClientS3LastRun{}
	s3Handler := &S3Handler{
		client:      s3Client,
		destination: *dest,
	}
	if lastRun, err := s3Handler.LastRun(); err == nil {
		t.Errorf("expected error, got %s", lastRun)
	} else if !lastRun.IsZero() {
		t.Errorf("expected zero time, got %s", lastRun)
	}
	if s3Client.CalledApis.ListObjects != 1 {
		t.Errorf("expected ListObjects to be called once, got %d", s3Client.CalledApis.ListObjects)
	}
}
