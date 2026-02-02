package gos3

// redesign notes:
// move top level functions to SVC object methods
// change return types to struct responses containing return values, errors, etc...
//   - improve backwards/forwards compatiblity by enabling additional return data to be added without affecting code structure
// implement customizable retry logic

import (
	"fmt"
	"io"
	"strings"

	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/ggarcia209/go-aws-v2/v1/goaws"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
)

const DefaultPartitionSize = int64(1024 * 1024 * 64) // 64mb

type S3Logic interface {
	GetObject(bucket, key string) ([]byte, error)
	UploadFile(bucket, key string, file io.Reader, publicRead bool) (UploadFileResponse, error)
	DeleteFile(bucket, key string) error
}

type S3 struct {
	svc      *s3.S3
	uploader *s3manager.Uploader
}

func NewS3(sess goaws.Session, partitionSize int64) *S3 {
	svc := s3.New(sess.GetSession())
	if partitionSize < 1024 {
		partitionSize = DefaultPartitionSize
	}
	uploader := s3manager.NewUploaderWithClient(svc, func(u *s3manager.Uploader) {
		u.PartSize = partitionSize
	})
	return &S3{
		svc:      svc,
		uploader: uploader,
	}
}

func NewS3Client(session goaws.Session) interface{} {
	return s3.New(session.GetSession())
}

// GetObject returns the S3 object at the given bucket/key as a byte slice.
func (s *S3) GetObject(bucket, key string) ([]byte, error) {
	obj, err := s.svc.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			if awsErr.Code() == "NoSuchKey" {
				return []byte{}, fmt.Errorf("s.svc.GetObject: %w", ErrNoSuchKey)
			}
		}
		return []byte{}, fmt.Errorf("s.svc.GetObject: %w", err)
	}

	buf := new(strings.Builder)
	if _, err = io.Copy(buf, obj.Body); err != nil {
		return []byte{}, fmt.Errorf("io.Copy: %w", err)
	}

	res := []byte(buf.String())

	return res, nil
}

// UploadFile uploads a new file to the given S3 bucket.
func (s *S3) UploadFile(bucket, key string, file io.Reader, publicRead bool) (UploadFileResponse, error) {
	input := &s3manager.UploadInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   file,
	}
	if publicRead {
		input.ACL = aws.String("public-read")
	}
	result, err := s.uploader.Upload(input)
	if err != nil {
		return UploadFileResponse{}, fmt.Errorf("s.uploader.Upload: %w", err)
	}

	resp := UploadFileResponse{
		Location: result.Location,
		UploadID: result.UploadID,
	}
	if result.VersionID != nil {
		resp.VersionID = *result.VersionID
	}

	return resp, nil
}

// UploadFileResponse contains the data returned by the S3 Upload operation.
type UploadFileResponse struct {
	Location  string `json:"location"`
	VersionID string `json:"version_id"`
	UploadID  string `json:"upload_id"`
	ETag      string `json:"etag"`
}

// DeleteFile deletes the the file at bucket/key
func (s *S3) DeleteFile(bucket, key string) error {
	input := &s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}

	if _, err := s.svc.DeleteObject(input); err != nil {
		return fmt.Errorf("s.svc.DeleteObject: %w", err)
	}

	return nil
}
