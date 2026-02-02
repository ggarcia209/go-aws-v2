package gos3

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"go.uber.org/mock/gomock"

	"github.com/ggarcia209/go-aws-v2/v2/goaws"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewS3(t *testing.T) {
	cfg, err := goaws.NewDefaultConfig(context.Background())
	if err != nil {
		t.Errorf("goaws.NewDefaultConfig: %v", err)
		return
	}

	require.NotNil(t, cfg)

	// test interface implementation
	s3 := NewS3(*cfg, 256)
	assert.NotNil(t, s3)
	assert.NotNil(t, s3.svc)
	assert.Implements(t, (*S3Logic)(nil), s3)
}

func TestS3_GetObject(t *testing.T) {
	tests := []struct {
		name          string
		req           GetFileRequest
		mockSetup     func(ctrl *gomock.Controller) S3ClientAPI
		expectedBytes *GetObjectResponse
		expectedError error
	}{
		{
			name: "Success",
			req: GetFileRequest{
				Bucket: "test-bucket",
				Key:    "test-key",
			},
			mockSetup: func(ctrl *gomock.Controller) S3ClientAPI {
				m := NewMockS3ClientAPI(ctrl)
				m.EXPECT().GetObject(context.Background(), &s3.GetObjectInput{
					Bucket: aws.String("test-bucket"),
					Key:    aws.String("test-key"),
				}).Return(&s3.GetObjectOutput{
					Body: io.NopCloser(strings.NewReader("test content")),
				}, nil).Times(1)
				return m
			},
			expectedBytes: &GetObjectResponse{
				File: []byte("test content"),
			},
			expectedError: nil,
		},
		{
			name: "NotFound",
			req: GetFileRequest{
				Bucket: "test-bucket",
				Key:    "missing-key",
			},
			mockSetup: func(ctrl *gomock.Controller) S3ClientAPI {
				m := NewMockS3ClientAPI(ctrl)
				m.EXPECT().GetObject(context.Background(), &s3.GetObjectInput{
					Bucket: aws.String("test-bucket"),
					Key:    aws.String("missing-key"),
				}).Return(nil, &types.NoSuchKey{}).Times(1)
				return m
			},
			expectedBytes: nil,
			expectedError: NewItemNotFoundError("missing-key"),
		},
		{
			name: "StatusNotFound",
			req: GetFileRequest{
				Bucket: "test-bucket",
				Key:    "missing-key",
			},
			mockSetup: func(ctrl *gomock.Controller) S3ClientAPI {
				m := NewMockS3ClientAPI(ctrl)
				m.EXPECT().GetObject(context.Background(), &s3.GetObjectInput{
					Bucket: aws.String("test-bucket"),
					Key:    aws.String("missing-key"),
				}).Return(nil, &awshttp.ResponseError{
					ResponseError: &smithyhttp.ResponseError{
						Response: &smithyhttp.Response{
							Response: &http.Response{
								StatusCode: http.StatusNotFound,
							},
						},
					},
				}).Times(1)
				return m
			},
			expectedBytes: nil,
			expectedError: NewItemNotFoundError("missing-key"),
		},
		{
			name: "OtherError",
			req: GetFileRequest{
				Bucket: "test-bucket",
				Key:    "error-key",
			},
			mockSetup: func(ctrl *gomock.Controller) S3ClientAPI {
				m := NewMockS3ClientAPI(ctrl)
				m.EXPECT().GetObject(context.Background(), &s3.GetObjectInput{
					Bucket: aws.String("test-bucket"),
					Key:    aws.String("error-key"),
				}).Return(nil, errors.New("some error")).Times(1)
				return m
			},
			expectedBytes: nil,
			expectedError: goaws.NewInternalError(errors.New("s.svc.GetObject: some error")),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			mockSvc := tt.mockSetup(ctrl)
			s := &S3{svc: mockSvc}

			res, err := s.GetObject(context.Background(), tt.req)

			if tt.expectedError != nil {
				require.Error(t, err)
				assert.EqualError(t, tt.expectedError, err.Error())
				assert.Implements(t, (*goaws.AwsError)(nil), err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expectedBytes, res)
			}
		})
	}
}

func TestS3_HeadObject(t *testing.T) {
	tests := []struct {
		name          string
		req           GetFileRequest
		mockSetup     func(ctrl *gomock.Controller) S3ClientAPI
		expectedResp  *HeadObjectResponse
		expectedError error
	}{
		{
			name: "Success",
			req: GetFileRequest{
				Bucket: "test-bucket",
				Key:    "test-key",
			},
			mockSetup: func(ctrl *gomock.Controller) S3ClientAPI {
				m := NewMockS3ClientAPI(ctrl)
				m.EXPECT().HeadObject(context.Background(), &s3.HeadObjectInput{
					Bucket: aws.String("test-bucket"),
					Key:    aws.String("test-key"),
				}).Return(&s3.HeadObjectOutput{
					Metadata:    map[string]string{"foo": "bar"},
					ContentType: aws.String("application/json"),
				}, nil).Times(1)
				return m
			},
			expectedResp: &HeadObjectResponse{
				Metadata:    map[string]string{"foo": "bar"},
				ContentType: "application/json",
			},
			expectedError: nil,
		},
		{
			name: "NotFound",
			req: GetFileRequest{
				Bucket: "test-bucket",
				Key:    "missing-key",
			},
			mockSetup: func(ctrl *gomock.Controller) S3ClientAPI {
				m := NewMockS3ClientAPI(ctrl)
				m.EXPECT().HeadObject(context.Background(), &s3.HeadObjectInput{
					Bucket: aws.String("test-bucket"),
					Key:    aws.String("missing-key"),
				}).Return(nil, &types.NoSuchKey{}).Times(1)
				return m
			},
			expectedResp:  nil,
			expectedError: NewItemNotFoundError("missing-key"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			mockSvc := tt.mockSetup(ctrl)
			s := &S3{svc: mockSvc}

			res, err := s.HeadObject(context.Background(), tt.req)

			if tt.expectedError != nil {
				require.Error(t, err)
				assert.EqualError(t, tt.expectedError, err.Error())
				assert.Implements(t, (*goaws.AwsError)(nil), err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expectedResp, res)
			}
		})
	}
}

func TestS3_CheckIfObjectExists(t *testing.T) {
	tests := []struct {
		name           string
		req            GetFileRequest
		mockSetup      func(ctrl *gomock.Controller) S3ClientAPI
		expectedExists *ObjectExistsResponse
		expectedError  error
	}{
		{
			name: "Exists",
			req: GetFileRequest{
				Bucket: "test-bucket",
				Key:    "test-key",
			},
			mockSetup: func(ctrl *gomock.Controller) S3ClientAPI {
				m := NewMockS3ClientAPI(ctrl)
				m.EXPECT().HeadObject(context.Background(), &s3.HeadObjectInput{
					Bucket: aws.String("test-bucket"),
					Key:    aws.String("test-key"),
				}).Return(&s3.HeadObjectOutput{}, nil).Times(1)
				return m
			},
			expectedExists: &ObjectExistsResponse{
				Exists: true,
			},
			expectedError: nil,
		},
		{
			name: "DoesNotExist",
			req: GetFileRequest{
				Bucket: "test-bucket",
				Key:    "missing-key",
			},
			mockSetup: func(ctrl *gomock.Controller) S3ClientAPI {
				m := NewMockS3ClientAPI(ctrl)
				m.EXPECT().HeadObject(context.Background(), &s3.HeadObjectInput{
					Bucket: aws.String("test-bucket"),
					Key:    aws.String("missing-key"),
				}).Return(nil, &types.NoSuchKey{}).Times(1)
				return m
			},
			expectedExists: &ObjectExistsResponse{
				Exists: false,
			},
			expectedError: nil,
		},
		{
			name: "Error",
			req: GetFileRequest{
				Bucket: "test-bucket",
				Key:    "error-key",
			},
			mockSetup: func(ctrl *gomock.Controller) S3ClientAPI {
				m := NewMockS3ClientAPI(ctrl)
				m.EXPECT().HeadObject(context.Background(), &s3.HeadObjectInput{
					Bucket: aws.String("test-bucket"),
					Key:    aws.String("error-key"),
				}).Return(nil, errors.New("some error")).Times(1)
				return m
			},
			expectedExists: nil,
			expectedError:  goaws.NewInternalError(errors.New("s.svc.HeadObject: some error")),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			mockSvc := tt.mockSetup(ctrl)
			s := &S3{svc: mockSvc}

			exists, err := s.CheckIfObjectExists(context.Background(), tt.req)

			if tt.expectedError != nil {
				require.Error(t, err)
				assert.EqualError(t, tt.expectedError, err.Error())
				assert.Implements(t, (*goaws.AwsError)(nil), err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expectedExists, exists, "Expected existing: %v, got: %v", tt.expectedExists, exists)
			}
		})
	}
}

func TestS3_UploadFile(t *testing.T) {
	tests := []struct {
		name          string
		req           UploadFileRequest
		mockSetup     func(ctrl *gomock.Controller) S3ClientAPI
		expectedResp  *UploadFileResponse
		expectedError error
	}{
		{
			name: "Success",
			req: UploadFileRequest{
				Bucket: "test-bucket",
				Key:    "test-key",
				File:   bytes.NewReader([]byte("content")),
			},
			mockSetup: func(ctrl *gomock.Controller) S3ClientAPI {
				m := NewMockS3ClientAPI(ctrl)
				m.EXPECT().PutObject(context.Background(), &s3.PutObjectInput{
					Bucket: aws.String("test-bucket"),
					Key:    aws.String("test-key"),
					Body:   bytes.NewReader([]byte("content")),
				}).Return(&s3.PutObjectOutput{
					VersionId: aws.String("v1"),
				}, nil).Times(1)
				return m
			},
			expectedResp: &UploadFileResponse{
				VersionID: "v1",
			},
			expectedError: nil,
		},
		{
			name: "Error",
			req: UploadFileRequest{
				Bucket: "test-bucket",
				Key:    "test-key",
				File:   bytes.NewReader([]byte("content")),
			},
			mockSetup: func(ctrl *gomock.Controller) S3ClientAPI {
				m := NewMockS3ClientAPI(ctrl)
				m.EXPECT().PutObject(context.Background(), &s3.PutObjectInput{
					Bucket: aws.String("test-bucket"),
					Key:    aws.String("test-key"),
					Body:   bytes.NewReader([]byte("content")),
				}).Return(nil, errors.New("upload fail")).Times(1)
				return m
			},
			expectedResp:  nil,
			expectedError: goaws.NewInternalError(errors.New("s.svc.PutObject: upload fail")),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			mockSvc := tt.mockSetup(ctrl)
			s := &S3{svc: mockSvc}

			res, err := s.UploadFile(context.Background(), tt.req)

			if tt.expectedError != nil {
				require.Error(t, err)
				assert.EqualError(t, tt.expectedError, err.Error())
				assert.Implements(t, (*goaws.AwsError)(nil), err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expectedResp, res)
			}
		})
	}
}

func TestS3_DeleteFile(t *testing.T) {
	tests := []struct {
		name          string
		bucket        string
		key           string
		versionId     *string
		mockSetup     func(ctrl *gomock.Controller) S3ClientAPI
		expectedError error
	}{
		{
			name:   "Success",
			bucket: "test-bucket",
			key:    "test-key",
			mockSetup: func(ctrl *gomock.Controller) S3ClientAPI {
				m := NewMockS3ClientAPI(ctrl)
				m.EXPECT().DeleteObject(context.Background(), &s3.DeleteObjectInput{
					Bucket: aws.String("test-bucket"),
					Key:    aws.String("test-key"),
				}).Return(&s3.DeleteObjectOutput{}, nil).Times(1)
				return m
			},
			expectedError: nil,
		},
		{
			name:   "Error",
			bucket: "test-bucket",
			key:    "test-key",
			mockSetup: func(ctrl *gomock.Controller) S3ClientAPI {
				m := NewMockS3ClientAPI(ctrl)
				m.EXPECT().DeleteObject(context.Background(), &s3.DeleteObjectInput{
					Bucket: aws.String("test-bucket"),
					Key:    aws.String("test-key"),
				}).Return(nil, errors.New("delete fail")).Times(1)
				return m
			},
			expectedError: goaws.NewInternalError(errors.New("s.svc.DeleteObject: delete fail")),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			mockSvc := tt.mockSetup(ctrl)
			s := &S3{svc: mockSvc}

			err := s.DeleteFile(context.Background(), tt.bucket, tt.key, tt.versionId)

			if tt.expectedError != nil {
				require.Error(t, err)
				assert.EqualError(t, tt.expectedError, err.Error())
				assert.Implements(t, (*goaws.AwsError)(nil), err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestS3_GetPresignedURL(t *testing.T) {
	tests := []struct {
		name          string
		req           GetPresignedUrlRequest
		mockSetup     func(ctrl *gomock.Controller) S3PresignClientAPI
		expectedResp  *GetPresignedUrlResponse
		expectedError error
	}{
		{
			name: "PutRequest",
			req: GetPresignedUrlRequest{
				Put: &UploadFileRequest{
					Bucket: "test-bucket",
					Key:    "test-key",
					File:   bytes.NewReader([]byte("content")),
				},
				ExpirySeconds: 3600,
			},
			mockSetup: func(ctrl *gomock.Controller) S3PresignClientAPI {
				m := NewMockS3PresignClientAPI(ctrl)
				m.EXPECT().PresignPutObject(context.Background(), &s3.PutObjectInput{
					Bucket: aws.String("test-bucket"),
					Key:    aws.String("test-key"),
					Body:   bytes.NewReader([]byte("content")),
				},
					gomock.Any(),
				).Return(&v4.PresignedHTTPRequest{
					URL: "https://test-bucket.s3.amazonaws.com/test-key?signature=xyz",
				}, nil).Times(1)
				return m
			},
			expectedResp: &GetPresignedUrlResponse{
				PutUrl: "https://test-bucket.s3.amazonaws.com/test-key?signature=xyz",
			},
			expectedError: nil,
		},
		{
			name: "GetRequest",
			req: GetPresignedUrlRequest{
				ExpirySeconds: 3600,
				Get: &GetFileRequest{
					Bucket: "test-bucket",
					Key:    "test-key",
				},
			},
			mockSetup: func(ctrl *gomock.Controller) S3PresignClientAPI {
				m := NewMockS3PresignClientAPI(ctrl)
				m.EXPECT().PresignGetObject(context.Background(), &s3.GetObjectInput{
					Bucket: aws.String("test-bucket"),
					Key:    aws.String("test-key"),
				},
					gomock.Any(),
				).Return(&v4.PresignedHTTPRequest{
					URL: "https://test-bucket.s3.amazonaws.com/test-key?signature=abc",
				}, nil).Times(1)
				return m
			},
			expectedResp: &GetPresignedUrlResponse{
				GetUrl: "https://test-bucket.s3.amazonaws.com/test-key?signature=abc",
			},
			expectedError: nil,
		},
		{
			name: "Error",
			req: GetPresignedUrlRequest{
				ExpirySeconds: 3600,
				Get: &GetFileRequest{
					Bucket: "test-bucket",
					Key:    "test-key",
				},
			},
			mockSetup: func(ctrl *gomock.Controller) S3PresignClientAPI {
				m := NewMockS3PresignClientAPI(ctrl)
				m.EXPECT().PresignGetObject(context.Background(), &s3.GetObjectInput{
					Bucket: aws.String("test-bucket"),
					Key:    aws.String("test-key"),
				},
					gomock.Any(),
				).Return(nil, errors.New("presign fail")).Times(1)
				return m
			},
			expectedResp:  nil,
			expectedError: goaws.NewInternalError(errors.New("psCli.PresignGetObject: presign fail")),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			mockPresign := tt.mockSetup(ctrl)
			s := &S3{presignSvc: mockPresign}

			res, err := s.GetPresignedURL(context.Background(), tt.req)

			if tt.expectedError != nil {
				require.Error(t, err)
				assert.EqualError(t, tt.expectedError, err.Error())
				assert.Implements(t, (*goaws.AwsError)(nil), err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expectedResp, res)
			}
		})
	}
}
