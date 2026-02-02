package gosqs

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/ggarcia209/go-aws-v2/v2/goaws"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	gomock "go.uber.org/mock/gomock"
)

func TestNewSQSQueues(t *testing.T) {
	cfg, err := goaws.NewDefaultConfig(context.Background())
	if err != nil {
		t.Errorf("goaws.NewDefaultConfig: %v", err)
		return
	}

	require.NotNil(t, cfg)

	// test interface implementation
	svc := sqs.New(sqs.Options{
		Credentials: cfg.Config.Credentials,
		Region:      cfg.Config.Region,
	})
	sqs := NewQueues(svc)
	assert.NotNil(t, sqs)
	assert.NotNil(t, sqs.svc)
	assert.Implements(t, (*QueuesLogic)(nil), sqs)
}

func TestSQSQueues_CreateQueue(t *testing.T) {
	tests := []struct {
		name          string
		queueName     string
		opts          QueueOptions
		tags          map[string]string
		mockSetup     func(ctrl *gomock.Controller) SQSQueuesClientAPI
		expectedResp  *CreateQueueResponse
		expectedError error
	}{
		{
			name:      "Success",
			queueName: "test-queue",
			opts:      QueueOptions{},
			tags:      map[string]string{"env": "test"},
			mockSetup: func(ctrl *gomock.Controller) SQSQueuesClientAPI {
				m := NewMockSQSQueuesClientAPI(ctrl)
				m.EXPECT().CreateQueue(gomock.Any(), gomock.Any(), gomock.Any()).Return(&sqs.CreateQueueOutput{
					QueueUrl: aws.String("https://sqs.us-east-1.amazonaws.com/123456789012/test-queue"),
				}, nil).Times(1)
				return m
			},
			expectedResp: &CreateQueueResponse{
				QueueUrl: "https://sqs.us-east-1.amazonaws.com/123456789012/test-queue",
			},
			expectedError: nil,
		},
		{
			name:      "Error",
			queueName: "test-queue",
			opts:      QueueOptions{},
			tags:      nil,
			mockSetup: func(ctrl *gomock.Controller) SQSQueuesClientAPI {
				m := NewMockSQSQueuesClientAPI(ctrl)
				m.EXPECT().CreateQueue(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, errors.New("create error")).Times(1)
				return m
			},
			expectedResp:  nil,
			expectedError: goaws.NewInternalError(errors.New("s.svc.CreateQueue: create error")),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockSvc := tt.mockSetup(ctrl)
			s := &Queues{svc: mockSvc}

			res, err := s.CreateQueue(context.Background(), tt.queueName, tt.opts, tt.tags)

			if tt.expectedError != nil {
				require.Error(t, err)
				assert.EqualError(t, err, tt.expectedError.Error())
				assert.Implements(t, (*goaws.AwsError)(nil), err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expectedResp, res)
			}
		})
	}
}

func TestSQSQueues_GetQueueURL(t *testing.T) {
	tests := []struct {
		name          string
		queueName     string
		mockSetup     func(ctrl *gomock.Controller) SQSQueuesClientAPI
		expectedResp  *GetQueueUrlResponse
		expectedError error
	}{
		{
			name:      "Success",
			queueName: "test-queue",
			mockSetup: func(ctrl *gomock.Controller) SQSQueuesClientAPI {
				m := NewMockSQSQueuesClientAPI(ctrl)
				m.EXPECT().GetQueueUrl(gomock.Any(), gomock.Any(), gomock.Any()).Return(&sqs.GetQueueUrlOutput{
					QueueUrl: aws.String("https://sqs.us-east-1.amazonaws.com/123456789012/test-queue"),
				}, nil).Times(1)
				return m
			},
			expectedResp: &GetQueueUrlResponse{
				QueueUrl: "https://sqs.us-east-1.amazonaws.com/123456789012/test-queue",
			},
			expectedError: nil,
		},
		{
			name:      "QueueDoesNotExist",
			queueName: "missing-queue",
			mockSetup: func(ctrl *gomock.Controller) SQSQueuesClientAPI {
				m := NewMockSQSQueuesClientAPI(ctrl)
				m.EXPECT().GetQueueUrl(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, &types.QueueDoesNotExist{}).Times(1)
				return m
			},
			expectedResp:  nil,
			expectedError: NewQueueNotFoundError("missing-queue"),
		},
		{
			name:      "Error",
			queueName: "test-queue",
			mockSetup: func(ctrl *gomock.Controller) SQSQueuesClientAPI {
				m := NewMockSQSQueuesClientAPI(ctrl)
				m.EXPECT().GetQueueUrl(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, errors.New("get error")).Times(1)
				return m
			},
			expectedResp:  nil,
			expectedError: goaws.NewInternalError(errors.New("s.svc.GetQueueUrl: get error")),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockSvc := tt.mockSetup(ctrl)
			s := &Queues{svc: mockSvc}

			res, err := s.GetQueueURL(context.Background(), tt.queueName)

			if tt.expectedError != nil {
				require.Error(t, err)
				assert.EqualError(t, err, tt.expectedError.Error())
				assert.Implements(t, (*goaws.AwsError)(nil), err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expectedResp, res)
			}
		})
	}
}

func TestSQSQueues_DeleteQueue(t *testing.T) {
	tests := []struct {
		name          string
		url           string
		mockSetup     func(ctrl *gomock.Controller) SQSQueuesClientAPI
		expectedError error
	}{
		{
			name: "Success",
			url:  "https://sqs.us-east-1.amazonaws.com/123456789012/test-queue",
			mockSetup: func(ctrl *gomock.Controller) SQSQueuesClientAPI {
				m := NewMockSQSQueuesClientAPI(ctrl)
				m.EXPECT().DeleteQueue(gomock.Any(), gomock.Any(), gomock.Any()).Return(&sqs.DeleteQueueOutput{}, nil).Times(1)
				return m
			},
			expectedError: nil,
		},
		{
			name: "QueueDoesNotExist",
			url:  "https://sqs.us-east-1.amazonaws.com/123456789012/missing-queue",
			mockSetup: func(ctrl *gomock.Controller) SQSQueuesClientAPI {
				m := NewMockSQSQueuesClientAPI(ctrl)
				m.EXPECT().DeleteQueue(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, &types.QueueDoesNotExist{}).Times(1)
				return m
			},
			expectedError: NewQueueNotFoundError("https://sqs.us-east-1.amazonaws.com/123456789012/missing-queue"),
		},
		{
			name: "Error",
			url:  "https://sqs.us-east-1.amazonaws.com/123456789012/test-queue",
			mockSetup: func(ctrl *gomock.Controller) SQSQueuesClientAPI {
				m := NewMockSQSQueuesClientAPI(ctrl)
				m.EXPECT().DeleteQueue(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, errors.New("delete error")).Times(1)
				return m
			},
			expectedError: goaws.NewInternalError(errors.New("s.svc.DeleteQueue: delete error")),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockSvc := tt.mockSetup(ctrl)
			s := &Queues{svc: mockSvc}

			err := s.DeleteQueue(context.Background(), tt.url)

			if tt.expectedError != nil {
				require.Error(t, err)
				assert.EqualError(t, err, tt.expectedError.Error())
				assert.Implements(t, (*goaws.AwsError)(nil), err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestSQSQueues_PurgeQueue(t *testing.T) {
	tests := []struct {
		name          string
		url           string
		mockSetup     func(ctrl *gomock.Controller) SQSQueuesClientAPI
		expectedError error
	}{
		{
			name: "Success",
			url:  "https://sqs.us-east-1.amazonaws.com/123456789012/test-queue",
			mockSetup: func(ctrl *gomock.Controller) SQSQueuesClientAPI {
				m := NewMockSQSQueuesClientAPI(ctrl)
				m.EXPECT().PurgeQueue(gomock.Any(), gomock.Any(), gomock.Any()).Return(&sqs.PurgeQueueOutput{}, nil).Times(1)
				return m
			},
			expectedError: nil,
		},
		{
			name: "QueueDoesNotExist",
			url:  "https://sqs.us-east-1.amazonaws.com/123456789012/missing-queue",
			mockSetup: func(ctrl *gomock.Controller) SQSQueuesClientAPI {
				m := NewMockSQSQueuesClientAPI(ctrl)
				m.EXPECT().PurgeQueue(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, &types.QueueDoesNotExist{}).Times(1)
				return m
			},
			expectedError: NewQueueNotFoundError("https://sqs.us-east-1.amazonaws.com/123456789012/missing-queue"),
		},
		{
			name: "Error",
			url:  "https://sqs.us-east-1.amazonaws.com/123456789012/test-queue",
			mockSetup: func(ctrl *gomock.Controller) SQSQueuesClientAPI {
				m := NewMockSQSQueuesClientAPI(ctrl)
				m.EXPECT().PurgeQueue(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, errors.New("purge error")).Times(1)
				return m
			},
			expectedError: goaws.NewInternalError(errors.New("s.svc.PurgeQueue: purge error")),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockSvc := tt.mockSetup(ctrl)
			s := &Queues{svc: mockSvc}

			err := s.PurgeQueue(context.Background(), tt.url)

			if tt.expectedError != nil {
				require.Error(t, err)
				assert.EqualError(t, err, tt.expectedError.Error())
				assert.Implements(t, (*goaws.AwsError)(nil), err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
