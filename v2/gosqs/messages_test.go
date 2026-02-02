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

func TestNewSQSMessages(t *testing.T) {
	cfg, err := goaws.NewDefaultConfig(context.Background())
	if err != nil {
		t.Errorf("goaws.NewDefaultConfig: %v", err)
		return
	}

	require.NotNil(t, cfg)

	svc := sqs.New(sqs.Options{
		Credentials: cfg.Config.Credentials,
		Region:      cfg.Config.Region,
	})

	messages := NewMessages(svc)
	assert.NotNil(t, messages)
	assert.NotNil(t, messages.svc)
	assert.Implements(t, (*MessagesLogic)(nil), messages)
}

func TestSQSMessages_SendMessage(t *testing.T) {
	tests := []struct {
		name          string
		opts          SendMsgOptions
		mockSetup     func(ctrl *gomock.Controller) SQSMessagesClientAPI
		expectedResp  *SendMsgResponse
		expectedError error
	}{
		{
			name: "Success",
			opts: SendMsgOptions{
				QueueURL:    "https://sqs.us-east-1.amazonaws.com/123456789012/test-queue",
				MessageBody: "hello world",
			},
			mockSetup: func(ctrl *gomock.Controller) SQSMessagesClientAPI {
				m := NewMockSQSMessagesClientAPI(ctrl)
				m.EXPECT().SendMessage(gomock.Any(), gomock.Any(), gomock.Any()).Return(&sqs.SendMessageOutput{
					MessageId: aws.String("msg-id-123"),
				}, nil).Times(1)
				return m
			},
			expectedResp: &SendMsgResponse{
				MessageId: "msg-id-123",
			},
			expectedError: nil,
		},
		{
			name: "QueueDoesNotExist",
			opts: SendMsgOptions{
				QueueURL:    "https://sqs.us-east-1.amazonaws.com/123456789012/missing-queue",
				MessageBody: "hello world",
			},
			mockSetup: func(ctrl *gomock.Controller) SQSMessagesClientAPI {
				m := NewMockSQSMessagesClientAPI(ctrl)
				m.EXPECT().SendMessage(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, &types.QueueDoesNotExist{}).Times(1)
				return m
			},
			expectedResp:  nil,
			expectedError: NewQueueNotFoundError("https://sqs.us-east-1.amazonaws.com/123456789012/missing-queue"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockSvc := tt.mockSetup(ctrl)
			s := &Messages{svc: mockSvc}

			res, err := s.SendMessage(context.Background(), tt.opts)

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

func TestSQSMessages_ReceiveMessage(t *testing.T) {
	tests := []struct {
		name          string
		opts          RecMsgOptions
		mockSetup     func(ctrl *gomock.Controller) SQSMessagesClientAPI
		expectedMsgs  []*Message
		expectedError error
	}{
		{
			name: "Success",
			opts: RecMsgOptions{
				QueueURL: "https://sqs.us-east-1.amazonaws.com/123456789012/test-queue",
			},
			mockSetup: func(ctrl *gomock.Controller) SQSMessagesClientAPI {
				m := NewMockSQSMessagesClientAPI(ctrl)
				m.EXPECT().ReceiveMessage(gomock.Any(), gomock.Any(), gomock.Any()).Return(&sqs.ReceiveMessageOutput{
					Messages: []types.Message{
						{
							Body:      aws.String("hello world"),
							MessageId: aws.String("msg-id-123"),
						},
					},
				}, nil).Times(1)
				return m
			},
			expectedMsgs: []*Message{
				{
					Body:                    "hello world",
					MessageId:               "msg-id-123",
					Attributes:              map[string]string{},
					MessageAttributes:       map[string]MsgAV{},
					MD5OfBody:               "",
					ReceiptHandle:           "",
					MD5OfMessagefAttributes: "",
				},
			},
			expectedError: nil,
		},
		{
			name: "Error",
			opts: RecMsgOptions{
				QueueURL: "https://sqs.us-east-1.amazonaws.com/123456789012/test-queue",
			},
			mockSetup: func(ctrl *gomock.Controller) SQSMessagesClientAPI {
				m := NewMockSQSMessagesClientAPI(ctrl)
				m.EXPECT().ReceiveMessage(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, errors.New("receive error")).Times(1)
				return m
			},
			expectedMsgs:  []*Message{},
			expectedError: goaws.NewInternalError(errors.New("s.svc.ReceiveMessage: receive error")),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockSvc := tt.mockSetup(ctrl)
			s := &Messages{svc: mockSvc}

			msgs, err := s.ReceiveMessage(context.Background(), tt.opts)

			if tt.expectedError != nil {
				require.Error(t, err)
				assert.EqualError(t, err, tt.expectedError.Error())
				assert.Implements(t, (*goaws.AwsError)(nil), err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, len(tt.expectedMsgs), len(msgs.Messages))
				if len(msgs.Messages) > 0 {
					assert.Equal(t, tt.expectedMsgs[0].Body, msgs.Messages[0].Body)
				}
			}
		})
	}
}

func TestSQSMessages_DeleteMessage(t *testing.T) {
	tests := []struct {
		name          string
		url           string
		handle        string
		mockSetup     func(ctrl *gomock.Controller) SQSMessagesClientAPI
		expectedError error
	}{
		{
			name:   "Success",
			url:    "https://sqs.us-east-1.amazonaws.com/123456789012/test-queue",
			handle: "handle-123",
			mockSetup: func(ctrl *gomock.Controller) SQSMessagesClientAPI {
				m := NewMockSQSMessagesClientAPI(ctrl)
				m.EXPECT().DeleteMessage(gomock.Any(), gomock.Any(), gomock.Any()).Return(&sqs.DeleteMessageOutput{}, nil).Times(1)
				return m
			},
			expectedError: nil,
		},
		{
			name:   "Error",
			url:    "https://sqs.us-east-1.amazonaws.com/123456789012/test-queue",
			handle: "handle-123",
			mockSetup: func(ctrl *gomock.Controller) SQSMessagesClientAPI {
				m := NewMockSQSMessagesClientAPI(ctrl)
				m.EXPECT().DeleteMessage(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, errors.New("delete error")).Times(1)
				return m
			},
			expectedError: goaws.NewInternalError(errors.New("s.svc.DeleteMessage: delete error")),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockSvc := tt.mockSetup(ctrl)
			s := &Messages{svc: mockSvc}

			err := s.DeleteMessage(context.Background(), tt.url, tt.handle)

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

func TestSQSMessages_DeleteMessageBatch(t *testing.T) {
	tests := []struct {
		name          string
		req           DeleteMessageBatchRequest
		mockSetup     func(ctrl *gomock.Controller) SQSMessagesClientAPI
		expectedResp  *DeleteMessageBatchResponse
		expectedError error
	}{
		{
			name: "Success",
			req: DeleteMessageBatchRequest{
				QueueURL:       "https://sqs.us-east-1.amazonaws.com/123456789012/test-queue",
				MessageIDs:     []string{"msg-1"},
				ReceiptHandles: []string{"handle-1"},
			},
			mockSetup: func(ctrl *gomock.Controller) SQSMessagesClientAPI {
				m := NewMockSQSMessagesClientAPI(ctrl)
				m.EXPECT().DeleteMessageBatch(gomock.Any(), gomock.Any(), gomock.Any()).Return(&sqs.DeleteMessageBatchOutput{
					Successful: []types.DeleteMessageBatchResultEntry{
						{Id: aws.String("msg-1")},
					},
				}, nil).Times(1)
				return m
			},
			expectedResp: &DeleteMessageBatchResponse{
				Successful: []BatchDeleteResultEntry{{MessageID: "msg-1"}},
				Failed:     []BatchDeleteErrEntry{},
			},
			expectedError: nil,
		},
		{
			name: "Error",
			req: DeleteMessageBatchRequest{
				QueueURL:       "https://sqs.us-east-1.amazonaws.com/123456789012/test-queue",
				MessageIDs:     []string{"msg-1"},
				ReceiptHandles: []string{"handle-1"},
			},
			mockSetup: func(ctrl *gomock.Controller) SQSMessagesClientAPI {
				m := NewMockSQSMessagesClientAPI(ctrl)
				m.EXPECT().DeleteMessageBatch(gomock.Any(), gomock.Any(), gomock.Any()).Return(&sqs.DeleteMessageBatchOutput{
					Failed: []types.BatchResultErrorEntry{
						{Id: aws.String("msg-1"), Code: aws.String("500"), Message: aws.String("failed")},
					},
				}, errors.New("batch error")).Times(1)
				return m
			},
			expectedResp:  nil,
			expectedError: goaws.NewInternalError(errors.New("s.svc.DeleteMessageBatch: batch error")),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockSvc := tt.mockSetup(ctrl)
			s := &Messages{svc: mockSvc}

			res, err := s.DeleteMessageBatch(context.Background(), tt.req)

			if tt.expectedError != nil {
				require.Error(t, err)
				assert.EqualError(t, err, tt.expectedError.Error())
				assert.Implements(t, (*goaws.AwsError)(nil), err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, len(tt.expectedResp.Successful), len(res.Successful))
			}
		})
	}
}

func TestSQSMessages_ChangeMessageVisibilityBatch(t *testing.T) {
	tests := []struct {
		name          string
		req           BatchUpdateVisibilityTimeoutRequest
		mockSetup     func(ctrl *gomock.Controller) SQSMessagesClientAPI
		expectedResp  *BatchUpdateVisibilityTimeoutResponse
		expectedError error
	}{
		{
			name: "Success",
			req: BatchUpdateVisibilityTimeoutRequest{
				QueueURL:       "https://sqs.us-east-1.amazonaws.com/123456789012/test-queue",
				MessageIDs:     []string{"msg-1"},
				ReceiptHandles: []string{"handle-1"},
				TimeoutSeconds: 30,
			},
			mockSetup: func(ctrl *gomock.Controller) SQSMessagesClientAPI {
				m := NewMockSQSMessagesClientAPI(ctrl)
				m.EXPECT().ChangeMessageVisibilityBatch(gomock.Any(), gomock.Any(), gomock.Any()).Return(&sqs.ChangeMessageVisibilityBatchOutput{
					Successful: []types.ChangeMessageVisibilityBatchResultEntry{
						{Id: aws.String("msg-1")},
					},
				}, nil).Times(1)
				return m
			},
			expectedResp: &BatchUpdateVisibilityTimeoutResponse{
				Successful: []BatchUpdateVisibilityTimeoutEntry{{MessageID: "msg-1"}},
				Failed:     []BatchUpdateVisibilityTimeoutErrEntry{},
			},
			expectedError: nil,
		},
		{
			name: "Error",
			req: BatchUpdateVisibilityTimeoutRequest{
				QueueURL:       "https://sqs.us-east-1.amazonaws.com/123456789012/test-queue",
				MessageIDs:     []string{"msg-1"},
				ReceiptHandles: []string{"handle-1"},
				TimeoutSeconds: 30,
			},
			mockSetup: func(ctrl *gomock.Controller) SQSMessagesClientAPI {
				m := NewMockSQSMessagesClientAPI(ctrl)
				m.EXPECT().ChangeMessageVisibilityBatch(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, errors.New("change visibility error")).Times(1)
				return m
			},
			expectedResp:  nil,
			expectedError: goaws.NewInternalError(errors.New("s.svc.ChangeMessageVisibilityBatch: change visibility error")),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockSvc := tt.mockSetup(ctrl)
			s := &Messages{svc: mockSvc}

			res, err := s.ChangeMessageVisibilityBatch(context.Background(), tt.req)

			if tt.expectedError != nil {
				require.Error(t, err)
				assert.EqualError(t, err, tt.expectedError.Error())
				assert.Implements(t, (*goaws.AwsError)(nil), err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, len(tt.expectedResp.Successful), len(res.Successful))
			}
		})
	}
}
