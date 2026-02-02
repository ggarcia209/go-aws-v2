package gosns

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sns/types"
	"github.com/ggarcia209/go-aws-v2/v2/goaws"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	gomock "go.uber.org/mock/gomock"
)

func TestNewSNS(t *testing.T) {
	cfg, err := goaws.NewDefaultConfig(context.Background())
	if err != nil {
		t.Errorf("goaws.NewDefaultConfig: %v", err)
		return
	}

	require.NotNil(t, cfg)

	// test interface implementation
	sns := NewSNS(*cfg)
	assert.NotNil(t, sns)
	assert.NotNil(t, sns.svc)
	assert.Implements(t, (*SNSLogic)(nil), sns)
}

func TestSNS_ListTopics(t *testing.T) {
	tests := []struct {
		name          string
		mockSetup     func(ctrl *gomock.Controller) SNSClientAPI
		expectedArns  []string
		expectedError error
	}{
		{
			name: "Success",
			mockSetup: func(ctrl *gomock.Controller) SNSClientAPI {
				m := NewMockSNSClientAPI(ctrl)
				m.EXPECT().ListTopics(gomock.Any(), gomock.Any(), gomock.Any()).Return(&sns.ListTopicsOutput{
					Topics: []types.Topic{
						{TopicArn: aws.String("arn:aws:sns:us-east-1:123456789012:MyTopic")},
					},
				}, nil).Times(1)
				return m
			},
			expectedArns:  []string{"arn:aws:sns:us-east-1:123456789012:MyTopic"},
			expectedError: nil,
		},
		{
			name: "Empty",
			mockSetup: func(ctrl *gomock.Controller) SNSClientAPI {
				m := NewMockSNSClientAPI(ctrl)
				m.EXPECT().ListTopics(gomock.Any(), gomock.Any(), gomock.Any()).Return(&sns.ListTopicsOutput{
					Topics: []types.Topic{},
				}, nil).Times(1)
				return m
			},
			expectedArns:  []string{},
			expectedError: nil,
		},
		{
			name: "Error",
			mockSetup: func(ctrl *gomock.Controller) SNSClientAPI {
				m := NewMockSNSClientAPI(ctrl)
				m.EXPECT().ListTopics(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, errors.New("aws error")).Times(1)
				return m
			},
			expectedArns:  []string{},
			expectedError: goaws.NewInternalError(errors.New("s.svc.ListTopics: aws error")),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockSvc := tt.mockSetup(ctrl)
			s := &SNS{svc: mockSvc}

			arns, err := s.ListTopics(context.Background())

			if tt.expectedError != nil {
				require.Error(t, err)
				assert.EqualError(t, err, tt.expectedError.Error())
				assert.Implements(t, (*goaws.AwsError)(nil), err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expectedArns, arns.TopicArns)
			}
		})
	}
}

func TestSNS_CreateTopic(t *testing.T) {
	tests := []struct {
		name          string
		topicName     string
		mockSetup     func(*gomock.Controller) SNSClientAPI
		expectedArn   string
		expectedError error
	}{
		{
			name:      "Success",
			topicName: "MyNewTopic",
			mockSetup: func(ctrl *gomock.Controller) SNSClientAPI {
				m := NewMockSNSClientAPI(ctrl)
				m.EXPECT().CreateTopic(gomock.Any(), gomock.Any(), gomock.Any()).Return(&sns.CreateTopicOutput{
					TopicArn: aws.String("arn:aws:sns:us-east-1:123456789012:MyNewTopic"),
				}, nil).Times(1)
				return m
			},
			expectedArn:   "arn:aws:sns:us-east-1:123456789012:MyNewTopic",
			expectedError: nil,
		},
		{
			name:      "Error",
			topicName: "MyNewTopic",
			mockSetup: func(ctrl *gomock.Controller) SNSClientAPI {
				m := NewMockSNSClientAPI(ctrl)
				m.EXPECT().CreateTopic(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, errors.New("create error")).Times(1)
				return m
			},
			expectedArn:   "arn:aws:sns:us-east-1:123456789012:MyNewTopic",
			expectedError: goaws.NewInternalError(errors.New("s.svc.CreateTopic: create error")),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockSvc := tt.mockSetup(ctrl)
			s := &SNS{svc: mockSvc}

			arn, err := s.CreateTopic(context.Background(), tt.topicName)

			if tt.expectedError != nil {
				require.Error(t, err)
				assert.EqualError(t, err, tt.expectedError.Error())
				assert.Implements(t, (*goaws.AwsError)(nil), err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expectedArn, arn.TopicArn)
			}
		})
	}
}

func TestSNS_Subscribe(t *testing.T) {
	tests := []struct {
		name          string
		endpoint      string
		protocol      string
		topicArn      string
		mockSetup     func(*gomock.Controller) SNSClientAPI
		expectedArn   string
		expectedError error
	}{
		{
			name:     "Success",
			endpoint: "test@example.com",
			protocol: "email",
			topicArn: "arn:aws:sns:us-east-1:123456789012:MyTopic",
			mockSetup: func(ctrl *gomock.Controller) SNSClientAPI {
				m := NewMockSNSClientAPI(ctrl)
				m.EXPECT().Subscribe(gomock.Any(), gomock.Any(), gomock.Any()).Return(&sns.SubscribeOutput{
					SubscriptionArn: aws.String("arn:aws:sns:us-east-1:123456789012:MyTopic:subscription-id"),
				}, nil).Times(1)
				return m
			},
			expectedArn:   "arn:aws:sns:us-east-1:123456789012:MyTopic:subscription-id",
			expectedError: nil,
		},
		{
			name:     "InvalidProtocol",
			endpoint: "test",
			protocol: "invalid",
			topicArn: "arn",
			mockSetup: func(ctrl *gomock.Controller) SNSClientAPI {
				return NewMockSNSClientAPI(ctrl)
			},
			expectedArn:   "",
			expectedError: NewInvalidProtocolError("invalid"),
		},
		{
			name:     "Error",
			endpoint: "test@example.com",
			protocol: "email",
			topicArn: "arn:aws:sns:us-east-1:123456789012:MyTopic",
			mockSetup: func(ctrl *gomock.Controller) SNSClientAPI {
				m := NewMockSNSClientAPI(ctrl)
				m.EXPECT().Subscribe(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, errors.New("subscribe error")).Times(1)
				return m
			},
			expectedArn:   "",
			expectedError: goaws.NewInternalError(errors.New("s.svc.Subscribe: subscribe error")),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockSvc := tt.mockSetup(ctrl)
			s := &SNS{svc: mockSvc}

			arn, err := s.Subscribe(context.Background(), tt.endpoint, tt.protocol, tt.topicArn)
			if tt.expectedError != nil {
				require.Error(t, err)
				assert.EqualError(t, err, tt.expectedError.Error())
				assert.Implements(t, (*goaws.AwsError)(nil), err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expectedArn, arn.SubscriptionArn)
			}
		})
	}
}

func TestSNS_Publish(t *testing.T) {
	tests := []struct {
		name          string
		msgStr        string
		topicArn      string
		mockSetup     func(*gomock.Controller) SNSClientAPI
		expectedId    string
		expectedError error
	}{
		{
			name:     "Success",
			msgStr:   "hello world",
			topicArn: "arn:aws:sns:us-east-1:123456789012:MyTopic",
			mockSetup: func(ctrl *gomock.Controller) SNSClientAPI {
				m := NewMockSNSClientAPI(ctrl)
				m.EXPECT().Publish(gomock.Any(), gomock.Any(), gomock.Any()).Return(&sns.PublishOutput{
					MessageId: aws.String("msg-id-123"),
				}, nil).Times(1)
				return m
			},
			expectedId:    "msg-id-123",
			expectedError: nil,
		},
		{
			name:     "Error",
			msgStr:   "hello world",
			topicArn: "arn:aws:sns:us-east-1:123456789012:MyTopic",
			mockSetup: func(ctrl *gomock.Controller) SNSClientAPI {
				m := NewMockSNSClientAPI(ctrl)
				m.EXPECT().Publish(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, errors.New("publish error")).Times(1)
				return m
			},
			expectedId:    "",
			expectedError: goaws.NewInternalError(errors.New("s.svc.Publish: publish error")),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockSvc := tt.mockSetup(ctrl)
			s := &SNS{svc: mockSvc}

			id, err := s.Publish(context.Background(), tt.msgStr, tt.topicArn)

			if tt.expectedError != nil {
				require.Error(t, err)
				assert.EqualError(t, err, tt.expectedError.Error())
				assert.Implements(t, (*goaws.AwsError)(nil), err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expectedId, id.MessageId)
			}
		})
	}
}
