package goses

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sesv2"
	"github.com/aws/aws-sdk-go-v2/service/sesv2/types"
	"github.com/ggarcia209/go-aws-v2/v2/goaws"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	gomock "go.uber.org/mock/gomock"
)

func TestNewSES(t *testing.T) {
	cfg, err := goaws.NewDefaultConfig(context.Background())
	if err != nil {
		t.Errorf("goaws.NewDefaultConfig: %v", err)
		return
	}

	require.NotNil(t, cfg)

	// test interface implementation
	ses := NewSES(*cfg)
	assert.NotNil(t, ses)
	assert.NotNil(t, ses.svc)
	assert.Implements(t, (*SESLogic)(nil), ses)
}

func TestSES_ListEmailIdentities(t *testing.T) {
	tests := []struct {
		name               string
		mockSetup          func(ctrl *gomock.Controller) SESClientAPI
		expectedIdentities []string
		expectedError      error
	}{
		{
			name: "Success",
			mockSetup: func(ctrl *gomock.Controller) SESClientAPI {
				mockSvc := NewMockSESClientAPI(ctrl)
				mockSvc.EXPECT().ListEmailIdentities(gomock.Any(), gomock.Any()).Return(&sesv2.ListEmailIdentitiesOutput{
					EmailIdentities: []types.IdentityInfo{{
						IdentityName:       aws.String("test-identity"),
						VerificationStatus: types.VerificationStatusSuccess,
					}},
				}, nil).Times(1)
				return mockSvc
			},
			expectedIdentities: []string{"test-identity"},
			expectedError:      nil,
		},
		{
			name: "Success - with unverified identity",
			mockSetup: func(ctrl *gomock.Controller) SESClientAPI {
				mockSvc := NewMockSESClientAPI(ctrl)
				mockSvc.EXPECT().ListEmailIdentities(gomock.Any(), gomock.Any()).Return(&sesv2.ListEmailIdentitiesOutput{
					EmailIdentities: []types.IdentityInfo{{
						IdentityName:       aws.String("test-identity"),
						VerificationStatus: types.VerificationStatusPending,
					}},
				}, nil).Times(1)
				return mockSvc
			},
			expectedIdentities: []string{},
			expectedError:      nil,
		},
		{
			name: "error",
			mockSetup: func(ctrl *gomock.Controller) SESClientAPI {
				mockSvc := NewMockSESClientAPI(ctrl)
				mockSvc.EXPECT().ListEmailIdentities(gomock.Any(), gomock.Any()).Return(nil, errors.New("email failed")).Times(1)
				return mockSvc
			},
			expectedIdentities: []string{},
			expectedError:      goaws.NewInternalError(errors.New("s.svc.ListEmailIdentities: email failed")),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockSvc := tt.mockSetup(ctrl)
			s := &SES{svc: mockSvc}

			res, err := s.ListVerifiedIdentities(context.Background())

			if tt.expectedError != nil {
				require.Error(t, err)
				assert.EqualError(t, err, tt.expectedError.Error())
				assert.Implements(t, (*goaws.AwsError)(nil), err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expectedIdentities, res.EmailAddresses)
			}
		})
	}
}

func TestSES_SendEmail(t *testing.T) {
	tests := []struct {
		name          string
		params        SendEmailParams
		mockSetup     func(ctrl *gomock.Controller) SESClientAPI
		expectedError error
	}{
		{
			name: "Success",
			params: SendEmailParams{
				Subject:     "help me spend my money",
				From:        "thelastprinceofnigeria@gmail.com",
				To:          []string{"chooch@gmail.com"},
				TextBody:    "give me your bitcoin keys and I will send you money",
				HtmlBody:    "give me your bitcoin keys and I will send you money",
				ConfigSet:   "test-config",
				Attachments: []Attachment{},
			},
			mockSetup: func(ctrl *gomock.Controller) SESClientAPI {
				mockSvc := NewMockSESClientAPI(ctrl)
				mockSvc.EXPECT().SendEmail(gomock.Any(), gomock.Any()).Return(&sesv2.SendEmailOutput{}, nil).Times(1)
				return mockSvc
			},
			expectedError: nil,
		},
		{
			name: "Success - with attachments",
			params: SendEmailParams{
				Subject:  "Test with attachments",
				From:     "sender@example.com",
				To:       []string{"recipient@example.com"},
				TextBody: "This email has attachments",
				HtmlBody: "<p>This email has attachments</p>",
				Attachments: []Attachment{
					{
						FileName:    "attachment1.txt",
						Data:        []byte("attachment1 content"),
						ContentType: aws.String("text/plain"),
					},
					{
						FileName:    "attachment2.txt",
						Data:        []byte("attachment2 content"),
						ContentType: aws.String("text/plain"),
					},
				},
			},
			mockSetup: func(ctrl *gomock.Controller) SESClientAPI {
				mockSvc := NewMockSESClientAPI(ctrl)
				// Custom matcher to verify attachments are included
				mockSvc.EXPECT().SendEmail(gomock.Any(), gomock.Any()).DoAndReturn(
					func(ctx context.Context, input *sesv2.SendEmailInput, optFns ...func(*sesv2.Options)) (*sesv2.SendEmailOutput, error) {
						// Verify attachments are present
						if input.Content == nil || input.Content.Simple == nil {
							return nil, errors.New("content or simple message is nil")
						}
						attachments := input.Content.Simple.Attachments
						if len(attachments) != 2 {
							return nil, errors.New("expected 2 attachments")
						}
						// Verify attachment content
						if *attachments[0].FileName != "attachment1.txt" {
							return nil, errors.New("attachment 1 filename mismatch")
						}
						if string(attachments[0].RawContent) != "attachment1 content" {
							return nil, errors.New("attachment 1 content mismatch")
						}
						if *attachments[0].ContentType != "text/plain" {
							return nil, errors.New("attachment 1 content type mismatch")
						}

						if *attachments[1].FileName != "attachment2.txt" {
							return nil, errors.New("attachment 2 filename mismatch")
						}
						if string(attachments[1].RawContent) != "attachment2 content" {
							return nil, errors.New("attachment 2 content mismatch")
						}
						if *attachments[1].ContentType != "text/plain" {
							return nil, errors.New("attachment 2 content type mismatch")
						}

						return &sesv2.SendEmailOutput{}, nil
					},
				).Times(1)
				return mockSvc
			},
			expectedError: nil,
		},
		{
			name: "error - invalid recipient",
			mockSetup: func(ctrl *gomock.Controller) SESClientAPI {
				return NewMockSESClientAPI(ctrl)
			},
			expectedError: NewInvalidRecipientError(),
		},
		{
			name: "error - message rejected",
			params: SendEmailParams{
				Subject:     "help me spend my money",
				From:        "thelastprinceofnigeria@gmail.com",
				To:          []string{"chooch@gmail.com"},
				TextBody:    "give me your bitcoin keys and I will send you money",
				HtmlBody:    "give me your bitcoin keys and I will send you money",
				ConfigSet:   "test-config",
				Attachments: []Attachment{{}, {}},
			},
			mockSetup: func(ctrl *gomock.Controller) SESClientAPI {
				mockSvc := NewMockSESClientAPI(ctrl)
				mockSvc.EXPECT().SendEmail(gomock.Any(), gomock.Any()).Return(nil, &types.MessageRejected{Message: aws.String("message rejected by server")}).Times(1)
				return mockSvc
			},
			expectedError: goaws.NewInternalError(errors.New("s.svc.SendEmail: message rejected by server")),
		},
		{
			name: "error - unverified domaind",
			params: SendEmailParams{
				Subject:     "help me spend my money",
				From:        "thelastprinceofnigeria@gmail.com",
				To:          []string{"chooch@gmail.com"},
				TextBody:    "give me your bitcoin keys and I will send you money",
				HtmlBody:    "give me your bitcoin keys and I will send you money",
				ConfigSet:   "test-config",
				Attachments: []Attachment{{}, {}},
			},
			mockSetup: func(ctrl *gomock.Controller) SESClientAPI {
				mockSvc := NewMockSESClientAPI(ctrl)
				mockSvc.EXPECT().SendEmail(gomock.Any(), gomock.Any()).Return(nil, &types.MailFromDomainNotVerifiedException{Message: aws.String("domain not verified")}).Times(1)
				return mockSvc
			},
			expectedError: NewUnverifiedDomainError("domain not verified"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockSvc := tt.mockSetup(ctrl)
			s := &SES{svc: mockSvc}

			err := s.SendEmail(context.Background(), tt.params)

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
