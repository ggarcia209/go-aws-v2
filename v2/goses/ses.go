// goses contains common methods for interacting with AWS SES
// and SES event type models
package goses

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/service/sesv2"
	"github.com/aws/aws-sdk-go-v2/service/sesv2/types"

	"github.com/ggarcia209/go-aws-v2/v2/goaws"
)

// CharSet repsents the charset type for email messages (UTF-8)
const CharSet = "UTF-8"

//go:generate mockgen -destination=../mocks/gosesmock/ses.go -package=gosesmock . SESLogic
type SESLogic interface {
	ListVerifiedIdentities(ctx context.Context) (*ListVerifiedIdentitiesResponse, error)
	SendEmail(ctx context.Context, params SendEmailParams) error
}

// SESClientAPI defines the interface for the AWS SES client methods used by this package.
//
//go:generate mockgen -destination=./ses_client_api_test.go -package=goses . SESClientAPI
type SESClientAPI interface {
	ListEmailIdentities(ctx context.Context, params *sesv2.ListEmailIdentitiesInput, optFns ...func(*sesv2.Options)) (*sesv2.ListEmailIdentitiesOutput, error)
	SendEmail(ctx context.Context, params *sesv2.SendEmailInput, optFns ...func(*sesv2.Options)) (*sesv2.SendEmailOutput, error)
}

type SES struct {
	svc SESClientAPI
}

func NewSES(config goaws.AwsConfig) *SES {
	return &SES{
		svc: sesv2.NewFromConfig(config.Config),
	}
}

// ListVerifiedIdentities lists the SES verified email addresses for the account.
func (s *SES) ListVerifiedIdentities(ctx context.Context) (*ListVerifiedIdentitiesResponse, error) {
	var verifiedIds = make([]string, 0)

	result, err := s.svc.ListEmailIdentities(ctx, &sesv2.ListEmailIdentitiesInput{})
	if err != nil {
		return nil, goaws.NewInternalError(fmt.Errorf("s.svc.ListEmailIdentities: %w", err))
	}

	for _, email := range result.EmailIdentities {
		if email.VerificationStatus == types.VerificationStatusSuccess && email.IdentityName != nil {
			verifiedIds = append(verifiedIds, *email.IdentityName)
		}
	}
	return &ListVerifiedIdentitiesResponse{EmailAddresses: verifiedIds}, nil
}

// SendEmail sends a new email message. To and CC addresses are passed as []string, all other fields as strings.
func (s *SES) SendEmail(ctx context.Context, params SendEmailParams) error {
	if len(params.To) == 0 {
		return NewInvalidRecipientError()
	}

	// Assemble the email.
	var htmlContent *types.Content
	if params.HtmlBody != "" {
		htmlContent = &types.Content{
			Charset: aws.String(CharSet),
			Data:    aws.String(params.HtmlBody),
		}
	}

	var configSet *string
	if params.ConfigSet != "" {
		configSet = aws.String(params.ConfigSet)
	}

	var attachements = make([]types.Attachment, 0)
	for _, attachment := range params.Attachments {
		attachements = append(attachements, types.Attachment{
			FileName:    aws.String(attachment.FileName),
			RawContent:  attachment.Data,
			ContentType: attachment.ContentType,
		})
	}

	input := &sesv2.SendEmailInput{
		Destination: &types.Destination{
			CcAddresses: params.Cc,
			ToAddresses: params.To,
		},
		Content: &types.EmailContent{
			Simple: &types.Message{
				Body: &types.Body{
					Html: htmlContent,
					Text: &types.Content{
						Charset: aws.String(CharSet),
						Data:    aws.String(params.TextBody),
					},
				},
				Subject: &types.Content{
					Charset: aws.String(CharSet),
					Data:    aws.String(params.Subject),
				},
				Attachments: attachements,
			},
		},
		ReplyToAddresses:     params.ReplyTo,
		FromEmailAddress:     aws.String(params.From),
		ConfigurationSetName: configSet,
	}

	// Attempt to send the email.
	if _, err := s.svc.SendEmail(ctx, input); err != nil {
		var re *awshttp.ResponseError
		var msgReject *types.MessageRejected
		var domainNotVerified *types.MailFromDomainNotVerifiedException

		switch {
		case errors.As(err, &msgReject):
			var msg = "message rejected"
			if msgReject.Message != nil {
				msg = *msgReject.Message
			}
			return goaws.NewInternalError(fmt.Errorf("s.svc.SendEmail: %s", msg))
		case errors.As(err, &domainNotVerified):
			return NewUnverifiedDomainError(*domainNotVerified.Message)
		case errors.As(err, &re):
			if re.ResponseError == nil {
				return goaws.NewInternalError(fmt.Errorf("s.svc.SendEmail: %w", re.Err))
			}
			switch re.HTTPStatusCode() {
			case http.StatusBadRequest:
				return NewInvalidSendRequestError(re.ResponseError.Error())
			default:
				return goaws.NewInternalError(fmt.Errorf("s.svc.SendEmail: %w", re.Err))
			}
		default:
			return goaws.NewInternalError(fmt.Errorf("s.svc.SendEmail: %w", err))
		}
	}

	return nil
}
