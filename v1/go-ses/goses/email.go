package goses

import (
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/ses"
	"github.com/ggarcia209/go-aws-v2/v1/goaws"
)

// CharSet repsents the charset type for email messages (UTF-8)
const CharSet = "UTF-8"

type SesLogic interface {
	ListVerifiedIdentities() ([]string, error)
	SendEmail(to, cc, replyTo []string, from, subject, textBody, htmlBody string) error
	SendEmailWithConfigSet(to, cc, replyTo []string, from, subject, textBody, htmlBody, configSetName string) error
	SendPlainTextEmail(to, cc, replyTo []string, from, subject, textBody string) error
}

type SES struct {
	svc *ses.SES
}

func NewSES(sess goaws.Session) *SES {
	return &SES{
		svc: ses.New(sess.GetSession()),
	}
}

// InitSesh initializes a new SES session.
func InitSesh(sess goaws.Session) *ses.SES {
	return ses.New(sess.GetSession())
}

func NewSESClient(session goaws.Session) interface{} {
	// Create SNS client
	svc := ses.New(session.GetSession())

	return svc
}

// ListVerifiedIdentities lists the SES verified email addresses for the account.
func (s *SES) ListVerifiedIdentities() ([]string, error) {
	var verifiedIds = make([]string, 0)

	result, err := s.svc.ListIdentities(&ses.ListIdentitiesInput{IdentityType: aws.String("EmailAddress")})
	if err != nil {
		return nil, fmt.Errorf("s.svc.ListIdentities: %w", err)
	}

	for _, email := range result.Identities {
		e := []*string{email}

		verified, err := s.svc.GetIdentityVerificationAttributes(&ses.GetIdentityVerificationAttributesInput{Identities: e})
		if err != nil {
			return nil, fmt.Errorf("s.svc.GetIdentityVerificationAttributes: %w", err)
		}

		for _, va := range verified.VerificationAttributes {
			if *va.VerificationStatus == "Success" {
				verifiedIds = append(verifiedIds, *email)
			}
		}
	}
	return verifiedIds, nil
}

// SendEmail sends a new email message. To and CC addresses are passed as []string, all other fields as strings.
func (s *SES) SendEmail(to, cc, replyTo []string, from, subject, textBody, htmlBody string) error {
	ccAddr, toAddr, replyToAddr := []*string{}, []*string{}, []*string{}
	for _, addr := range to {
		a := aws.String(addr)
		toAddr = append(toAddr, a)
	}
	for _, addr := range cc {
		a := aws.String(addr)
		ccAddr = append(ccAddr, a)
	}
	for _, addr := range replyTo {
		a := aws.String(addr)
		replyToAddr = append(replyToAddr, a)
	}

	// Assemble the email.
	input := &ses.SendEmailInput{
		Destination: &ses.Destination{
			CcAddresses: ccAddr,
			ToAddresses: toAddr,
		},
		Message: &ses.Message{
			Body: &ses.Body{
				Html: &ses.Content{
					Charset: aws.String(CharSet),
					Data:    aws.String(htmlBody),
				},
				Text: &ses.Content{
					Charset: aws.String(CharSet),
					Data:    aws.String(textBody),
				},
			},
			Subject: &ses.Content{
				Charset: aws.String(CharSet),
				Data:    aws.String(subject),
			},
		},
		ReplyToAddresses: replyToAddr,
		Source:           aws.String(from),
		// Uncomment to use a configuration set
		//ConfigurationSetName: aws.String(ConfigurationSet),
	}

	// Attempt to send the email.
	if _, err := s.svc.SendEmail(input); err != nil {
		// if aerr, ok := err.(awserr.Error); ok {
		// 	switch aerr.Code() {
		// 	case ses.ErrCodeMessageRejected:
		// 		log.Printf("SendEmail failed: %v: %v", ses.ErrCodeMessageRejected, aerr.Error())
		// 	case ses.ErrCodeMailFromDomainNotVerifiedException:
		// 		log.Printf("SendEmail failed: %v: %v", ses.ErrCodeMailFromDomainNotVerifiedException, aerr.Error())
		// 	case ses.ErrCodeConfigurationSetDoesNotExistException:
		// 		log.Printf("SendEmail failed: %v: %v", ses.ErrCodeConfigurationSetDoesNotExistException, aerr.Error())
		// 	default:
		// 		log.Printf("SendEmail failed: %v", aerr.Error())
		// 	}
		// } else {
		// 	// Print the error, cast err to awserr.Error to get the Code and
		// 	// Message from an error.
		// 	log.Printf("SendEmail failed: %v", err.Error())
		// }

		return fmt.Errorf("s.svc.SendEmail: %w", err)
	}

	return nil
}

// SendEmailWithConfigSet sends a new email message with a configuration set option. To and CC addresses are passed as []string, all other fields as strings.
func (s *SES) SendEmailWithConfigSet(
	to, cc, replyTo []string,
	from, subject, textBody, htmlBody, configSetName string,
) error {
	ccAddr, toAddr, replyToAddr := []*string{}, []*string{}, []*string{}
	for _, addr := range to {
		a := aws.String(addr)
		toAddr = append(toAddr, a)
	}
	for _, addr := range cc {
		a := aws.String(addr)
		ccAddr = append(ccAddr, a)
	}
	for _, addr := range replyTo {
		a := aws.String(addr)
		replyToAddr = append(replyToAddr, a)
	}

	// Assemble the email.
	input := &ses.SendEmailInput{
		Destination: &ses.Destination{
			CcAddresses: ccAddr,
			ToAddresses: toAddr,
		},
		Message: &ses.Message{
			Body: &ses.Body{
				Html: &ses.Content{
					Charset: aws.String(CharSet),
					Data:    aws.String(htmlBody),
				},
				Text: &ses.Content{
					Charset: aws.String(CharSet),
					Data:    aws.String(textBody),
				},
			},
			Subject: &ses.Content{
				Charset: aws.String(CharSet),
				Data:    aws.String(subject),
			},
		},
		ReplyToAddresses:     replyToAddr,
		Source:               aws.String(from),
		ConfigurationSetName: aws.String(configSetName),
	}

	// Attempt to send the email.
	if _, err := s.svc.SendEmail(input); err != nil {
		// if aerr, ok := err.(awserr.Error); ok {
		// 	switch aerr.Code() {
		// 	case ses.ErrCodeMessageRejected:
		// 		log.Printf("SendEmail failed: %v: %v", ses.ErrCodeMessageRejected, aerr.Error())
		// 	case ses.ErrCodeMailFromDomainNotVerifiedException:
		// 		log.Printf("SendEmail failed: %v: %v", ses.ErrCodeMailFromDomainNotVerifiedException, aerr.Error())
		// 	case ses.ErrCodeConfigurationSetDoesNotExistException:
		// 		log.Printf("SendEmail failed: %v: %v", ses.ErrCodeConfigurationSetDoesNotExistException, aerr.Error())
		// 	default:
		// 		log.Printf("SendEmail failed: %v", aerr.Error())
		// 	}
		// } else {
		// 	// Print the error, cast err to awserr.Error to get the Code and
		// 	// Message from an error.
		// 	log.Printf("SendEmail failed: %v", err.Error())
		// }

		return fmt.Errorf("s.svc.SendEmail: %w", err)
	}

	return nil
}

// SendEmail sends a new email message. To and CC addresses are passed as []string, all other fields as strings.
func (s *SES) SendPlainTextEmail(to, cc, replyTo []string, from, subject, textBody string) error {
	ccAddr, toAddr, replyToAddr := []*string{}, []*string{}, []*string{}
	for _, addr := range to {
		a := aws.String(addr)
		toAddr = append(toAddr, a)
	}
	for _, addr := range cc {
		a := aws.String(addr)
		ccAddr = append(ccAddr, a)
	}
	for _, addr := range replyTo {
		a := aws.String(addr)
		replyToAddr = append(replyToAddr, a)
	}

	// Assemble the email.
	input := &ses.SendEmailInput{
		Destination: &ses.Destination{
			CcAddresses: ccAddr,
			ToAddresses: toAddr,
		},
		Message: &ses.Message{
			Body: &ses.Body{
				Text: &ses.Content{
					Charset: aws.String(CharSet),
					Data:    aws.String(textBody),
				},
			},
			Subject: &ses.Content{
				Charset: aws.String(CharSet),
				Data:    aws.String(subject),
			},
		},
		ReplyToAddresses: replyToAddr,
		Source:           aws.String(from),
		// Uncomment to use a configuration set
		//ConfigurationSetName: aws.String(ConfigurationSet),
	}

	// Attempt to send the email.
	if _, err := s.svc.SendEmail(input); err != nil {
		// if aerr, ok := err.(awserr.Error); ok {
		// 	switch aerr.Code() {
		// 	case ses.ErrCodeMessageRejected:
		// 		log.Printf("SendEmail failed: %v: %v", ses.ErrCodeMessageRejected, aerr.Error())
		// 	case ses.ErrCodeMailFromDomainNotVerifiedException:
		// 		log.Printf("SendEmail failed: %v: %v", ses.ErrCodeMailFromDomainNotVerifiedException, aerr.Error())
		// 	case ses.ErrCodeConfigurationSetDoesNotExistException:
		// 		log.Printf("SendEmail failed: %v: %v", ses.ErrCodeConfigurationSetDoesNotExistException, aerr.Error())
		// 	default:
		// 		log.Printf("SendEmail failed: %v", aerr.Error())
		// 	}
		// } else {
		// 	// Print the error, cast err to awserr.Error to get the Code and
		// 	// Message from an error.
		// 	log.Printf("SendEmail failed: %v", err.Error())
		// }

		return fmt.Errorf("s.svc.SendEmail: %w", err)
	}

	return nil
}
