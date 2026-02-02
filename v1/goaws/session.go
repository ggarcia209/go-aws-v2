package goaws

// TO DO: add error handling for credentials not found

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
)

// Session contains an AWS Session for use with other AWS services in the go-aws package.
type Session struct {
	session *session.Session
}

// Retrieve AWS Session from Session object.
func (s *Session) GetSession() *session.Session {
	return s.session
}

func NewDefaultSession() Session {
	// Initialize a session that the SDK will use to load
	// credentials from the shared credentials file ~/.aws/credentials
	s := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))

	sesh := Session{session: s}

	return sesh
}

// InitSesh initializes a new SES session.
func NewSessionWithProfile(profile string) Session {
	// Initialize a session that the SDK will use to load
	// credentials from the shared credentials file ~/.aws/credentials
	// matching the given profile
	s := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
		Profile:           profile,
		Config:            aws.Config{},
	}))

	sesh := Session{session: s}

	return sesh
}

// NewSessionFromEnv uses AWS_ env vars to initialize the session
func NewSessionFromEnv(profile string) Session {
	s := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigDisable,
		Config: aws.Config{
			Credentials: credentials.NewEnvCredentials(),
		},
	}))

	sesh := Session{session: s}

	return sesh
}

// NewSessionWithDefaultConfig creates session with default configuration
func NewSessionWithDefaultConfig() Session {
	s := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigDisable,
	}))

	sesh := Session{session: s}

	return sesh
}
