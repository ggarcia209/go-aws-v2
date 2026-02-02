//go:build integration

package goses

import (
	"testing"

	"github.com/ggarcia209/go-aws-v2/v1/goaws"
)

func TestListVerifiedIdentities(t *testing.T) {
	svc := NewSES(goaws.NewDefaultSession())
	ids, err := svc.ListVerifiedIdentities()
	if err != nil {
		t.Errorf("FAIL: %v", err)
	}
	t.Logf("ids: %v", ids)
}

func TestSendEmail(t *testing.T) {
	var tests = []struct {
		to       []string
		cc       []string
		replyTo  []string
		textBody string
		htmlBody string
	}{
		{to: []string{"tests@gmail.com"}, cc: []string{}, textBody: "Testing\nThis is a test", replyTo: []string{}, htmlBody: "<h1>Testing</h1><p>This is a test</p>"},
		{to: []string{"tests@gmail.com"}, cc: []string{}, textBody: "Testing\nThis is a test", replyTo: []string{}, htmlBody: ""}, // result: empty msg body - no text body output
	}
	subject := "SES TEST"
	from := "devtest0@gmail.com"
	svc := NewSES(goaws.NewDefaultSession())
	for _, test := range tests {
		err := svc.SendEmail(test.to, test.cc, test.replyTo, from, subject, test.textBody, test.htmlBody)
		if err != nil {
			t.Errorf("FAIL: %v", err)
		}
	}
}

func TestSendEmailWithSession(t *testing.T) {
	var tests = []struct {
		to       []string
		cc       []string
		replyTo  []string
		textBody string
		htmlBody string
	}{
		{to: []string{"tests@gmail.com"}, cc: []string{}, replyTo: []string{}, textBody: "Testing\nThis is a test", htmlBody: "<h1>Testing</h1><br><p>This is an HTML test</p>"},
		{to: []string{"tests@gmail.com"}, cc: []string{}, replyTo: []string{}, textBody: "Testing\nThis is a test", htmlBody: ""}, // result: empty msg body - no text body output
	}
	subject := "SES TEST"
	from := "devtest0@gmail.com"

	for _, test := range tests {
		svc := NewSES(goaws.NewDefaultSession())
		err := svc.SendEmail(test.to, test.cc, test.replyTo, from, subject, test.textBody, test.htmlBody)
		if err != nil {
			t.Errorf("FAIL: %v", err)
		}
	}
}
