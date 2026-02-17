package goses

type SendEmailParams struct {
	Subject     string       `json:"subject"`
	From        string       `json:"from"`
	To          []string     `json:"to"`
	ReplyTo     []string     `json:"reply_to,omitempty"`
	Cc          []string     `json:"cc,omitempty"`
	TextBody    string       `json:"text_body"`
	HtmlBody    string       `json:"html_body,omitempty"`
	ConfigSet   string       `json:"config_set,omitempty"`
	Attachments []Attachment `json:"attachments,omitempty"`
}

type Attachment struct {
	FileName    string  `json:"file_name"`
	Data        []byte  `json:"data"`
	ContentType *string `json:"content_type,omitempty"`
}

type ListVerifiedIdentitiesResponse struct {
	EmailAddresses []string `json:"email_addresses"`
}
