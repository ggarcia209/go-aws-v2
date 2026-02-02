package events

// EmailNotification wraps the SES Event data type.
type EmailNotification struct {
	NotificationType string           `json:"notificationType"`
	Bounce           Bounce           `json:"bounce"`
	Complaint        Complaint        `json:"complaint"`
	Delivery         Delivery         `json:"delivery"`
	Send             any              `json:"send"`
	Reject           Reject           `json:"reject"`
	Open             Open             `json:"open"`
	Click            Click            `json:"click"`
	RenderingFailure RenderingFailure `json:"failure"`
	DeliveryDelay    DeliveryDelay    `json:"deliveryDelay"`
	Mail             Mail             `json:"mail"`
}

type Mail struct {
	Timestamp        string              `json:"timestamp"`
	Source           string              `json:"source"`
	SourceArn        string              `json:"sourceArn"`
	SendingAcctId    string              `json:"sendingAccountId"`
	MessageId        string              `json:"messageId"`
	Destination      []string            `json:"destination"`
	HeadersTruncated bool                `json:"headersTruncated"`
	Headers          []header            `json:"headers"`
	CommonHeaders    map[string]string   `json:"commonHeaders"`
	Tags             map[string][]string `json:"tags"`
}

type header struct {
	Name   string `json:"name"`
	String string `json:"value"`
}
