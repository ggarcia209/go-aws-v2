package events

// Delivery contains info for Delivery events.
type Delivery struct {
	Timestamp            string   `json:"timestamp"`
	ProcessingTimeMillis string   `json:"processingTimeMillis"`
	Recipients           []string `json:"recipients"`
	SmtpResponse         string   `json:"smtpResponse"`
	ReportingMTA         string   `json:"reportingMTA"`
}
