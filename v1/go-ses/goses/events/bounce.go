package events

// Bounce containts info for bounce events.
type Bounce struct {
	BounceType        string            `json:"bounceType"`
	BounceSubType     string            `json:"bounceSubType"`
	BouncedRecipients []bounceRecipient `json:"bouncedRecipients"`
	Timestamp         string            `json:"timestamp"`
	FeedbackId        string            `json:"feedbackId"`
	ReportingMTA      string            `json:"reportingMTA"`
}

type bounceRecipient struct {
	EmailAddress   string `json:"emailAddress"`
	Action         string `json:"action"`
	Status         string `json:"status"`
	DiagnosticCode string `json:"diagnosticCode"`
}
