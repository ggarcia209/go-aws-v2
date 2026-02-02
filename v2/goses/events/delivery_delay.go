package events

// DeliveryDelay contains info for the DeliveryDelay events.
type DeliveryDelay struct {
	DelayType         string             `json:"delayType"`
	DelayedRecipients []delayedRecipient `json:"delayedRecipients"`
	Timestamp         string             `json:"timestamp"`
	ExpirationTime    string             `json:"expirationTime"`
	ReportingMTA      string             `json:"reportingMTA"`
}

type delayedRecipient struct {
	EmailAddress   string `json:"emailAddress"`
	Status         string `json:"status"`
	DiagnosticCode string `json:"diagnosticCode"`
}
