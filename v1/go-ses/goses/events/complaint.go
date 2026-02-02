package events

type Complaint struct {
	ComplainedRecipients  []complaintRecipient `json:"complainedRecipients"`
	Timestamp             string               `json:"timestamp"`
	FeedbackId            string               `json:"feedbackId"`
	UserAgent             string               `json:"userAgent"`
	ComplaintFeedbackType string               `json:"complaintFeedbackType"`
	ArrivalDate           string               `json:"arrivalDate"`
}

type complaintRecipient struct {
	EmailAddress string `json:"emailAddress"`
}
