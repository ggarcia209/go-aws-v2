package events

// Open contains info for the Open events.
type Open struct {
	IpAddress string `json:"ipAddress"`
	Timestamp string `json:"timestamp"`
	UserAgent string `json:"userAgent"`
}
