package events

// Click contains info for the Click events.
type Click struct {
	IpAddress string              `json:"ipAddress"`
	Timestamp string              `json:"timestamp"`
	UserAgent string              `json:"userAgent"`
	Link      string              `json:"link"`
	LinkTags  map[string][]string `json:"linkTags"`
}
