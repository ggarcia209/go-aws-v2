package events

// RenderingFailure contains info for the RenderingFailure events.
type RenderingFailure struct {
	TemplateName string `json:"templateName"`
	ErrorMessage string `json:"errorMessage"`
}
