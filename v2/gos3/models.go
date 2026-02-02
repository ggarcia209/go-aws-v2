package gos3

import "io"

type SHA256Checksum string

type UploadFileRequest struct {
	Bucket   string            `json:"bucket"`
	Key      string            `json:"key"`
	File     io.Reader         `json:"file"`
	Checksum *SHA256Checksum   `json:"checksum,omitempty"`
	Metadata map[string]string `json:"metadata,omitempty"`
}

type GetFileRequest struct {
	Bucket      string  `json:"bucket"`
	Key         string  `json:"key"`
	VersionId   *string `json:"version_id,omitempty"`
	UseChecksum bool    `json:"use_checksum"`
}

type GetObjectResponse struct {
	File []byte `json:"file"`
}

type ObjectExistsResponse struct {
	Exists bool `json:"exists"`
}

type HeadObjectResponse struct {
	ContentType    string            `json:"content_type"`
	Sha256Checksum string            `json:"sha256_checksum"`
	Metadata       map[string]string `json:"metadata,omitempty"`
}

type GetPresignedUrlRequest struct {
	ExpirySeconds int                `json:"expiry_seconds"`
	Put           *UploadFileRequest `json:"put,omitempty"`
	Get           *GetFileRequest    `json:"get,omitempty"`
}

type GetPresignedUrlResponse struct {
	PutUrl string `json:"put,omitempty"`
	GetUrl string `json:"get,omitempty"`
}

// UploadFileResponse contains the data returned by the S3 Upload operation.
type UploadFileResponse struct {
	Location  string `json:"location"`
	VersionID string `json:"version_id"`
	UploadID  string `json:"upload_id"`
	ETag      string `json:"etag"`
}
