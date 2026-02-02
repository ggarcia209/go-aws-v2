//go:build integration

package gos3

import (
	"fmt"
	"os"
	"testing"

	"github.com/ggarcia209/go-aws-v2/v1/goaws"
)

func TestGetObject(t *testing.T) {
	var tests = []struct {
		bucket string
		key    string
		want   error
	}{
		{bucket: "test-bucket-2", key: "html/email-receipt-tmpl.html", want: nil},
		{bucket: "test-bucket-2", key: "img/pw-banner.jpg", want: nil},
		{bucket: "test-bucket", key: "img/pw-banner.jpg", want: fmt.Errorf("ITEM_NOT_FOUND")},
	}
	svc := NewS3(goaws.NewDefaultSession(), DefaultPartitionSize)
	for _, test := range tests {
		_, err := svc.GetObject(test.bucket, test.key)
		if err != nil {
			if test.want == nil {
				t.Errorf("FAIL: %v", err)
			}
			if err.Error() != test.want.Error() {
				t.Errorf("FAIL: %v; want: %v", err, test.want.Error())
			}

		}
		// t.Logf("result: %v", obj)
	}
}

func TestUploadFile(t *testing.T) {
	var tests = []struct {
		bucket   string
		key      string
		filepath string
		public   bool
		want     string
	}{
		{bucket: "test-bucket", key: "/img/test001.jpg", filepath: "./img/king.jpg", public: true, want: ""},
		{bucket: "test-bucket", key: "/img/test002.jpg", filepath: "./img/queen.jpg", public: false, want: ""},
		{bucket: "", key: "/img/test001.jpg", filepath: "./img/king.jpg", public: true, want: "InvalidParameter"},
		{bucket: "test-bucket", key: "", filepath: "./img/king.jpg", public: true, want: "InvalidParameter"},
		{bucket: "", key: "", filepath: "./img/king.jpg", public: true, want: "InvalidParameter"},
	}
	svc := NewS3(goaws.NewDefaultSession(), DefaultPartitionSize)

	for _, test := range tests {
		file, err := os.Open(test.filepath)
		if err != nil {
			t.Errorf("FAIL - file: %v", err)
		}
		res, err := svc.UploadFile(test.bucket, test.key, file, test.public)
		if err == nil && test.want != "" {
			t.Errorf("FAIL: %v; want: %v", err, test.want)
		}
		if err != nil && err.Error() != test.want {
			t.Errorf("FAIL: %v; want: %v", err, test.want)
		}
		t.Logf("result: %v", res)
	}
}
