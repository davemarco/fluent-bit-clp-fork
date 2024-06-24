package context

import (
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

// Holds data generated during runtime for output plugin.
// TODO: Add fields to store. Fields will be required if/when data buffering is implemented.
type S3AWS struct {
	Session    *session.Session
	S3Uploader *s3manager.Uploader
}
