// Package implements a context which is accessible by output plugin and stored by fluent-bit
// engine.
package context

import (
	"unsafe"
	"context"
	"log"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsConfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// Holds configuration and state
type S3Context struct {
	Config S3Config
	AWS    S3AWS
}

// Creates a new context including loading of configuration and initialization of plugin state.
//
// Parameters:
//   - plugin: fluent-bit plugin reference
//
// Returns:
//   - S3Context: plugin context
//   - err: configuration load failed
func NewS3Context(plugin unsafe.Pointer) (*S3Context, error) {
	var config S3Config
	err := config.New(plugin)
	if err != nil {
		return nil, err
	}

	// Load the aws credentials. Library will look for credentials in a specfic [hierarchy].
	// [hierarchy]: https://aws.github.io/aws-sdk-go-v2/docs/configuring-sdk/
	cfg, err := awsConfig.LoadDefaultConfig(context.TODO())
	if err != nil {
		log.Fatal(err)
	}
	

	sess, err := session.NewSession(&aws.Config{
		Region: aws.String("us-east-2")},
	)

	if err != nil {
		return nil, err
	}


	uploader := s3manager.NewUploader(sess)

	ctx := S3Context{
		Config: config,
		AWS: S3AWS{
			Session:    sess,
			S3Uploader: uploader,
		},
	}

	return &ctx, nil
}
