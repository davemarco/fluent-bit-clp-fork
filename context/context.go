// Package implements a context which is accessible by output plugin and stored by fluent-bit
// engine.
package context

import (
	"unsafe"
	"context"
	"log"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
)

// Holds configuration and state
type S3Context struct {
	Config S3Config
	S3Uploader *manager.Uploader
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
	var S3config S3Config
	err := S3config.New(plugin)
	if err != nil {
		return nil, err
	}

	// Load the aws credentials. Library will look for credentials in a specfic [hierarchy].
	// [hierarchy]: https://aws.github.io/aws-sdk-go-v2/docs/configuring-sdk/
	cfg, err := config.LoadDefaultConfig(context.TODO(),config.WithRegion("us-west-2"))
	if err != nil {
		log.Fatal(err)
	}
	
	
	if true {
		stsSvc := sts.NewFromConfig(cfg)
		creds := stscreds.NewAssumeRoleProvider(stsSvc,"arn:aws:iam::568954113123:role/mawk_test_role")
		cfg.Credentials = aws.NewCredentialsCache(creds)
	}

	// Create an Amazon S3 service client. Older version described client as thread safe, assuming 
	// v2 also thread safe.
	client := s3.NewFromConfig(cfg)

	// Create an uploader passing it the client
	uploader := manager.NewUploader(client)

	ctx := S3Context{
		Config: S3config,
		S3Uploader: uploader,
	}

	return &ctx, nil
}
