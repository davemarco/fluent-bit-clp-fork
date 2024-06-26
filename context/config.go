// Package implements loading of Fluent Bit configuration file. Configuration is accessible by
// output plugin and stored by Fluent Bit engine.

package context

import (
	"encoding/json"
	"unsafe"

	"github.com/fluent/fluent-bit-go/output"
	"github.com/go-playground/validator/v10"
)

// Holds settings for S3 CLP plugin from user defined Fluent Bit configuration file.
type S3Config struct {
	Id              string `json:"id" validate:"required"`
	Path            string `json:"path" validate:"required"`
	File            string `json:"file" validate:"required"`
	UseSingleKey    bool   `json:"use_single_key,string" validate:"required"`
	AllowMissingKey bool   `json:"allow_missing_key,string" validate:"required"`
	SingleKey       string `json:"single_key" validate:"required"`
	TimeZone        string `json:"time_zone" validate:"required"`
	IREncoding      string `json:"IR_encoding" validate:"required"`
	S3Bucket        string `json:"s3_bucket" validate:"required"`
	S3Region        string `json:"s3_region," validate:"required"`
	RoleArn         string `json:"role_arn" validate:"required"`
}

// Keys to set configuration values in fluent-bit.conf. Keys are presented to user in README. 
var configKeys = []string{
	"id",
	"path",
	"file",
	"use_single_key",
	"allow_missing_key",
	"single_key",
	"time_zone",
	"IR_encoding",
	"s3_bucket",
	"s3_region",
	"role_arn",
}

// Generates configuration struct containing user-defined settings.
//
// Parameters:
//   - plugin: Fluent Bit plugin reference
//
// Returns:
//   - S3Config: Configuration based on fluent-bit.conf
//   - err: Validation errors, encoding/json errors
func (s *S3Config) New(plugin unsafe.Pointer) error {

	var userInput = make(map[string]string)
	// Load user input into map
	for _, key := range configKeys {
		userInput[key] = output.FLBPluginConfigKey(plugin, key)
	}

	// Convert map to JSON
	JsonUserInput, err := json.Marshal(userInput)
	if err != nil {
		return err
	}

	// Unmarshal JSON directly into s
	if err := json.Unmarshal(configJSON, s); err != nil {
		return err
	}

	// Validate using validator package
	validate := validator.New(validator.WithRequiredStructEnabled())
	if err := validate.Struct(s); err != nil {
		return err
	}

	return nil
}