// Package implements loading of Fluent Bit configuration file. Configuration is accessible by
// output plugin and stored by Fluent Bit engine.

package outctx

import (
	"fmt"
	"errors"
	"reflect"
	"strconv"
	"unsafe"
	"strings"


	"github.com/go-playground/validator/v10"
	"github.com/google/uuid"

	"github.com/fluent/fluent-bit-go/output"
)

// Holds settings for S3 CLP plugin from user defined Fluent Bit configuration file.
type S3Config struct {
	Id              string
	UseSingleKey    bool
	AllowMissingKey bool
	SingleKey       string
	TimeZone        string
	S3Bucket        string
	S3BucketPrefix  string
	S3Region        string
	RoleArn         string
}

// Holds settings for S3 CLP plugin from user defined Fluent Bit configuration file.
type S3UserInput struct {
	Id              string `conf:"id" validate:"-" `
	UseSingleKey    string `conf:"use_single_key" validate:"boolean"`
	AllowMissingKey string `conf:"allow_missing_key" validate:"boolean"`
	SingleKey       string `conf:"time_zone" validate:"required_if=use_single_key true"`
	TimeZone        string `conf:"time_zone" validate:"timezone"`
	S3Bucket        string `conf:"s3_bucket" validate:"required"`
	S3BucketPrefix  string `conf:"s3_bucket_prefix" validate:"dirpath"`
	S3Region        string `conf:"time_zone" validate:"required"`
	RoleArn         string `conf:"role_arn" validate:"omitempty,startswith=arn:aws:iam"`
}

// Map keys are the plugin options described to user in README. The values are rules to be consumed
// by [validator]. The map is used to check if the user input meets specific rules. The
// functionality of each rule can be found in docs for [validator]. "required" is only
// neccesary if no rule is defined. Effectively means user must put something. "omitempty"
// ignores validation for fields which user left blank.
/*
var pluginOptions = map[string]interface{}{
	"id":                "-",
	"use_single_key":    "boolean",
	"allow_missing_key": "boolean",
	"single_key":        "required_if=use_single_key true",
	"time_zone":         "timezone",
	"s3_bucket":         "required",
	"s3_bucket_prefix":  "dirpath",
	"s3_region":         "required",
	"role_arn":          "omitempty,startswith=arn:aws:iam",
}
*/

// Generates configuration struct containing user-defined settings.
//
// Parameters:
//   - plugin: Fluent Bit plugin reference
//
// Returns:
//   - S3Config: Configuration based on fluent-bit.conf
//   - err: All errors in config wrapped
func NewS3(plugin unsafe.Pointer) (*S3Config, error) {
	// TODO: Redo validation to simplify configuration error reporting.
	// https://pkg.go.dev/github.com/go-playground/validator/v10

	userInput := S3UserInput{
		Id:              uuid.New().String(),
		UseSingleKey:    "true",
		AllowMissingKey: "true",
		SingleKey:       "log",
	}
	var err error

	/*
	var pluginOptions = map[string]*string{
		"id":                &userInput.Id,
		"use_single_key":    &userInput.UseSingleKey,
		"allow_missing_key": &userInput.AllowMissingKey,
		"single_key":        &userInput.SingleKey,
		"time_zone":         &userInput.TimeZone,
		"s3_bucket":         &userInput.S3Bucket,
		"s3_bucket_prefix":  &userInput.S3BucketPrefix,
		"s3_region":         &userInput.S3Region,
		"role_arn":          &userInput.RoleArn,
	}

	// Retrieve values defined in fluent-bit.conf. Function supplied by Fluent Bit retrieves all
	// values as strings. If the option is not defined by user, it is set to "".
	for key, userValue := range pluginOptions {
		*userValue = output.FLBPluginConfigKey(plugin, key)
	}
	*/

	val := reflect.ValueOf(userInput).Elem()

	for i := 0; i < val.NumField(); i++ {
		field := val.Type().Field(i)
		tag := field.Tag.Get("conf")
		value := output.FLBPluginConfigKey(plugin, tag)
		if value != "" {
			val.Field(i).SetString(value)
		}
	}

	// Define default values for optional settings. Setting defaults before validation simplifies
	// validation settings, and ensures that default settings are also validated.
	defaultConfig := map[string]string{
		"id ":               uuid.New().String(),
		"use_single_key":    "true",
		"allow_missing_key": "true",
		"single_key":        "log",
	}

	// If user did not specify a value, replace empty string with default value
	for key, userValue := range pluginOptions {
		if *userValue == "" {
			// If user value is empty, check if a default value exists. If exists, set config value
			// to default value.
			defaultValue, ok := defaultConfig[key]
			if ok {
				*userValue = defaultValue
			}
		}
	}

	validate := validator.New(validator.WithRequiredStructEnabled())

	validate.RegisterTagNameFunc(func(fld reflect.StructField) string {
		name := strings.SplitN(fld.Tag.Get("conf"), ",", 2)[0]
		// skip if tag key says it should be ignored
		if name == "-" {
			return ""
		}
		return name
	})

	err = validate.Struct(&userInput)

	configErrors := []error{}

	if err != nil {
		valErr := err.(validator.ValidationErrors)
		// ValidateStruct will provide an error for each field, so loop over all errors.
		for _, err := range valErr {
			err := fmt.Errorf("error validating option %s=%s failed test %s",
				err.Field(), err.Value(), err.Tag())
			configErrors = append(configErrors, err)
		}
		err = errors.Join(configErrors...)
		return nil, err
	}

	// Type conversion to bool.
	useSingleKey, err := strconv.ParseBool(userInput.UseSingleKey)
	if err != nil {
		return nil, err
	}

	allowMissingKey, err := strconv.ParseBool(userInput.AllowMissingKey)
	if err != nil {
		return nil, err
	}

	config := S3Config{
		Id:              userInput.Id,
		UseSingleKey:    useSingleKey,
		AllowMissingKey: allowMissingKey,
		SingleKey:       userInput.SingleKey,
		TimeZone:        userInput.TimeZone,
		S3Bucket:        userInput.S3Bucket,
		S3BucketPrefix:  userInput.S3BucketPrefix,
		S3Region:        userInput.S3Region,
		RoleArn:         userInput.RoleArn,
	}

	return &config, err
}
