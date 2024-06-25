// Package implements methods to send data to output. All data provided by fluent-bit is encoded
// with msgpack.

package flush

import (
	"bytes"
	"C"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"
	"unsafe"
	"net/url"
	"github.com/google/uuid"

	"github.com/fluent/fluent-bit-go/output"
	jsoniter "github.com/json-iterator/go"
	"github.com/klauspost/compress/zstd"
	"github.com/y-scope/clp-ffi-go/ffi"

	pluginCtx "github.com/y-scope/fluent-bit-clp/context"
	"github.com/y-scope/fluent-bit-clp/decoder"
	"github.com/y-scope/fluent-bit-clp/internal/constant"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// Flushes data to a file in IR format. Decode of msgpack based on [fluent-bit reference].
//
// Parameters:
//   - data: msgpack data
//   - length: Byte length
//   - tag: fluent-bit tag
//   - S3Context: Plugin context
//
// Returns:
//   - code: fluent-bit success code (OK, RETRY, ERROR)
//   - err: Error if flush fails
//
// [fluent-bit reference]: https://github.com/fluent/fluent-bit-go/blob/a7a013e2473cdf62d7320822658d5816b3063758/examples/out_multiinstance/out.go#L41
func File(data unsafe.Pointer, length int, tag string, ctx *pluginCtx.S3Context) (int, error) {
	// Buffer to store events from fluent-bit chunk.
	var logEvents []ffi.LogEvent

	dec := decoder.NewStringDecoder(data, length)

	// Loop through all records in fluent-bit chunk.
	for {
		ret, ts, record := output.GetRecord(dec)
		if ret != 0 {
			break
		}

		timestamp := DecodeTs(ts)
		msg, err := GetMessage(record, ctx.Config)
		if err != nil {
			err = fmt.Errorf("failed to get message from record: %w", err)
			return output.FLB_ERROR, err
		}

		msgString, ok := msg.(string)
		if !ok {
			err = fmt.Errorf("string type assertion for message failed %v", msg)
			return output.FLB_ERROR, err
		}

		event := ffi.LogEvent{
			LogMessage: msgString,
			Timestamp:  ffi.EpochTimeMs(timestamp.UnixMilli()),
		}
		logEvents = append(logEvents, event)
	}

	var buf bytes.Buffer

	code, err := EncodeEvents(buf,logEvents,length,ctx.Config.IREncoding,ctx.Config.TimeZone)
	if err != nil {
		stringCode := constant.FLBCodes[code]
		err = fmt.Errorf("error encoding log events, forwarding %s to engine: %w", stringCode,err)
		return code, err
	}

	currentTime := time.Now()

	// Format the time as a string in RFC3339 format.
	timeString := currentTime.Format(time.RFC3339)
	uuid := uuid.New()

	fileWithTs := fmt.Sprintf("%s_%s_%s.zst", tag, timeString,uuid)
	fullFilePath := filepath.Join(ctx.Config.Path, fileWithTs)

	// Upload the file to S3.
	tag = fmt.Sprintf("fluentBitTag=%s",tag)
	result, err := ctx.S3Uploader.Upload(context.TODO(), &s3.PutObjectInput{
		Bucket: aws.String(ctx.Config.S3Bucket),
		Key:    aws.String(fullFilePath),
		Body:   &buf,
		Tagging: &tag,
	})

	if err != nil {
		err = fmt.Errorf("failed to upload file, %v", err)
		return output.FLB_ERROR, err
	}

	url, err := url.QueryUnescape(result.Location)
	if err != nil {
		url = result.Location
	}

	fmt.Printf("file uploaded to %s \n", url)
	return output.FLB_OK, nil
}

// Decodes timestamp provided by fluent-bit engine into time.Time. If timestamp cannot be
// decoded, returns system time.
//
// Parameters:
//   - ts: timestamp provided by fluent-bit
//
// Returns:
//   - timestamp: time.Time timestamp
func DecodeTs(ts interface{}) time.Time {
	var timestamp time.Time
	switch t := ts.(type) {
	case output.FLBTime:
		timestamp = ts.(output.FLBTime).Time
	case uint64:
		timestamp = time.Unix(int64(t), 0)
	default:
		fmt.Println("time provided invalid, defaulting to now.")
		timestamp = time.Now()
	}
	return timestamp
}

// Retrieves message as a string from record object. The message can consist of the entire object or
// just a single key. For a single key, user should set set_single_key to true in fluentbit.conf.
// In addition user, should set single_key to "log" which is default fluent-bit key for unparsed
// messages; however, single_key can be set to another value. To prevent failure if the key is
// missing, user can specify allow_missing_key, and behaviour will fallback to the entire object.
//
// Parameters:
//   - record: Structured record from fluent-bit with variable amount of keys
//   - config: Configuration based on fluent-bit.conf
//
// Returns:
//   - msg: Retrieved message
//   - err: Key not found, json.Marshal error
func GetMessage(record map[interface{}]interface{}, config pluginCtx.S3Config) (interface{}, error) {
	var msg interface{}
	var ok bool
	var err error
	json := jsoniter.ConfigCompatibleWithStandardLibrary

	// If use_single_key=true, then look for key in record, and set message to the key's value.
	if config.UseSingleKey {
		msg, ok = record[config.SingleKey]
		if !ok {
			// If key not found in record, see if allow_missing_key=false. If missing key is
			// allowed. then fallback to marshal entire object.
			if config.AllowMissingKey {
				msg, err = json.MarshalToString(record)
				if err != nil {
					return nil, fmt.Errorf("failed to marshal record %v: %w", record, err)
				}
				// If key not found in record and allow_missing_key=false, then return an error.
			} else {
				return nil, fmt.Errorf("key %s not found in record %v", config.SingleKey, record)
			}
		}
	} else {
		msg, err = json.MarshalToString(record)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal record %v: %w", record, err)
		}
	}
	return msg, nil
}

// Creates a new file to output IR. A new file is created for every fluent-bit chunk. File name is
// based on user configuration and the system timestamp is added as a suffix.
//
// Parameters:
//   - path: path from fluent-bit.conf
//   - file: file name from fluent-bit.conf
//
// Returns:
//   - f: os file
//   - err: could not create directory, could not create file
func CreateFile(path string, file string) (*os.File, error) {
	// Make directory if does not exist.
	err := os.MkdirAll(path, 0o644)
	if err != nil {
		err = fmt.Errorf("failed to create directory %s: %w", path, err)
		return nil, err
	}

	currentTime := time.Now()

	// Format the time as a string in RFC3339 format.
	timeString := currentTime.Format(time.RFC3339)

	fileWithTs := fmt.Sprintf("%s_%s.zst", file, timeString)

	fullFilePath := filepath.Join(path, fileWithTs)

	// If the file doesn't exist, create it.
	f, err := os.OpenFile(fullFilePath, os.O_WRONLY|os.O_CREATE, 0o644)
	if err != nil {
		err = fmt.Errorf("failed to create file %s: %w", fullFilePath, err)
		return nil, err
	}
	return f, nil
}

// Encodes slice containing log events into zstd compressed IR and writes to buffer.
//
// Parameters:
//   - buf
//   - logEvents: slice of log events
//   - length: Byte length of fluent-bit msgpack chunk
//	 - encoding: Type of IR to encode
//	 - timezone: Time zone of the source producing the log events, so that local times (any time
//	that is not a unix timestamp) are handled correctly
//
// Returns:
//   - code: fluent-bit success code (OK, RETRY, ERROR)
//   - err: error from zstd encoder, IR writer
func EncodeEvents(buf bytes.Buffer, logEvents []ffi.LogEvent, length int, encoding string, timezone string) (int, error) {

	zstdEncoder, err := zstd.NewWriter(&buf)
	if err != nil {
		err = fmt.Errorf("error opening zstd encoder: %w", err)
		return output.FLB_RETRY, err
	}
	defer zstdEncoder.Close()

	// IR buffer using bytes.Buffer. So it will dynamically adjust if undersized.
	irWriter, err := OpenIRWriter(length, encoding, timezone)
	if err != nil {
		err = fmt.Errorf("error opening IR writer: %w", err)
		return output.FLB_RETRY, err
	}

	err = EncodeIR(irWriter, logEvents)
	if err != nil {
		err = fmt.Errorf("error while encoding IR: %w", err)
		return output.FLB_ERROR, err
	}

	// Write zstd compressed IR to buffer.
	_, err = irWriter.CloseTo(zstdEncoder)
	if err != nil {
		err = fmt.Errorf("error writting IR to buf: %w", err)
		return output.FLB_RETRY, err
	}

	return output.FLB_OK, nil
}
