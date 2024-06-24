// Package contains constants.
package constant

import (
	"github.com/fluent/fluent-bit-go/output"
)

const S3PluginName = "out_clp_s3"

// Map of Fluent-bit codes to string
var FLBCodes = map[int] string{
    output.FLB_ERROR: "FLB_ERROR",
    output.FLB_OK: "FLB_OK",
    output.FLB_RETRY: "FLB_RETRY",
}
