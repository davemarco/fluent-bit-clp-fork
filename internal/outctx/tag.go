package outctx

import (
	"os"

	"github.com/klauspost/compress/zstd"
	"github.com/y-scope/clp-ffi-go/ir"
)

type TagState struct {
	Index   int
	Buffer  *TagBuffer
}

type TagBuffer struct {
	File    os.File
	IrWriter *ir.Writer
	ZstdWriter *zstd.Encoder
}
