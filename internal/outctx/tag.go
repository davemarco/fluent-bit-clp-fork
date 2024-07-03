package outctx

import (
	"io"

	"github.com/klauspost/compress/zstd"
	"github.com/y-scope/clp-ffi-go/ir"
)

type TagContext struct {
	Name string
	Index   int
	Io  *TagIo
}

type TagIo struct {
	Buf    io.Writer
	IrWriter *ir.Writer
	ZstdWriter *zstd.Encoder
}
