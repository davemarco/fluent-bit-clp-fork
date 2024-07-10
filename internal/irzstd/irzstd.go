package irzstd
// using irzstd to prevent namespace collision with [ir].

import (
	"io"
	"fmt"

	"github.com/fluent/fluent-bit-go/output"
	"github.com/klauspost/compress/zstd"

	"github.com/y-scope/clp-ffi-go/ir"
	"github.com/y-scope/clp-ffi-go/ffi"
)

// Converts log events into Zstd compressed IR. Effectively chains [ir.Writer] then [zstd.Encoder] in series.
// Compressed IR output is [io.Writer] provided to [zstd.Encoder].
type IrZstdWriter struct {
	TotalBytes int
	IrWriter *ir.Writer
	ZstdWriter *zstd.Encoder
}

// Creates a new irZstdWriter
//
// Parameters:
//   - writer: Msgpack data
//   - length: Byte length
//   - tag: Fluent Bit tag
//   - S3Config: Plugin configuration
//
// Returns:
//   - code: Fluent Bit success code (OK, RETRY, ERROR)
//   - err: Error if flush fails
//
func NewIrZstdWriter(writer io.Writer, timezone string, size int) (*IrZstdWriter, error) {
	zstdWriter, err := zstd.NewWriter(writer)
	if err != nil {
		return nil, fmt.Errorf("error opening zstd writer: %w", err)
	}

	// IR buffer using bytes.Buffer internally, so it will dynamically grow if undersized. Using
	// FourByteEncoding as default encoding.
	irWriter, err := ir.NewWriterSize[ir.FourByteEncoding](size, timezone)
	if err != nil {
		return nil, fmt.Errorf("error opening IR writer: %w", err)
	}

	IrZstdWriter := IrZstdWriter{
		IrWriter: irWriter,
		ZstdWriter: zstdWriter,
	}

	return &IrZstdWriter, nil
}


func (w *IrZstdWriter) WriteIrZstd(logEvents []ffi.LogEvent) (int, error) {
	err := writeIr(w.IrWriter, logEvents)
	if err != nil {
		return output.FLB_ERROR, err
	}

	// Flush irWriter buffer to zstdWriter.
	numBytes, err := w.IrWriter.WriteTo(w.ZstdWriter)
	fmt.Printf("%d \n",numBytes)

	// Increment total bytes written.
	// TODO: Improve error handling for partially written bytes.
	w.TotalBytes += int(numBytes)
	if err != nil {
		return output.FLB_RETRY, err
	}

	// Flush zstd writer to store. Calling flush may reduce performance, but in case where store is
	// file, preferable to actually write to file instead of keeping in memory.
	//err = w.ZstdWriter.Flush()
	//err = w.ZstdWriter.Close()
	//if err != nil {
	//	return output.FLB_RETRY, err
	//}

	return output.FLB_OK, nil
}

func (w IrZstdWriter) Close() (error) {
	// Null terminate ir stream and flush to zstdwriter
	_, err := w.IrWriter.CloseTo(w.ZstdWriter)
	if err != nil {
		return err
	}

	// ZstdWriter can still be re-used after calling close.
	err = w.ZstdWriter.Close()
	if err != nil {
		return err
	}

	return nil
}

// Writes log events to a IR Writer.
//
// Parameters:
//   - irWriter: CLP IR writer to write each log event with
//   - eventBuffer: A slice of log events to be encoded
//
// Returns:
//   - err: error if an event could not be written
func writeIr(irWriter *ir.Writer, eventBuffer []ffi.LogEvent) error {
	for _, event := range eventBuffer {
		_, err := irWriter.Write(event)
		if err != nil {
			err = fmt.Errorf("failed to encode event %v into ir: %w", event, err)
			return err
		}
	}
	return nil
}
