package irzstd

// using irzstd to prevent namespace collision with [ir].

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"os"
	"path/filepath"

	"github.com/fluent/fluent-bit-go/output"
	"github.com/klauspost/compress/zstd"

	"github.com/y-scope/clp-ffi-go/ffi"
	"github.com/y-scope/clp-ffi-go/ir"
)

// 2 MB
const irSizeThreshold = 2 << 20


// Converts log events into Zstd compressed IR. Effectively chains [ir.Writer] then [zstd.Encoder] in series.
// Compressed IR output is [io.Writer] provided to [zstd.Encoder].
type IrZstdWriter struct {
	Store        bool
	Size         int
	Timezone     string
	IrTotalBytes int
	IrStore      io.ReadWriter
	ZstdStore    io.ReadWriter
	IrWriter     *ir.Writer
	ZstdWriter   *zstd.Encoder
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
func NewIrZstdWriter(timezone string, size int, store bool, irStore io.ReadWriter, zstdStore io.ReadWriter) (*IrZstdWriter, error) {

	// Create Zstd writer with zstdStore as its output.
	zstdWriter, err := zstd.NewWriter(zstdStore)
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
		Store:      store,
		Size:       size,
		Timezone:   timezone,
		IrStore:    irStore,
		ZstdStore:  zstdStore,
		IrWriter:   irWriter,
		ZstdWriter: zstdWriter,
	}

	return &IrZstdWriter, nil
}

// TODO: Improve error handling for partially written bytes.
func (w *IrZstdWriter) WriteIrZstd(logEvents []ffi.LogEvent) (int, error) {

	// Write log events to irWriter buffer.
	err := writeIr(w.IrWriter, logEvents)
	if err != nil {
		return output.FLB_ERROR, err
	}

	// If no disk store, skip writing to ir store.
	if !w.Store {
		// Flush irWriter buffer to zstdWriter.
		_, err := w.IrWriter.WriteTo(w.ZstdWriter)
		if err != nil {
			return output.FLB_RETRY, err
		}

		return output.FLB_OK, nil
	}

	// Flush irWriter buffer to ir disk store.
	numBytes, err := w.IrWriter.WriteTo(w.IrStore)
	if err != nil {
		return output.FLB_RETRY, err
	}

	// Increment total bytes written.
	w.IrTotalBytes += int(numBytes)

	// If total bytes greater than ir threshold, compress IR into Zstd frame. Else keep
	// accumulating IR in store until threshold is reached.
	if (w.IrTotalBytes) >= irSizeThreshold {
		err := w.FlushIrStore()
		if err != nil {
			return output.FLB_ERROR, fmt.Errorf("error flushing IR store: %w", err)
		}
	}

	return output.FLB_OK, nil
}

func (w *IrZstdWriter) EndStream() error {
	// Null terminate ir stream and flush to zstdwriter
	_, err := w.IrWriter.CloseTo(w.ZstdWriter)
	if err != nil {
		return err
	}

	err = w.ZstdWriter.Close()
	if err != nil {
		return err
	}

	return nil
}

func (w *IrZstdWriter) Reset() error {
	// Make a new IR writer to get a new preamble.
	var err error
	w.IrWriter, err = ir.NewWriterSize[ir.FourByteEncoding](w.Size, w.Timezone)
	if err != nil {
		return err
	}

	if !w.Store {
		buf, ok := w.ZstdStore.(*bytes.Buffer)
		if !ok {
			return fmt.Errorf("error type assertion from store to buf failed")
		}
		buf.Reset()
		return nil
	}

	zstdFile, ok := w.ZstdStore.(*os.File)
	if !ok {
		return  fmt.Errorf("error type assertion from store to file failed")
	}

	// Reset Zstd Store.
	err = zstdFile.Truncate(0)
	if err != nil {
		return err
	}

	// Re-initialize Zstd writer to recieve more input.
	w.ZstdWriter.Reset(w.ZstdStore)

	return nil
}

func (w *IrZstdWriter) GetZstdStoreSize() (int, error) {
	fileInfo, err := w.ZstdStore.(*os.File).Stat()
	if err != nil {
		return 0, err
	}
	return int(fileInfo.Size()), nil
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

func (w *IrZstdWriter) FlushIrStore() (error) {

	if w.IrStore == nil {
		return fmt.Errorf("error flush called on non-existant ir store")
	}

	_, err := io.Copy(w.ZstdWriter, w.IrStore)
	if err != nil {
		return err
	}

	// Close zstd Frame.
	err = w.ZstdWriter.Close()
	if err != nil {
		return err
	}

	// Re-initialize Zstd writer to recieve more input.
	w.ZstdWriter.Reset(w.ZstdStore)

	irFile, ok := w.IrStore.(*os.File)
	if !ok {
		return fmt.Errorf("error type assertion from store to file failed")
	}

	// Reset ir Store.
	err = irFile.Truncate(0)
	if err != nil {
		return err
	}
	w.IrTotalBytes = 0

	return nil
}




