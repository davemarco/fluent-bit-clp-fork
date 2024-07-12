package flush

import (
	"encoding/gob"
	"fmt"
	"github.com/y-scope/fluent-bit-clp/internal/outctx"
	"log"
	"os"
	"path/filepath"
	"time"
	"github.com/y-scope/fluent-bit-clp/internal/irzstd"
	"slices"
)

type TagRecovery struct {
	Key     string
	Index   int
	Start   time.Time
	ModTime time.Time
}

// perhaps safer not to send to s3.
// will terminate all IR streams
func gracefulExit(ctx *outctx.S3Context)  {

	for _, tag := range ctx.Tags {
		if tag.Writer == nil {
			continue
		}

		if tag.Writer.IrStore != nil {
			irFile, ok := tag.IrStore.(*os.File)
			if !ok {
				log.Printf("error type assertion from store to file failed")
				continue
			}
			irFileName := irFile.Name()
			err := irFile.Close()
			log.Printf("error could not close file %s: %w", irFileName, err)
		}
		if tag.Writer.ZstdStore != nil {
			zstdFile, ok := tag.ZstdStore.(*os.File)
			if !ok {
				log.Printf("error type assertion from store to file failed")
				continue
			}
			zstdFileName := zstdFile.Name()
			err := zstdFile.Close()
			log.Printf("error could not close file %s: %w", zstdFileName, err)
		}
	}
}

func recoverOnStart(ctx *outctx.S3Context) error {
	irStoreDir := filepath.Join(ctx.Config.StoreDir, irzstd.IrDir)
	zstdStoreDir := filepath.Join(ctx.Config.StoreDir, irzstd.ZstdDir)

	irFiles, err := os.ReadDir(irStoreDir)
	if os.IsNotExist(err) {
		log.Printf("Existing storage directory %s not found during startup", irStoreDir)
		return nil
	} else if err != nil {
		// Handle other errors
		return fmt.Errorf("error reading directory '%s'. Deleting directory will silence error "+
		"but destory recovered logs: %w", irStoreDir, err)
	}

	var irFileNames []string

	for _, file := range irFiles {
		fileName := file.Name()
		fileInfo, err := file.Info()
		if err != nil {
			return fmt.Errorf("error retrieving file info '%s'. Deleting file will silence error "+
			"but destory recovered logs: %w", fileName, err)
		}

		// skip non-store files (directories, etc..)
		if fileInfo.Mode().IsRegular() == false {
			continue
		}

		size := int(fileInfo.Size())

		if size == 0 {
			filePath := filepath.Join(irStoreDir, fileName)
			err := os.Remove(filePath)
			if err != nil {
				return fmt.Errorf("error deleting file '%s'. Deleting file manually will silence error "+
				": %w", fileName, err)
			}
			continue
		}

		irFileNames = append(irFileNames,fileName)

		zstdPath := filepath.Join(zstdStoreDir, fileName)

		zstdStore, err := os.Open(zstdPath)

		if (os.IsNotExist(err)) {
			zstdStore, err = CreateFile(zstdStoreDir,fileName)
			if err != nil {
				return fmt.Errorf("error creating file %s: %w",fileName, err)
			}
		}

		irPath := filepath.Join(irStoreDir, fileName)
		irStore, err := os.Open(irPath)
		if err != nil {
			return fmt.Errorf("error opening ir file %s: %w",fileName, err)
		}

		tag, err := newTag(fileName,size,ctx,irStore,zstdStore)
		if err != nil {
			return  fmt.Errorf("error creating tag: %w", err)
		}

		ctx.Tags[fileName] = tag

		err = tag.Writer.FlushIrStore()
		if err != nil {
			return  fmt.Errorf("error flushing IR store: %w", err)
		}

		err = FlushZstdToS3(tag,ctx)
		if err != nil {
			return fmt.Errorf("error flushing zstdStore to s3: %w", err)
		}
	}

	zstdFiles, err := os.ReadDir(zstdStoreDir)

	if os.IsNotExist(err) {
		log.Printf("Existing storage directory %s not found during startup", zstdStoreDir)
		return nil
	} else if err != nil {
		// Handle other errors
		return fmt.Errorf("error reading directory '%s'. Deleting directory will silence error "+
		"but destory recovered logs: %w", zstdStoreDir, err)
	}

	for _, file := range zstdFiles {
		fileName := file.Name()
		fileInfo, err := file.Info()
		if err != nil {
			return fmt.Errorf("error retrieving file info '%s'. Deleting file will silence error "+
			"but destory recovered logs: %w", fileName, err)
		}

		// skip non-store files (directories, etc..)
		if fileInfo.Mode().IsRegular() == false {
			continue
		}

		size := int(fileInfo.Size())

		if size == 0 {
			filePath := filepath.Join(irStoreDir, fileName)
			err := os.Remove(filePath)
			if err != nil {
				return fmt.Errorf("error deleting file '%s'. Deleting file manually will silence error "+
				": %w", fileName, err)
			}
			continue
		}

		_, found := slices.BinarySearch(irFileNames, fileName)
		if found {
			continue
		}

		
























		//DELETE METADATA FILE

		//check if valid IR?

		//create tag

		//send to s3
		//increment index
	}

	return nil
}


func removeEmptyFiles(string *outctx.S3Context) error {

}