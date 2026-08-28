package mesh0

import (
	"archive/zip"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

const (
	maxBackupEntries             = 4096
	maxBackupEntryBytes    int64 = int64(maxBatchBytes) * 128
	maxBackupExpandedBytes int64 = 512 << 20
)

// Backup creates a portable, self-contained ZIP archive at a stable snapshot
// frontier. It includes public actor-key bindings and collection-write grants
// but excludes PEER_IDENTITY, whose private signing key must remain local to
// this machine.
func (db *DB) Backup(ctx context.Context, destination string, includeBlobs bool) error {
	if destination == "" {
		return fmt.Errorf("%w: backup destination", ErrInvalidArgument)
	}
	if _, err := db.Snapshot(ctx); err != nil {
		return err
	}
	db.mu.RLock()
	if db.closed {
		db.mu.RUnlock()
		return ErrClosed
	}
	root := db.path
	db.mu.RUnlock()
	output, err := os.OpenFile(destination, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	ok := false
	defer func() {
		_ = output.Close()
		if !ok {
			_ = os.Remove(destination)
		}
	}()
	archive := zip.NewWriter(output)
	allowed := map[string]bool{identityName: true, manifestName: true, actorBindingsName: true, actorWriteGrantsName: true, segmentsDir: true, snapshotsDir: true}
	if includeBlobs {
		allowed[blobsDir] = true
	}
	err = filepath.Walk(root, func(path string, info os.FileInfo, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if info.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		if rel == lockName || rel == "" {
			return nil
		}
		first := strings.Split(filepath.ToSlash(rel), "/")[0]
		if !allowed[first] {
			return nil
		}
		if info.Size() > maxBackupEntryBytes {
			return ErrResourceLimit
		}
		entry, err := archive.Create(filepath.ToSlash(rel))
		if err != nil {
			return err
		}
		input, err := os.Open(path)
		if err != nil {
			return err
		}
		_, copyErr := io.Copy(entry, input)
		closeErr := input.Close()
		if copyErr != nil {
			return copyErr
		}
		return closeErr
	})
	if err == nil {
		err = archive.Close()
	}
	if err == nil {
		err = output.Sync()
	}
	if err != nil {
		return err
	}
	ok = true
	return nil
}

// Restore expands a checked portable archive into a new empty directory. It
// rejects traversal paths, duplicate entries, and unsupported top-level files.
func Restore(source, destination string) error {
	if source == "" || destination == "" {
		return fmt.Errorf("%w: restore path", ErrInvalidArgument)
	}
	if entries, err := os.ReadDir(destination); err == nil && len(entries) != 0 {
		return fmt.Errorf("%w: restore destination is not empty", ErrInvalidArgument)
	} else if err != nil && !os.IsNotExist(err) {
		return err
	}
	archive, err := zip.OpenReader(source)
	if err != nil {
		return err
	}
	defer archive.Close()
	if len(archive.File) > maxBackupEntries {
		return ErrResourceLimit
	}
	if err := os.MkdirAll(destination, 0700); err != nil {
		return err
	}
	seen := map[string]bool{}
	var expanded int64
	for _, entry := range archive.File {
		name := filepath.Clean(filepath.FromSlash(entry.Name))
		if name == "." || filepath.IsAbs(name) || strings.HasPrefix(name, ".."+string(filepath.Separator)) || seen[name] {
			return fmt.Errorf("%w: unsafe archive entry", ErrCorruption)
		}
		seen[name] = true
		first := strings.Split(filepath.ToSlash(name), "/")[0]
		if first != identityName && first != manifestName && first != actorBindingsName && first != actorWriteGrantsName && first != segmentsDir && first != snapshotsDir && first != blobsDir {
			return fmt.Errorf("%w: archive entry", ErrCorruption)
		}
		if entry.UncompressedSize64 > uint64(maxBackupEntryBytes) || entry.UncompressedSize64 > uint64(maxBackupExpandedBytes) || expanded > maxBackupExpandedBytes-int64(entry.UncompressedSize64) {
			return ErrResourceLimit
		}
		if entry.FileInfo().IsDir() {
			continue
		}
		output := filepath.Join(destination, name)
		if err := os.MkdirAll(filepath.Dir(output), 0700); err != nil {
			return err
		}
		input, err := entry.Open()
		if err != nil {
			return err
		}
		file, err := os.OpenFile(output, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
		if err != nil {
			_ = input.Close()
			return err
		}
		limit := maxBackupEntryBytes
		if remaining := maxBackupExpandedBytes - expanded; remaining < limit {
			limit = remaining
		}
		copied, copyErr := io.Copy(file, io.LimitReader(input, limit+1))
		closeErr, inputClose := file.Close(), input.Close()
		if copyErr != nil {
			return copyErr
		}
		if copied > limit {
			return ErrResourceLimit
		}
		expanded += copied
		if closeErr != nil {
			return closeErr
		}
		if inputClose != nil {
			return inputClose
		}
	}
	// Open validates every restored file format and leaves the destination usable.
	db, err := Open(destination, Options{})
	if err != nil {
		return err
	}
	return db.Close()
}
