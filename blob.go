package mesh0

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

var maxBlobBytes int64 = 2 << 30

type BlobRef struct {
	Hash [32]byte
	Size int64
}

func (r BlobRef) String() string { return fmt.Sprintf("sha256:%x", r.Hash) }

// PutBlob writes one bounded, whole-file content-addressed blob. It must never
// return a successful reference for a truncated prefix of the supplied stream.
func (db *DB) PutBlob(reader io.Reader) (BlobRef, error) {
	if err := db.openErr(); err != nil {
		return BlobRef{}, err
	}
	temporary, err := os.CreateTemp(filepath.Join(db.path, tmpDir), "blob-")
	if err != nil {
		return BlobRef{}, err
	}
	name := temporary.Name()
	defer os.Remove(name)

	hash := sha256.New()
	written, err := io.Copy(io.MultiWriter(temporary, hash), io.LimitReader(reader, maxBlobBytes+1))
	if err == nil && written > maxBlobBytes {
		err = ErrResourceLimit
	}
	if err == nil {
		err = temporary.Sync()
	}
	if closeErr := temporary.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return BlobRef{}, err
	}

	var reference BlobRef
	copy(reference.Hash[:], hash.Sum(nil))
	reference.Size = written
	directory := filepath.Join(db.path, blobsDir, fmt.Sprintf("%02x", reference.Hash[0]))
	if err = os.MkdirAll(directory, 0700); err != nil {
		return BlobRef{}, err
	}
	destination := filepath.Join(directory, fmt.Sprintf("%x", reference.Hash))
	if info, statErr := os.Stat(destination); statErr == nil {
		if info.Size() != reference.Size {
			return BlobRef{}, ErrCorruption
		}
		return reference, nil
	} else if !errors.Is(statErr, os.ErrNotExist) {
		return BlobRef{}, statErr
	}
	if err = os.Rename(name, destination); err != nil {
		// Another writer can have committed the same content after our Stat.
		// Treat that exact content-addressed destination as deduplicated success,
		// including on Windows where Rename refuses replacement.
		if info, statErr := os.Stat(destination); statErr == nil && info.Size() == reference.Size {
			return reference, nil
		}
		return BlobRef{}, err
	}
	if err = syncDir(directory); err != nil {
		return BlobRef{}, err
	}
	return reference, nil
}

func (db *DB) OpenBlob(reference BlobRef) (io.ReadCloser, error) {
	if err := db.openErr(); err != nil {
		return nil, err
	}
	path := filepath.Join(db.path, blobsDir, fmt.Sprintf("%02x", reference.Hash[0]), fmt.Sprintf("%x", reference.Hash))
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	hash := sha256.New()
	written, err := io.Copy(hash, file)
	if err != nil || written != reference.Size || string(hash.Sum(nil)) != string(reference.Hash[:]) {
		_ = file.Close()
		return nil, ErrCorruption
	}
	if _, err = file.Seek(0, io.SeekStart); err != nil {
		_ = file.Close()
		return nil, err
	}
	return file, nil
}

// VerifyBlobs streams each file rather than reading arbitrary local content into memory.
func (db *DB) VerifyBlobs() error {
	if err := db.openErr(); err != nil {
		return err
	}
	root := filepath.Join(db.path, blobsDir)
	return filepath.Walk(root, func(path string, info os.FileInfo, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if info.IsDir() {
			return nil
		}
		file, err := os.Open(path)
		if err != nil {
			return err
		}
		hash := sha256.New()
		_, copyErr := io.Copy(hash, file)
		closeErr := file.Close()
		if copyErr != nil {
			return copyErr
		}
		if closeErr != nil {
			return closeErr
		}
		if filepath.Base(path) != fmt.Sprintf("%x", hash.Sum(nil)) {
			return ErrCorruption
		}
		return nil
	})
}
