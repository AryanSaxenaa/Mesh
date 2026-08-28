package mesh0

import (
	"archive/zip"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestRestoreRejectsExcessiveEntryCountBeforeExtraction(t *testing.T) {
	archivePath := filepath.Join(t.TempDir(), "oversized.zip")
	output, err := os.Create(archivePath)
	if err != nil {
		t.Fatal(err)
	}
	archive := zip.NewWriter(output)
	for index := 0; index < maxBackupEntries+1; index++ {
		entry, err := archive.Create(fmt.Sprintf("segments/%08d.seg", index))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := entry.Write([]byte("x")); err != nil {
			t.Fatal(err)
		}
	}
	if err := archive.Close(); err != nil {
		t.Fatal(err)
	}
	if err := output.Close(); err != nil {
		t.Fatal(err)
	}
	if err := Restore(archivePath, filepath.Join(t.TempDir(), "restored")); !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("oversized archive error = %v, want resource limit", err)
	}
}
