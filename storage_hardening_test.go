package mesh0

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func TestStaleLockFileDoesNotBlockRecovery(t *testing.T) {
	path := t.TempDir()
	if err := os.WriteFile(filepath.Join(path, lockName), []byte("stale process metadata\n"), 0600); err != nil {
		t.Fatal(err)
	}
	db, err := Open(path, Options{})
	if err != nil {
		t.Fatalf("stale diagnostic lock must not block OS lock acquisition: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestSegmentHeaderIdentityIsValidated(t *testing.T) {
	db := newTestDB(t)
	status, err := db.Status()
	if err != nil {
		t.Fatal(err)
	}
	wrong, err := newID()
	if err != nil {
		t.Fatal(err)
	}
	_, err = scanSegment(segmentPath(db.Path(), status.ActiveSegment), DatabaseID(wrong), status.ActiveSegment, true, func(Batch) error { return nil })
	if !errors.Is(err, ErrCorruption) {
		t.Fatalf("expected foreign segment header corruption, got %v", err)
	}
}

func TestCheckpointReclaimsSegmentsAndRecovers(t *testing.T) {
	path := t.TempDir()
	db, err := Open(path, Options{MaxSegmentBytes: 1024})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		value := String(strings.Repeat("x", 850) + string(rune('a'+i)))
		mustSet(t, db, "notes", "one", "body", value)
	}
	before, err := db.LogicalDigest()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Snapshot(context.Background()); err != nil {
		t.Fatal(err)
	}
	status, err := db.Status()
	if err != nil {
		t.Fatal(err)
	}
	files, err := segmentFiles(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 1 || filepath.Base(files[0]) != filepath.Base(segmentPath(path, status.ActiveSegment)) {
		t.Fatalf("expected only active segment after checkpoint compaction, got %v", files)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(path, Options{MaxSegmentBytes: 1024})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	after, err := db.LogicalDigest()
	if err != nil {
		t.Fatal(err)
	}
	if before != after {
		t.Fatal("checkpoint recovery changed canonical digest")
	}
}

func TestBlobLimitRejectsRatherThanTruncates(t *testing.T) {
	db := newTestDB(t)
	previous := maxBlobBytes
	maxBlobBytes = 8
	t.Cleanup(func() { maxBlobBytes = previous })
	_, err := db.PutBlob(strings.NewReader("0123456789"))
	if !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("expected oversized blob rejection, got %v", err)
	}
}

func TestMetadataReplacementSurvivesRepeatedUpdatesAndCheckpoint(t *testing.T) {
	path := t.TempDir()
	db, err := Open(path, Options{MaxSegmentBytes: 1024})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		mustSet(t, db, "notes", "one", "body", String(strings.Repeat("x", 850)+string(rune('a'+i))))
	}
	if _, err := db.Snapshot(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if db, err = Open(path, Options{MaxSegmentBytes: 1024}); err != nil {
		t.Fatalf("reopen after metadata replacement: %v", err)
	}
	defer db.Close()
	if _, err := db.Status(); err != nil {
		t.Fatal(err)
	}
}

func TestRecoveryFenceBlocksPublicOperationsBeforeCallbacks(t *testing.T) {
	db := newTestDB(t)
	db.mu.Lock()
	db.failed = ErrRecoveryRequired
	db.mu.Unlock()

	called := false
	if err := db.Update(context.Background(), func(*Tx) error { called = true; return nil }); !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("Update error = %v, want recovery fence", err)
	}
	if called {
		t.Fatal("Update invoked callback after recovery fence")
	}
	checks := []struct {
		name string
		err  error
	}{
		{"View", db.View(context.Background(), func(*ReadTx) error { return nil })},
		{"ApplyRemote", db.ApplyRemote(context.Background(), Batch{})},
		{"Snapshot", func() error { _, err := db.Snapshot(context.Background()); return err }()},
		{"Subscribe", func() error { _, _, err := db.Subscribe(1); return err }()},
		{"Status", func() error { _, err := db.Status(); return err }()},
		{"History", func() error { _, err := db.History("", ""); return err }()},
		{"Documents", func() error { _, err := db.Documents(""); return err }()},
		{"Verify", db.Verify(context.Background(), false)},
		{"PutBlob", func() error { _, err := db.PutBlob(strings.NewReader("x")); return err }()},
		{"OpenBlob", func() error { _, err := db.OpenBlob(BlobRef{}); return err }()},
		{"VerifyBlobs", db.VerifyBlobs()},
		{"RotateActor", func() error { _, err := db.RotateActor(); return err }()},
	}
	for _, check := range checks {
		if !errors.Is(check.err, ErrRecoveryRequired) {
			t.Errorf("%s error = %v, want recovery fence", check.name, check.err)
		}
	}
}

func TestVerifyDoesNotMutateTornActiveTail(t *testing.T) {
	path := t.TempDir()
	db, err := Open(path, Options{})
	if err != nil {
		t.Fatal(err)
	}
	status, err := db.Status()
	if err != nil {
		t.Fatal(err)
	}
	segment := segmentPath(path, status.ActiveSegment)
	file, err := os.OpenFile(segment, os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write([]byte("torn")); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	before, err := os.Stat(segment)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Verify(context.Background(), false); !errors.Is(err, ErrCorruption) {
		t.Fatalf("Verify error = %v, want corrupt torn tail", err)
	}
	after, err := os.Stat(segment)
	if err != nil {
		t.Fatal(err)
	}
	if after.Size() != before.Size() {
		t.Fatalf("Verify mutated active WAL: before=%d after=%d", before.Size(), after.Size())
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if db, err = Open(path, Options{}); err != nil {
		t.Fatalf("Open should repair a torn active tail: %v", err)
	}
	defer db.Close()
	if err := db.Verify(context.Background(), false); err != nil {
		t.Fatalf("repaired database verify: %v", err)
	}
}

func TestConcurrentDuplicateBlobPutIsDeduplicated(t *testing.T) {
	db := newTestDB(t)
	const writers = 8
	refs := make([]BlobRef, writers)
	errs := make([]error, writers)
	var group sync.WaitGroup
	for i := range refs {
		group.Add(1)
		go func(index int) {
			defer group.Done()
			refs[index], errs[index] = db.PutBlob(strings.NewReader("identical content"))
		}(i)
	}
	group.Wait()
	for index, err := range errs {
		if err != nil {
			t.Fatalf("writer %d: %v", index, err)
		}
		if refs[index] != refs[0] {
			t.Fatalf("writer %d returned different blob reference", index)
		}
	}
	if err := db.VerifyBlobs(); err != nil {
		t.Fatal(err)
	}
}

func TestDatabaseDirectoryPermitsOnlyOneOpenProcess(t *testing.T) {
	path := t.TempDir()
	db, err := Open(path, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if second, err := Open(path, Options{}); !errors.Is(err, ErrLocked) {
		if second != nil {
			_ = second.Close()
		}
		t.Fatalf("second Open error = %v, want ErrLocked", err)
	}
}

func TestMissingManifestRecoversContiguousUncheckpointedLog(t *testing.T) {
	path := t.TempDir()
	db, err := Open(path, Options{MaxSegmentBytes: 1024})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		mustSet(t, db, "notes", "one", "body", String(strings.Repeat("x", 850)+string(rune('a'+i))))
	}
	before, err := db.LogicalDigest()
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(path, manifestName)); err != nil {
		t.Fatal(err)
	}
	if db, err = Open(path, Options{MaxSegmentBytes: 1024}); err != nil {
		t.Fatalf("recover missing manifest from contiguous WAL: %v", err)
	}
	defer db.Close()
	after, err := db.LogicalDigest()
	if err != nil {
		t.Fatal(err)
	}
	if after != before {
		t.Fatal("reconstructed manifest changed logical history")
	}
}

func TestMissingManifestWithCheckpointFailsWithoutSelectingUnsafeSnapshot(t *testing.T) {
	path := t.TempDir()
	db, err := Open(path, Options{})
	if err != nil {
		t.Fatal(err)
	}
	mustSet(t, db, "notes", "one", "body", String("checkpointed"))
	if _, err := db.Snapshot(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(path, manifestName)); err != nil {
		t.Fatal(err)
	}
	if recovered, err := Open(path, Options{}); !errors.Is(err, ErrCorruption) {
		if recovered != nil {
			_ = recovered.Close()
		}
		t.Fatalf("Open error = %v, want corruption instead of unsafe snapshot choice", err)
	}
}
