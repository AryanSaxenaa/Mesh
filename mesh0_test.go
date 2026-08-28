package mesh0

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
)

func newTestDB(t *testing.T) *DB {
	t.Helper()
	db, err := Open(t.TempDir(), Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}
func mustSet(t *testing.T, db *DB, collection, id, field string, value Value) {
	t.Helper()
	if err := db.Update(context.Background(), func(tx *Tx) error { return tx.Document(collection, id).Set(field, value) }); err != nil {
		t.Fatal(err)
	}
}

// syncAuthorizedPair is test-only setup. Production pairing is deliberately
// default-deny and requires the administrator to grant every collection.
func syncAuthorizedPair(t *testing.T, left, right *DB, collections ...string) {
	t.Helper()
	leftKey, err := left.PeerPublicKey()
	if err != nil {
		t.Fatal(err)
	}
	rightKey, err := right.PeerPublicKey()
	if err != nil {
		t.Fatal(err)
	}
	if err := left.TrustPeer("right", rightKey); err != nil {
		t.Fatal(err)
	}
	if err := right.TrustPeer("left", leftKey); err != nil {
		t.Fatal(err)
	}
	if err := left.BindPeerActor(right.ActorID(), rightKey); err != nil {
		t.Fatal(err)
	}
	if err := right.BindPeerActor(left.ActorID(), leftKey); err != nil {
		t.Fatal(err)
	}
	for _, collection := range collections {
		if err := left.GrantPeerCollectionWrite(right.ActorID(), collection); err != nil {
			t.Fatal(err)
		}
		if err := right.GrantPeerCollectionWrite(left.ActorID(), collection); err != nil {
			t.Fatal(err)
		}
	}
	if err := SyncPair(context.Background(), left, right); err != nil {
		t.Fatal(err)
	}
}

func TestObservedAssignmentConvergesAndResolves(t *testing.T) {
	ctx := context.Background()
	left := newTestDB(t)
	mustSet(t, left, "tasks", "42", "status", String("draft"))
	archive := filepath.Join(t.TempDir(), "seed.zip")
	if err := left.Backup(ctx, archive, false); err != nil {
		t.Fatal(err)
	}
	rightPath := filepath.Join(t.TempDir(), "right")
	if err := Restore(archive, rightPath); err != nil {
		t.Fatal(err)
	}
	right, err := Open(rightPath, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer right.Close()
	if _, err := right.RotateActor(); err != nil {
		t.Fatal(err)
	}
	mustSet(t, left, "tasks", "42", "status", String("ready"))
	mustSet(t, right, "tasks", "42", "status", String("blocked"))
	syncAuthorizedPair(t, left, right, "tasks")
	if err := left.View(ctx, func(read *ReadTx) error {
		document, ok := read.Document("tasks", "42")
		if !ok {
			t.Fatal("document absent")
		}
		if values := document.Values("status"); len(values) != 2 {
			t.Fatalf("want two concurrent values, got %#v", values)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	mustSet(t, left, "tasks", "42", "status", String("ready"))
	syncAuthorizedPair(t, left, right, "tasks")
	if err := right.View(ctx, func(read *ReadTx) error {
		document, _ := read.Document("tasks", "42")
		values := document.Values("status")
		if len(values) != 1 || !values[0].Equal(String("ready")) {
			t.Fatalf("resolution did not supersede conflict: %#v", values)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestObservedRemovePreservesOtherMembers(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	if err := db.Update(ctx, func(tx *Tx) error {
		document := tx.Document("project", "launch")
		if err := document.SetAdd("members", String("alice")); err != nil {
			return err
		}
		return document.SetAdd("members", String("bob"))
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.Update(ctx, func(tx *Tx) error { return tx.Document("project", "launch").SetRemove("members", String("alice")) }); err != nil {
		t.Fatal(err)
	}
	if err := db.View(ctx, func(read *ReadTx) error {
		document, _ := read.Document("project", "launch")
		members := document.Set("members")
		if len(members) != 1 || !members[0].Equal(String("bob")) {
			t.Fatalf("unexpected members: %#v", members)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestSnapshotRestartAndBlobVerification(t *testing.T) {
	ctx := context.Background()
	path := t.TempDir()
	db, err := Open(path, Options{})
	if err != nil {
		t.Fatal(err)
	}
	mustSet(t, db, "notes", "one", "title", String("offline"))
	ref, err := db.PutBlob(strings.NewReader("attachment"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Snapshot(ctx); err != nil {
		t.Fatal(err)
	}
	before, err := db.LogicalDigest()
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(path, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	after, err := db.LogicalDigest()
	if err != nil {
		t.Fatal(err)
	}
	if before != after {
		t.Fatal("snapshot restart digest changed")
	}
	if err := db.Verify(ctx, true); err != nil {
		t.Fatal(err)
	}
	reader, err := db.OpenBlob(ref)
	if err != nil {
		t.Fatal(err)
	}
	_ = reader.Close()
}

func TestActorForkIsRejected(t *testing.T) {
	actorID, _ := newID()
	actor := ActorID(actorID)
	first := Batch{First: Dot{Actor: actor, Seq: 1}, Count: 1, Dependencies: VersionVector{}, Operations: []Operation{{Dot: Dot{Actor: actor, Seq: 1}, Document: DocumentKey{"c", "d"}, Path: []string{"x"}, Action: MapAssign, Value: String("one")}}}
	fork := first
	fork.Operations = append([]Operation(nil), first.Operations...)
	fork.Operations[0].Value = String("two")
	root := newState()
	root, err := root.apply(first)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := root.apply(fork); !errors.Is(err, ErrActorFork) {
		t.Fatalf("want actor fork error, got %v", err)
	}
}

func TestConcurrentPermutationDigest(t *testing.T) {
	leftID, _ := newID()
	rightID, _ := newID()
	left := Batch{First: Dot{Actor: ActorID(leftID), Seq: 1}, Count: 1, Dependencies: VersionVector{}, Operations: []Operation{{Dot: Dot{Actor: ActorID(leftID), Seq: 1}, Document: DocumentKey{"c", "d"}, Path: []string{"v"}, Action: MapAssign, Value: String("left")}}}
	right := Batch{First: Dot{Actor: ActorID(rightID), Seq: 1}, Count: 1, Dependencies: VersionVector{}, Operations: []Operation{{Dot: Dot{Actor: ActorID(rightID), Seq: 1}, Document: DocumentKey{"c", "d"}, Path: []string{"v"}, Action: MapAssign, Value: String("right")}}}
	a := newState()
	var err error
	if a, err = a.apply(left); err != nil {
		t.Fatal(err)
	}
	if a, err = a.apply(right); err != nil {
		t.Fatal(err)
	}
	b := newState()
	if b, err = b.apply(right); err != nil {
		t.Fatal(err)
	}
	if b, err = b.apply(left); err != nil {
		t.Fatal(err)
	}
	if a.digest(DatabaseID{}) != b.digest(DatabaseID{}) {
		t.Fatal("concurrent operation order changed canonical digest")
	}
}

func FuzzBatchDecoderNeverPanics(f *testing.F) {
	f.Add([]byte("M0BT"))
	f.Fuzz(func(t *testing.T, input []byte) { _, _ = UnmarshalBatch(input) })
}
