package mesh0

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
)

func TestActorRangeCodecIsCanonicalAndBounded(t *testing.T) {
	first, err := newID()
	if err != nil {
		t.Fatal(err)
	}
	second, err := newID()
	if err != nil {
		t.Fatal(err)
	}
	ranges := []actorRange{{actor: ActorID(first), first: 3, last: 9}, {actor: ActorID(second), first: 1, last: 2}}
	// The wire form requires actor ordering, so sort the test fixture via the
	// production missing-range helper's canonical ordering rule.
	if idCompare(ID(ranges[0].actor), ID(ranges[1].actor)) > 0 {
		ranges[0], ranges[1] = ranges[1], ranges[0]
	}
	decoded, err := decodeActorRanges(encodeActorRanges(ranges))
	if err != nil || len(decoded) != len(ranges) {
		t.Fatalf("range round trip = %#v, %v", decoded, err)
	}
	if _, err := decodeActorRanges([]byte{1}); !errors.Is(err, ErrCorruption) {
		t.Fatalf("truncated range error = %v", err)
	}
	oversized := []actorRange{{actor: ranges[0].actor, first: 1, last: maxSyncRangeSpan + 1}}
	if _, err := decodeActorRanges(encodeActorRanges(oversized)); !errors.Is(err, ErrCorruption) {
		t.Fatalf("oversized range error = %v", err)
	}
}

func TestRangeSelectionKeepsWholeBatch(t *testing.T) {
	db := newTestDB(t)
	if err := db.Update(context.Background(), func(tx *Tx) error {
		if err := tx.Document("tasks", "one").Set("a", String("one")); err != nil {
			return err
		}
		return tx.Document("tasks", "one").Set("b", String("two"))
	}); err != nil {
		t.Fatal(err)
	}
	actor := db.ActorID()
	batches, more, err := db.batchesForRanges([]actorRange{{actor: actor, first: 2, last: 2}})
	if err != nil {
		t.Fatal(err)
	}
	if more {
		t.Fatal("single matching batch reported continuation")
	}
	if len(batches) != 1 || batches[0].Count != 2 || batches[0].First.Seq != 1 {
		t.Fatalf("range split atomic batch: %#v", batches)
	}
}

func TestMissingActorRangesOnlyRequestsDirectPeer(t *testing.T) {
	peer, err := newID()
	if err != nil {
		t.Fatal(err)
	}
	foreign, err := newID()
	if err != nil {
		t.Fatal(err)
	}
	local := VersionVector{ActorID(peer): 4}
	remote := VersionVector{
		ActorID(peer):    maxSyncRangeSpan + 6,
		ActorID(foreign): 99,
	}
	ranges, err := missingActorRanges(local, remote, ActorID(peer))
	if err != nil {
		t.Fatal(err)
	}
	if len(ranges) != 2 {
		t.Fatalf("range count = %d, want 2", len(ranges))
	}
	if ranges[0] != (actorRange{actor: ActorID(peer), first: 5, last: maxSyncRangeSpan + 4}) ||
		ranges[1] != (actorRange{actor: ActorID(peer), first: maxSyncRangeSpan + 5, last: maxSyncRangeSpan + 6}) {
		t.Fatalf("direct peer ranges = %#v", ranges)
	}
}

func TestRangeSelectionPaginatesBoundedResponse(t *testing.T) {
	db := newTestDB(t)
	actor := db.ActorID()
	db.mu.Lock()
	for seq := uint64(1); seq <= maxSyncResponseBatches+1; seq++ {
		dot := Dot{Actor: actor, Seq: seq}
		db.state.Batches[dot] = Batch{
			First:        dot,
			Count:        1,
			Dependencies: VersionVector{},
			Operations: []Operation{{
				Dot:      dot,
				Document: DocumentKey{Collection: "tasks", ID: "one"},
				Path:     []string{"value"},
				Action:   MapAssign,
				Value:    String("value"),
			}},
		}
	}
	db.mu.Unlock()
	batches, more, err := db.batchesForRanges([]actorRange{{actor: actor, first: 1, last: maxSyncResponseBatches + 1}})
	if err != nil {
		t.Fatal(err)
	}
	if !more || len(batches) != maxSyncResponseBatches {
		t.Fatalf("bounded response = %d batches, more=%v", len(batches), more)
	}
	for index, batch := range batches {
		if batch.First.Seq != uint64(index+1) {
			t.Fatalf("page batch %d sequence = %d", index, batch.First.Seq)
		}
	}
}

func TestActorRangeCodecRejectsMalformedRequests(t *testing.T) {
	first, err := newID()
	if err != nil {
		t.Fatal(err)
	}
	second, err := newID()
	if err != nil {
		t.Fatal(err)
	}
	if idCompare(first, second) > 0 {
		first, second = second, first
	}
	valid := actorRange{actor: ActorID(first), first: 1, last: 1}
	cases := []struct {
		name string
		data []byte
	}{
		{"zero actor", encodeActorRanges([]actorRange{{first: 1, last: 1}})},
		{"zero sequence", encodeActorRanges([]actorRange{{actor: ActorID(first), last: 1}})},
		{"duplicate actor", encodeActorRanges([]actorRange{valid, {actor: ActorID(first), first: 2, last: 2}})},
		{"descending actor", encodeActorRanges([]actorRange{{actor: ActorID(second), first: 1, last: 1}, valid})},
		{"trailing bytes", append(encodeActorRanges([]actorRange{valid}), 0)},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			if _, err := decodeActorRanges(test.data); !errors.Is(err, ErrCorruption) {
				t.Fatalf("decode error = %v", err)
			}
		})
	}
	var encoded encoder
	encoded.u(maxSyncRanges + 1)
	if _, err := decodeActorRanges(encoded.Bytes()); !errors.Is(err, ErrCorruption) {
		t.Fatalf("excessive count error = %v", err)
	}
}

func TestDirectRangeSyncConvergesAcrossResponsePages(t *testing.T) {
	source := newTestDB(t)
	archive := filepath.Join(t.TempDir(), "seed.zip")
	if err := source.Backup(context.Background(), archive, false); err != nil {
		t.Fatal(err)
	}
	destinationPath := filepath.Join(t.TempDir(), "destination")
	if err := Restore(archive, destinationPath); err != nil {
		t.Fatal(err)
	}
	destination, err := Open(destinationPath, Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = destination.Close() })
	if _, err := destination.RotateActor(); err != nil {
		t.Fatal(err)
	}
	syncAuthorizedPair(t, source, destination, "tasks")
	for index := 0; index < maxSyncResponseBatches+1; index++ {
		if err := source.Update(context.Background(), func(tx *Tx) error {
			return tx.Document("tasks", "one").Set("value", Int(int64(index)))
		}); err != nil {
			t.Fatalf("write %d: %v", index, err)
		}
	}
	if err := SyncPair(context.Background(), source, destination); err != nil {
		t.Fatalf("paginated sync: %v", err)
	}
	sourceStatus, err := source.Status()
	if err != nil {
		t.Fatal(err)
	}
	destinationStatus, err := destination.Status()
	if err != nil {
		t.Fatal(err)
	}
	if sourceStatus.Frontier.Compare(destinationStatus.Frontier) != ClockEqual || sourceStatus.LogicalDigest != destinationStatus.LogicalDigest {
		t.Fatal("paginated range sync did not converge")
	}
}
