package mesh0

import (
	"context"
	"errors"
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
	batches, err := db.batchesForRanges([]actorRange{{actor: actor, first: 2, last: 2}})
	if err != nil {
		t.Fatal(err)
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

func TestRangeSelectionRejectsUnboundedResponse(t *testing.T) {
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
	_, err := db.batchesForRanges([]actorRange{{actor: actor, first: 1, last: maxSyncResponseBatches + 1}})
	if !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("unbounded response error = %v", err)
	}
}
