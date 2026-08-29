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
	batches := db.batchesForRanges([]actorRange{{actor: actor, first: 2, last: 2}})
	if len(batches) != 1 || batches[0].Count != 2 || batches[0].First.Seq != 1 {
		t.Fatalf("range split atomic batch: %#v", batches)
	}
}
