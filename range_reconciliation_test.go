package mesh0

import (
	"bufio"
	"bytes"
	"context"
	"crypto/ed25519"
	"errors"
	"path/filepath"
	"strings"
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
	ranges := missingActorRanges(local, remote, ActorID(peer))
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

func TestSyncHelloNegotiatesRequiredCapabilities(t *testing.T) {
	database, err := newID()
	if err != nil {
		t.Fatal(err)
	}
	actor, err := newID()
	if err != nil {
		t.Fatal(err)
	}
	hello := syncHello{
		database:     DatabaseID(database),
		actor:        ActorID(actor),
		public:       make(ed25519.PublicKey, ed25519.PublicKeySize),
		capabilities: syncSupportedCapabilities | (1 << 17),
	}
	decoded, err := decodeHello(hello.encode())
	if err != nil {
		t.Fatal(err)
	}
	if decoded.database != hello.database || decoded.actor != hello.actor || string(decoded.public) != string(hello.public) || decoded.capabilities != hello.capabilities {
		t.Fatalf("hello round trip = %#v", decoded)
	}
	negotiated, err := negotiateSyncCapabilities(syncSupportedCapabilities, decoded.capabilities)
	if err != nil || negotiated != syncSupportedCapabilities {
		t.Fatalf("negotiated capabilities = %b, %v", negotiated, err)
	}
	if _, err := negotiateSyncCapabilities(syncSupportedCapabilities, 0); !errors.Is(err, ErrProtocolIncompatible) {
		t.Fatalf("missing capability error = %v", err)
	}
	var legacy encoder
	legacy.raw([]byte("M0HL"))
	legacy.u(syncProtocolGeneration - 1)
	legacy.id(database)
	legacy.id(actor)
	legacy.bytes(hello.public)
	if _, err := decodeHello(legacy.Bytes()); !errors.Is(err, ErrProtocolIncompatible) {
		t.Fatalf("legacy hello error = %v", err)
	}
	truncated := hello.encode()[:len(hello.encode())-1]
	if _, err := decodeHello(truncated); !errors.Is(err, ErrCorruption) {
		t.Fatalf("truncated capability error = %v", err)
	}
}

func TestMissingActorRangesPagesOversizedSuffix(t *testing.T) {
	peer, err := newID()
	if err != nil {
		t.Fatal(err)
	}
	pageEnd := uint64(maxSyncRanges) * maxSyncRangeSpan
	ranges := missingActorRanges(nil, VersionVector{ActorID(peer): pageEnd + 1}, ActorID(peer))
	if len(ranges) != maxSyncRanges {
		t.Fatalf("range page count = %d, want %d", len(ranges), maxSyncRanges)
	}
	if ranges[0] != (actorRange{actor: ActorID(peer), first: 1, last: maxSyncRangeSpan}) || ranges[len(ranges)-1].last != pageEnd {
		t.Fatalf("range page boundaries = first %#v, last %#v", ranges[0], ranges[len(ranges)-1])
	}
}

func TestWireErrorsAreTypedAndCanonical(t *testing.T) {
	for _, expected := range []error{ErrProtocolIncompatible, ErrAuthorizationDenied, ErrCausalGap, ErrResourceLimit} {
		var wire bytes.Buffer
		signalWireError(&wire, expected)
		kind, payload, err := readWireFrame(bufio.NewReader(&wire))
		if err != nil || kind != wireError {
			t.Fatalf("wire error frame = kind %d, err %v", kind, err)
		}
		if decoded := decodeWireError(payload); !errors.Is(decoded, expected) {
			t.Fatalf("wire error %v decoded as %v", expected, decoded)
		}
	}
	if err := decodeWireError([]byte{0}); !errors.Is(err, ErrCorruption) {
		t.Fatalf("unknown wire error = %v", err)
	}
	if err := decodeWireError([]byte{byte(wireErrorProtocolIncompatible), 0}); !errors.Is(err, ErrCorruption) {
		t.Fatalf("noncanonical wire error = %v", err)
	}
}

func TestRangeSelectionPaginatesByCanonicalBytes(t *testing.T) {
	db := newTestDB(t)
	actor := db.ActorID()
	payload := String(strings.Repeat("x", maxStringBytes))
	const batchCount = 65
	db.mu.Lock()
	for seq := uint64(1); seq <= batchCount; seq++ {
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
				Value:    payload,
			}},
		}
	}
	db.mu.Unlock()
	batches, more, err := db.batchesForRanges([]actorRange{{actor: actor, first: 1, last: batchCount}})
	if err != nil {
		t.Fatal(err)
	}
	if !more || len(batches) == 0 || len(batches) >= batchCount {
		t.Fatalf("byte page = %d batches, more=%v", len(batches), more)
	}
	total := 0
	for index, batch := range batches {
		if batch.First.Seq != uint64(index+1) {
			t.Fatalf("byte page batch %d sequence = %d", index, batch.First.Seq)
		}
		raw, err := batch.MarshalBinary()
		if err != nil {
			t.Fatal(err)
		}
		total += len(raw)
	}
	if total > maxSyncResponseBytes {
		t.Fatalf("byte page payload = %d", total)
	}
}

func TestDirectRangeDigestIsCanonicalAndWholeBatch(t *testing.T) {
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
	whole, err := db.DirectRangeDigest(actor, 1, 2)
	if err != nil {
		t.Fatal(err)
	}
	partial, err := db.DirectRangeDigest(actor, 2, 2)
	if err != nil {
		t.Fatal(err)
	}
	if whole == partial {
		t.Fatal("range identity was not bound into digest")
	}
	again, err := db.DirectRangeDigest(actor, 2, 2)
	if err != nil || again != partial {
		t.Fatalf("digest is not stable: %x, %v", again, err)
	}
	if _, err := db.DirectRangeDigest(actor, 0, 1); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("invalid digest range error = %v", err)
	}
}
