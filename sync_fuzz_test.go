package mesh0

import (
	"bufio"
	"bytes"
	"crypto/ed25519"
	"testing"
)

func FuzzSyncDecodersNeverPanic(f *testing.F) {
	f.Add([]byte("M0HL"))
	f.Add(encodeActorRanges(nil))
	f.Add(encodeDigest(VersionVector{}, [32]byte{}))
	f.Add([]byte{1})
	f.Fuzz(func(t *testing.T, input []byte) {
		_, _ = decodeHello(input)
		_, _ = decodeActorRanges(input)
		_, _, _ = decodeDigest(input)
		_ = decodeWireError(input)
		_, _, _ = readWireFrame(bufio.NewReader(bytes.NewReader(input)))
		_, _ = decodeSignedBatch(input, make(ed25519.PublicKey, ed25519.PublicKeySize), DatabaseID{})
	})
}

func FuzzEqualityIndexValueKeysNeverPanic(f *testing.F) {
	f.Add(byte(0), []byte(nil))
	f.Add(byte(StringValue), []byte("value"))
	f.Fuzz(func(t *testing.T, kind byte, raw []byte) {
		value := Value{Kind: ValueKind(kind), Bytes: append([]byte(nil), raw...)}
		_, _ = equalityValueKey(value)
		_ = validateEqualityIndex(EqualityIndex{Collection: string(raw), Path: string(raw)})
	})
}

func FuzzDirectRangeDigestInputsNeverPanic(f *testing.F) {
	f.Add(uint64(1), uint64(1))
	f.Add(uint64(0), uint64(1))
	f.Add(uint64(2), uint64(1))
	f.Fuzz(func(t *testing.T, first, last uint64) {
		db := newTestDB(t)
		_, _ = db.DirectRangeDigest(db.ActorID(), first, last)
	})
}

func FuzzRangeDigestNodeDecodersNeverPanic(f *testing.F) {
	f.Add(encodeRangeDigestNode(rangeDigestNode{}))
	f.Add(encodeRangeDigestNodes(nil))
	f.Add([]byte{1})
	f.Fuzz(func(t *testing.T, input []byte) {
		_, _ = decodeRangeDigestNode(input)
		_, _ = decodeRangeDigestNodes(input)
	})
}

func FuzzRangeDigestNodeForInputsNeverPanic(f *testing.F) {
	f.Add(uint64(1), uint64(1))
	f.Add(uint64(0), uint64(1))
	f.Add(uint64(2), uint64(1))
	f.Add(uint64(1), rangeDigestLeafSpan+1)
	f.Fuzz(func(t *testing.T, first, last uint64) {
		db := newTestDB(t)
		_, _ = db.rangeDigestNodeFor(db.ActorID(), first, last)
	})
}
