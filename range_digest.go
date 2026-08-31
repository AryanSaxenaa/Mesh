package mesh0

import (
	"crypto/sha256"
	"fmt"
)

const rangeDigestGeneration uint64 = 1

// DirectRangeDigest returns a deterministic, domain-separated digest of whole
// atomic batches overlapping one direct actor interval. It is derived from
// canonical retained history only; it never authorizes or applies data.
func (db *DB) DirectRangeDigest(actor ActorID, first, last uint64) ([32]byte, error) {
	if ID(actor).IsZero() || first == 0 || last < first || last-first >= maxSyncRangeSpan {
		return [32]byte{}, ErrInvalidArgument
	}
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return [32]byte{}, ErrClosed
	}
	if db.failed != nil {
		return [32]byte{}, db.failed
	}
	batches := make([]Batch, 0)
	for _, batch := range db.state.Batches {
		end := batch.First.Seq + uint64(batch.Count) - 1
		if batch.First.Actor == actor && batch.First.Seq <= last && end >= first {
			batches = append(batches, batch)
		}
	}
	BatchSort(batches)
	var encoded encoder
	encoded.raw([]byte("M0RD"))
	encoded.u(rangeDigestGeneration)
	encoded.id(ID(db.manifest.Database))
	encoded.id(ID(actor))
	encoded.u(first)
	encoded.u(last)
	encoded.u(uint64(len(batches)))
	for _, batch := range batches {
		raw, err := batch.MarshalBinary()
		if err != nil {
			return [32]byte{}, fmt.Errorf("range digest: %w", err)
		}
		encoded.bytes(raw)
	}
	return sha256.Sum256(encoded.Bytes()), nil
}

// rangeDigestNode is a bounded direct-actor digest-tree leaf/interval.
type rangeDigestNode struct {
	actor       ActorID
	first, last uint64
	digest      [32]byte
}

func encodeRangeDigestNode(node rangeDigestNode) []byte {
	var encoded encoder
	encoded.id(ID(node.actor))
	encoded.u(node.first)
	encoded.u(node.last)
	encoded.raw(node.digest[:])
	return encoded.Bytes()
}

func decodeRangeDigestNode(data []byte) (rangeDigestNode, error) {
	decoded := decoder{b: data}
	actor, err := decoded.id()
	if err != nil || ID(actor).IsZero() {
		return rangeDigestNode{}, ErrCorruption
	}
	first, err := decoded.u()
	if err != nil || first == 0 {
		return rangeDigestNode{}, ErrCorruption
	}
	last, err := decoded.u()
	if err != nil || last < first || last-first >= maxSyncRangeSpan {
		return rangeDigestNode{}, ErrCorruption
	}
	raw, err := decoded.raw(32)
	if err != nil || decoded.done() != nil {
		return rangeDigestNode{}, ErrCorruption
	}
	var digest [32]byte
	copy(digest[:], raw)
	return rangeDigestNode{actor: ActorID(actor), first: first, last: last, digest: digest}, nil
}
