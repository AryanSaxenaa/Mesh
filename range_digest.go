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

// Fixed, protocol-level constants for the negotiated range-digest exchange.
// Fanout and leaf span are deliberately small and fixed so descent depth for
// any one bounded segment is a small constant, never attacker-influenced.
const (
	rangeDigestFanout           = 16
	rangeDigestLeafSpan  uint64 = 1024
	maxRangeDigestNodes         = 4096
)

// rangeSpan is a bare contiguous sequence interval, used to key the local
// digest-descent worklist independent of the wire node representation.
type rangeSpan struct{ first, last uint64 }

// rangeDigestChildren deterministically splits [first,last] into at most
// rangeDigestFanout contiguous, equally sized (except the final) children.
// Both peers derive identical children for identical ranges, so descent never
// needs to negotiate a split shape over the wire.
func rangeDigestChildren(first, last uint64) []rangeSpan {
	count := last - first + 1
	if count <= 1 {
		return nil
	}
	childCount := uint64(rangeDigestFanout)
	if count < childCount {
		childCount = count
	}
	chunk := (count + childCount - 1) / childCount
	children := make([]rangeSpan, 0, childCount)
	for start := first; start <= last; start += chunk {
		end := start + chunk - 1
		if end > last {
			end = last
		}
		children = append(children, rangeSpan{first: start, last: end})
	}
	return children
}

// rangeDigestNodeFor computes one node of the direct-actor digest tree. Spans
// at or below the leaf threshold reuse DirectRangeDigest exactly, so leaf
// semantics never diverge from the existing canonical whole-batch digest.
// Larger spans recurse through deterministic fixed-fanout children and hash
// the child summary with its own domain separator, distinct from a leaf's.
func (db *DB) rangeDigestNodeFor(actor ActorID, first, last uint64) (rangeDigestNode, error) {
	if ID(actor).IsZero() || first == 0 || last < first || last-first >= maxSyncRangeSpan {
		return rangeDigestNode{}, ErrInvalidArgument
	}
	if last-first+1 <= rangeDigestLeafSpan {
		digest, err := db.DirectRangeDigest(actor, first, last)
		if err != nil {
			return rangeDigestNode{}, err
		}
		return rangeDigestNode{actor: actor, first: first, last: last, digest: digest}, nil
	}
	children := rangeDigestChildren(first, last)
	db.mu.RLock()
	database := db.manifest.Database
	db.mu.RUnlock()
	var encoded encoder
	encoded.raw([]byte("M0RN"))
	encoded.u(rangeDigestGeneration)
	encoded.id(ID(database))
	encoded.id(ID(actor))
	encoded.u(first)
	encoded.u(last)
	encoded.u(uint64(len(children)))
	for _, child := range children {
		node, err := db.rangeDigestNodeFor(actor, child.first, child.last)
		if err != nil {
			return rangeDigestNode{}, err
		}
		encoded.u(node.first)
		encoded.u(node.last)
		encoded.raw(node.digest[:])
	}
	return rangeDigestNode{actor: actor, first: first, last: last, digest: sha256.Sum256(encoded.Bytes())}, nil
}

func encodeRangeDigestNodes(nodes []rangeDigestNode) []byte {
	var encoded encoder
	encoded.u(uint64(len(nodes)))
	for _, node := range nodes {
		encoded.id(ID(node.actor))
		encoded.u(node.first)
		encoded.u(node.last)
		encoded.raw(node.digest[:])
	}
	return encoded.Bytes()
}

func decodeRangeDigestNodes(data []byte) ([]rangeDigestNode, error) {
	decoded := decoder{b: data}
	count, err := decoded.u()
	if err != nil || count > maxRangeDigestNodes {
		return nil, ErrCorruption
	}
	nodes := make([]rangeDigestNode, 0, count)
	var previous ID
	for index := uint64(0); index < count; index++ {
		id, err := decoded.id()
		if err != nil || ID(id).IsZero() || (index > 0 && idCompare(previous, id) > 0) {
			return nil, ErrCorruption
		}
		first, err := decoded.u()
		if err != nil || first == 0 {
			return nil, ErrCorruption
		}
		last, err := decoded.u()
		if err != nil || last < first || last-first >= maxSyncRangeSpan {
			return nil, ErrCorruption
		}
		raw, err := decoded.raw(32)
		if err != nil {
			return nil, ErrCorruption
		}
		var digest [32]byte
		copy(digest[:], raw)
		nodes = append(nodes, rangeDigestNode{actor: ActorID(id), first: first, last: last, digest: digest})
		previous = id
	}
	if err := decoded.done(); err != nil {
		return nil, ErrCorruption
	}
	return nodes, nil
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
