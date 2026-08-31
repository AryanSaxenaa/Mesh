package mesh0

import (
	"context"
	"fmt"
)

// EqualityIndex is a process-local derived index over live map-register values.
// It is never replicated or persisted; it can always be rebuilt from canonical
// state. Concurrent register values place a document in every matching bucket.
type EqualityIndex struct {
	Collection string
	Path       string
}

type equalityIndexSnapshot struct {
	declared map[EqualityIndex]struct{}
	buckets  map[EqualityIndex]map[string]map[DocumentKey]struct{}
}

func newEqualityIndexSnapshot() *equalityIndexSnapshot {
	return &equalityIndexSnapshot{declared: map[EqualityIndex]struct{}{}, buckets: map[EqualityIndex]map[string]map[DocumentKey]struct{}{}}
}

func validateEqualityIndex(spec EqualityIndex) error {
	if spec.Collection == "" || spec.Path == "" || len(spec.Collection) > 1024 || len(spec.Path) > maxStringBytes {
		return fmt.Errorf("%w: equality index", ErrInvalidArgument)
	}
	return nil
}

func equalityValueKey(value Value) (string, error) {
	if value.Kind == ListRefValue || value.Kind == TextRefValue {
		return "", fmt.Errorf("%w: container equality index", ErrInvalidArgument)
	}
	if err := value.Validate(); err != nil {
		return "", err
	}
	var encoded encoder
	if err := value.encode(&encoded); err != nil {
		return "", err
	}
	return string(encoded.Bytes()), nil
}

func cloneIndexDeclarations(indexes *equalityIndexSnapshot) map[EqualityIndex]struct{} {
	declared := make(map[EqualityIndex]struct{}, len(indexes.declared))
	for spec := range indexes.declared {
		declared[spec] = struct{}{}
	}
	return declared
}

func rebuildEqualityIndexes(ctx context.Context, root *state, declared map[EqualityIndex]struct{}) (*equalityIndexSnapshot, error) {
	indexes := &equalityIndexSnapshot{declared: cloneIndexDeclarations(&equalityIndexSnapshot{declared: declared}), buckets: make(map[EqualityIndex]map[string]map[DocumentKey]struct{}, len(declared))}
	for spec := range indexes.declared {
		if err := validateEqualityIndex(spec); err != nil {
			return nil, err
		}
		buckets := map[string]map[DocumentKey]struct{}{}
		for key, document := range root.Documents {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			if key.Collection != spec.Collection {
				continue
			}
			for _, value := range (DocumentView{key: key, doc: document}).Values(spec.Path) {
				valueKey, err := equalityValueKey(value)
				if err != nil {
					return nil, err
				}
				bucket := buckets[valueKey]
				if bucket == nil {
					bucket = map[DocumentKey]struct{}{}
					buckets[valueKey] = bucket
				}
				bucket[key] = struct{}{}
			}
		}
		indexes.buckets[spec] = buckets
	}
	return indexes, nil
}

// EnsureEqualityIndex declares and atomically builds a local equality index.
// The index observes the same conflict-aware Values(path) semantics as Query.
func (db *DB) EnsureEqualityIndex(ctx context.Context, spec EqualityIndex) error {
	if err := validateEqualityIndex(spec); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return ErrClosed
	}
	if db.failed != nil {
		return db.failed
	}
	declared := cloneIndexDeclarations(db.indexes)
	declared[spec] = struct{}{}
	indexes, err := rebuildEqualityIndexes(ctx, db.state, declared)
	if err != nil {
		return err
	}
	db.indexes = indexes
	return nil
}

// DropEqualityIndex removes a local derived index. Canonical data is unchanged.
func (db *DB) DropEqualityIndex(spec EqualityIndex) error {
	if err := validateEqualityIndex(spec); err != nil {
		return err
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return ErrClosed
	}
	if db.failed != nil {
		return db.failed
	}
	declared := cloneIndexDeclarations(db.indexes)
	delete(declared, spec)
	indexes, err := rebuildEqualityIndexes(context.Background(), db.state, declared)
	if err != nil {
		return err
	}
	db.indexes = indexes
	return nil
}

// RebuildEqualityIndexes replaces every local derived index from canonical data.
func (db *DB) RebuildEqualityIndexes(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return ErrClosed
	}
	if db.failed != nil {
		return db.failed
	}
	indexes, err := rebuildEqualityIndexes(ctx, db.state, cloneIndexDeclarations(db.indexes))
	if err != nil {
		return err
	}
	db.indexes = indexes
	return nil
}
