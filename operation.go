package mesh0

import (
	"crypto/sha256"
	"fmt"
	"sort"
)

type Action uint8

const (
	MapAssign Action = iota + 1
	MapDelete
	SetAdd
	SetRemove
	CounterAdd
	DocumentDelete
	MakeList
	ListInsert
	ListDelete
)

type DocumentKey struct{ Collection, ID string }

func (key DocumentKey) Validate() error {
	if key.Collection == "" || key.ID == "" || len(key.Collection) > 1024 || len(key.ID) > maxStringBytes {
		return fmt.Errorf("%w: document key", ErrInvalidArgument)
	}
	return nil
}
func (key DocumentKey) String() string { return key.Collection + "/" + key.ID }

// Operation is immutable canonical CRDT history. Object/anchor payloads are
// used only by the anchored list/text actions; existing scalar actions retain
// their generation-one byte encoding.
type Operation struct {
	Dot        Dot
	Document   DocumentKey
	Path       []string
	Action     Action
	Value      Value
	Delta      int64
	Object     ObjectID
	ObjectKind ObjectKind
	Anchor     ElementID
	AnchorHead bool
	Values     []Value
	Targets    []ElementID
}
type Batch struct {
	First          Dot
	Count          uint32
	Dependencies   VersionVector
	Operations     []Operation
	TimestampNanos int64 // Informational only; never affects causality or ordering.
}

func (batch Batch) ID() string { return fmt.Sprintf("%s+%d", batch.First, batch.Count) }
func (batch Batch) Validate() error {
	if batch.Count == 0 || batch.Count > maxBatchOperations || len(batch.Operations) != int(batch.Count) {
		return fmt.Errorf("%w: transaction size", ErrInvalidArgument)
	}
	for index, operation := range batch.Operations {
		if operation.Dot.Actor != batch.First.Actor || operation.Dot.Seq != batch.First.Seq+uint64(index) {
			return fmt.Errorf("%w: operation dots", ErrInvalidArgument)
		}
		if err := operation.Document.Validate(); err != nil {
			return err
		}
		if len(operation.Path) > maxPathParts {
			return ErrResourceLimit
		}
		for _, part := range operation.Path {
			if part == "" || len(part) > maxStringBytes {
				return fmt.Errorf("%w: path", ErrInvalidArgument)
			}
		}
		switch operation.Action {
		case MapAssign, SetAdd, SetRemove:
			if err := operation.Value.Validate(); err != nil {
				return err
			}
		case MapDelete, CounterAdd:
		case DocumentDelete:
			if len(operation.Path) != 0 {
				return fmt.Errorf("%w: document delete path", ErrInvalidArgument)
			}
		case MakeList:
			if len(operation.Path) != 0 || !operation.Object.valid() || operation.Object != (ObjectID{Dot: operation.Dot}) || !operation.ObjectKind.valid() {
				return fmt.Errorf("%w: list creation", ErrInvalidArgument)
			}
		case ListInsert:
			if len(operation.Path) != 0 || !operation.Object.valid() || len(operation.Values) == 0 || len(operation.Values) > maxBatchOperations {
				return fmt.Errorf("%w: list insertion", ErrInvalidArgument)
			}
			if !operation.AnchorHead && !operation.Anchor.valid() {
				return fmt.Errorf("%w: list anchor", ErrInvalidArgument)
			}
			for _, value := range operation.Values {
				if err := value.Validate(); err != nil {
					return err
				}
			}
		case ListDelete:
			if len(operation.Path) != 0 || !operation.Object.valid() || len(operation.Targets) == 0 || len(operation.Targets) > maxBatchOperations {
				return fmt.Errorf("%w: list deletion", ErrInvalidArgument)
			}
			for targetIndex, target := range operation.Targets {
				if !target.valid() || (targetIndex > 0 && operation.Targets[targetIndex-1].Compare(target) >= 0) {
					return fmt.Errorf("%w: list delete targets", ErrInvalidArgument)
				}
			}
		default:
			return fmt.Errorf("%w: action", ErrInvalidArgument)
		}
	}
	return nil
}
func (batch Batch) MarshalBinary() ([]byte, error) {
	if err := batch.Validate(); err != nil {
		return nil, err
	}
	var encoded encoder
	encoded.raw([]byte("M0BT"))
	encoded.u(uint64(formatGeneration))
	encoded.dot(batch.First)
	encoded.u(uint64(batch.Count))
	encoded.clock(batch.Dependencies)
	encoded.i(batch.TimestampNanos)
	encoded.u(uint64(len(batch.Operations)))
	for _, operation := range batch.Operations {
		encoded.dot(operation.Dot)
		encoded.str(operation.Document.Collection)
		encoded.str(operation.Document.ID)
		encoded.u(uint64(len(operation.Path)))
		for _, part := range operation.Path {
			encoded.str(part)
		}
		encoded.u(uint64(operation.Action))
		switch operation.Action {
		case MapAssign, SetAdd, SetRemove:
			if err := operation.Value.encode(&encoded); err != nil {
				return nil, err
			}
		case CounterAdd:
			encoded.i(operation.Delta)
		case MapDelete, DocumentDelete:
		case MakeList:
			encoded.dot(operation.Object.Dot)
			encoded.u(uint64(operation.ObjectKind))
		case ListInsert:
			encoded.dot(operation.Object.Dot)
			if operation.AnchorHead {
				encoded.u(1)
			} else {
				encoded.u(0)
				encoded.element(operation.Anchor)
			}
			encoded.u(uint64(len(operation.Values)))
			for _, value := range operation.Values {
				if err := value.encode(&encoded); err != nil {
					return nil, err
				}
			}
		case ListDelete:
			encoded.dot(operation.Object.Dot)
			encoded.u(uint64(len(operation.Targets)))
			for _, target := range operation.Targets {
				encoded.element(target)
			}
		}
	}
	if encoded.Len() > maxBatchBytes {
		return nil, ErrResourceLimit
	}
	return encoded.Bytes(), nil
}
func UnmarshalBatch(raw []byte) (Batch, error) {
	if len(raw) > maxBatchBytes {
		return Batch{}, ErrResourceLimit
	}
	decoded := decoder{b: raw}
	magic, err := decoded.raw(4)
	if err != nil || string(magic) != "M0BT" {
		return Batch{}, ErrCorruption
	}
	generation, err := decoded.u()
	if err != nil || generation != formatGeneration {
		return Batch{}, fmt.Errorf("%w: batch generation", ErrCorruption)
	}
	first, err := decoded.dot()
	if err != nil {
		return Batch{}, err
	}
	count, err := decoded.u()
	if err != nil || count == 0 || count > maxBatchOperations {
		return Batch{}, ErrCorruption
	}
	dependencies, err := decoded.clock()
	if err != nil {
		return Batch{}, err
	}
	timestamp, err := decoded.i()
	if err != nil {
		return Batch{}, err
	}
	countOnWire, err := decoded.u()
	if err != nil || countOnWire != count {
		return Batch{}, ErrCorruption
	}
	batch := Batch{First: first, Count: uint32(count), Dependencies: dependencies, TimestampNanos: timestamp, Operations: make([]Operation, 0, count)}
	for operationIndex := uint64(0); operationIndex < count; operationIndex++ {
		dot, decodeErr := decoded.dot()
		if decodeErr != nil {
			return Batch{}, decodeErr
		}
		collection, decodeErr := decoded.str(1024)
		if decodeErr != nil {
			return Batch{}, decodeErr
		}
		id, decodeErr := decoded.str(maxStringBytes)
		if decodeErr != nil {
			return Batch{}, decodeErr
		}
		pathCount, decodeErr := decoded.u()
		if decodeErr != nil || pathCount > maxPathParts {
			return Batch{}, ErrCorruption
		}
		path := make([]string, pathCount)
		for pathIndex := range path {
			path[pathIndex], decodeErr = decoded.str(maxStringBytes)
			if decodeErr != nil {
				return Batch{}, decodeErr
			}
		}
		action, decodeErr := decoded.u()
		if decodeErr != nil {
			return Batch{}, decodeErr
		}
		operation := Operation{Dot: dot, Document: DocumentKey{Collection: collection, ID: id}, Path: path, Action: Action(action)}
		switch operation.Action {
		case MapAssign, SetAdd, SetRemove:
			operation.Value, decodeErr = decodeValue(&decoded)
		case CounterAdd:
			operation.Delta, decodeErr = decoded.i()
		case MapDelete, DocumentDelete:
		case MakeList:
			operation.Object.Dot, decodeErr = decoded.dot()
			if decodeErr == nil {
				kind, kindErr := decoded.u()
				operation.ObjectKind, decodeErr = ObjectKind(kind), kindErr
			}
		case ListInsert:
			operation.Object.Dot, decodeErr = decoded.dot()
			if decodeErr == nil {
				anchorHead, anchorErr := decoded.u()
				if anchorErr != nil || anchorHead > 1 {
					return Batch{}, ErrCorruption
				}
				operation.AnchorHead = anchorHead == 1
				if !operation.AnchorHead {
					operation.Anchor, decodeErr = decoded.element()
				}
			}
			if decodeErr == nil {
				valueCount, countErr := decoded.u()
				if countErr != nil || valueCount == 0 || valueCount > maxBatchOperations {
					return Batch{}, ErrCorruption
				}
				operation.Values = make([]Value, 0, valueCount)
				for valueIndex := uint64(0); valueIndex < valueCount; valueIndex++ {
					value, valueErr := decodeValue(&decoded)
					if valueErr != nil {
						return Batch{}, valueErr
					}
					operation.Values = append(operation.Values, value)
				}
			}
		case ListDelete:
			operation.Object.Dot, decodeErr = decoded.dot()
			if decodeErr == nil {
				targetCount, countErr := decoded.u()
				if countErr != nil || targetCount == 0 || targetCount > maxBatchOperations {
					return Batch{}, ErrCorruption
				}
				operation.Targets = make([]ElementID, targetCount)
				for targetIndex := range operation.Targets {
					operation.Targets[targetIndex], decodeErr = decoded.element()
					if decodeErr != nil {
						return Batch{}, decodeErr
					}
				}
			}
		default:
			return Batch{}, ErrCorruption
		}
		if decodeErr != nil {
			return Batch{}, decodeErr
		}
		batch.Operations = append(batch.Operations, operation)
	}
	if err = decoded.done(); err != nil {
		return Batch{}, err
	}
	return batch, batch.Validate()
}
func (batch Batch) Hash() ([32]byte, error) {
	raw, err := batch.MarshalBinary()
	return sha256.Sum256(raw), err
}

// BatchSort makes equivalent operation histories have a stable serialized order.
func BatchSort(batches []Batch) {
	sort.Slice(batches, func(left, right int) bool { return batches[left].First.Compare(batches[right].First) < 0 })
}
