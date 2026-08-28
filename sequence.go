package mesh0

import (
	"fmt"
	"sort"
	"strings"
	"unicode/utf8"
)

// ObjectID is the immutable identity of a sequence container. It is the dot of
// the operation which created that container, never a mutable document path.
type ObjectID struct{ Dot Dot }

func (id ObjectID) Compare(other ObjectID) int { return id.Dot.Compare(other.Dot) }
func (id ObjectID) String() string             { return id.Dot.String() }
func (id ObjectID) valid() bool                { return !ID(id.Dot.Actor).IsZero() && id.Dot.Seq != 0 }

// ElementID identifies one sequence element. Offset is local to the ListInsert
// operation dot, permitting one operation to insert an ordered run of values.
type ElementID struct {
	Dot    Dot
	Offset uint32
}

func (id ElementID) Compare(other ElementID) int {
	if cmp := id.Dot.Compare(other.Dot); cmp != 0 {
		return cmp
	}
	if id.Offset < other.Offset {
		return -1
	}
	if id.Offset > other.Offset {
		return 1
	}
	return 0
}
func (id ElementID) String() string { return fmt.Sprintf("%s/%d", id.Dot, id.Offset) }
func (id ElementID) valid() bool    { return !ID(id.Dot.Actor).IsZero() && id.Dot.Seq != 0 }

// ObjectKind is retained in the canonical MakeList operation. Text shares the
// same anchored ordering CRDT as a list, while its public API uses Unicode
// code-point strings and exposes a string projection.
type ObjectKind uint8

const (
	ListObject ObjectKind = iota + 1
	TextObject
)

func (kind ObjectKind) valid() bool { return kind == ListObject || kind == TextObject }

type sequenceElement struct {
	ID    ElementID
	After ElementID
	Head  bool
	Value Value
}

type sequenceState struct {
	Kind     ObjectKind
	Elements map[ElementID]sequenceElement
	Deleted  map[ElementID]struct{}
}

func newSequence(kind ObjectKind) *sequenceState {
	return &sequenceState{Kind: kind, Elements: map[ElementID]sequenceElement{}, Deleted: map[ElementID]struct{}{}}
}

func cloneSequence(sequence *sequenceState) *sequenceState {
	clone := &sequenceState{Kind: sequence.Kind, Elements: make(map[ElementID]sequenceElement, len(sequence.Elements)), Deleted: make(map[ElementID]struct{}, len(sequence.Deleted))}
	for id, element := range sequence.Elements {
		element.Value = cloneValue(element.Value)
		clone.Elements[id] = element
	}
	for id := range sequence.Deleted {
		clone.Deleted[id] = struct{}{}
	}
	return clone
}

func observedBy(dependencies VersionVector, current Dot, target Dot) bool {
	if dependencies.Contains(target) {
		return true
	}
	// Operations in one batch are ordered and atomically visible. A later
	// operation from the same actor therefore observes an earlier operation
	// from that batch even though the batch carries one shared base frontier.
	return current.Actor == target.Actor && current.Seq > target.Seq
}

func (sequence *sequenceState) insert(operation Operation, dependencies VersionVector) error {
	if !operation.AnchorHead {
		anchor, exists := sequence.Elements[operation.Anchor]
		if !exists || !observedBy(dependencies, operation.Dot, anchor.ID.Dot) {
			return fmt.Errorf("%w: sequence anchor", ErrCorruption)
		}
	}
	if len(operation.Values) == 0 || len(operation.Values) > maxBatchOperations {
		return ErrResourceLimit
	}
	anchor, head := operation.Anchor, operation.AnchorHead
	for index, value := range operation.Values {
		id := ElementID{Dot: operation.Dot, Offset: uint32(index)}
		if _, exists := sequence.Elements[id]; exists {
			return fmt.Errorf("%w: duplicate sequence element", ErrCorruption)
		}
		sequence.Elements[id] = sequenceElement{ID: id, After: anchor, Head: head, Value: cloneValue(value)}
		anchor, head = id, false
	}
	return nil
}

func (sequence *sequenceState) remove(operation Operation, dependencies VersionVector) error {
	for _, id := range operation.Targets {
		element, exists := sequence.Elements[id]
		if !exists || !observedBy(dependencies, operation.Dot, element.ID.Dot) {
			return fmt.Errorf("%w: sequence delete target", ErrCorruption)
		}
		sequence.Deleted[id] = struct{}{}
	}
	return nil
}

// ListElement is a stable immutable view of one visible sequence element.
type ListElement struct {
	ID    ElementID
	Value Value
}

// ListView projects a list or text object from an immutable read root.
type ListView struct{ sequence *sequenceState }

func (view ListView) Kind() ObjectKind {
	if view.sequence == nil {
		return 0
	}
	return view.sequence.Kind
}

func (view ListView) Elements() []ListElement {
	if view.sequence == nil {
		return nil
	}
	children := make(map[ElementID][]sequenceElement)
	var roots []sequenceElement
	for _, element := range view.sequence.Elements {
		if element.Head {
			roots = append(roots, element)
		} else {
			children[element.After] = append(children[element.After], element)
		}
	}
	sortElements := func(elements []sequenceElement) {
		sort.Slice(elements, func(i, j int) bool { return elements[i].ID.Compare(elements[j].ID) < 0 })
	}
	sortElements(roots)
	for id := range children {
		sortElements(children[id])
	}
	out := make([]ListElement, 0, len(view.sequence.Elements)-len(view.sequence.Deleted))
	var visit func(sequenceElement)
	visit = func(element sequenceElement) {
		if _, deleted := view.sequence.Deleted[element.ID]; !deleted {
			out = append(out, ListElement{ID: element.ID, Value: cloneValue(element.Value)})
		}
		for _, child := range children[element.ID] {
			visit(child)
		}
	}
	for _, root := range roots {
		visit(root)
	}
	return out
}

// Text returns the UTF-8 string represented by a text sequence. It returns
// false for a list or for impossible historical text content.
func (view ListView) Text() (string, bool) {
	if view.sequence == nil || view.sequence.Kind != TextObject {
		return "", false
	}
	var builder strings.Builder
	for _, element := range view.Elements() {
		if element.Value.Kind != StringValue || utf8.RuneCountInString(element.Value.Text) != 1 {
			return "", false
		}
		builder.WriteString(element.Value.Text)
	}
	return builder.String(), true
}
