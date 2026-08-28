package mesh0

import (
	"crypto/sha256"
	"fmt"
	"math"
	"sort"
	"unicode/utf8"
)

// A register retains all causally concurrent assignments. Removed records the
// operation dots causally observed by a later assignment/delete.
type register struct {
	Removed VersionVector
	Values  map[Dot]Value
}

// An observed-remove set keeps only the individual add dots removed for a
// particular element. This avoids a remove for one member deleting other
// members that happened to be observed in the same transaction.
type setState struct {
	Removed map[Dot]struct{}
	Adds    map[Dot]Value
}

type documentState struct {
	Deleted  VersionVector
	Fields   map[string]*register
	Sets     map[string]*setState
	Counters map[string]map[Dot]int64
	Lists    map[ObjectID]*sequenceState
}

type state struct {
	Frontier  VersionVector
	Documents map[DocumentKey]*documentState
	Batches   map[Dot]Batch
	Hashes    map[Dot][32]byte
}

func newState() *state {
	return &state{
		Frontier: VersionVector{}, Documents: map[DocumentKey]*documentState{},
		Batches: map[Dot]Batch{}, Hashes: map[Dot][32]byte{},
	}
}

func cloneValue(v Value) Value                  { v.Bytes = append([]byte(nil), v.Bytes...); return v }
func cloneVector(v VersionVector) VersionVector { return v.Clone() }
func cloneRegister(r *register) *register {
	n := &register{Removed: cloneVector(r.Removed), Values: make(map[Dot]Value, len(r.Values))}
	for dot, value := range r.Values {
		n.Values[dot] = cloneValue(value)
	}
	return n
}
func cloneSet(s *setState) *setState {
	n := &setState{Removed: make(map[Dot]struct{}, len(s.Removed)), Adds: make(map[Dot]Value, len(s.Adds))}
	for dot := range s.Removed {
		n.Removed[dot] = struct{}{}
	}
	for dot, value := range s.Adds {
		n.Adds[dot] = cloneValue(value)
	}
	return n
}
func cloneDoc(document *documentState) *documentState {
	clone := &documentState{Deleted: cloneVector(document.Deleted), Fields: make(map[string]*register, len(document.Fields)), Sets: make(map[string]*setState, len(document.Sets)), Counters: make(map[string]map[Dot]int64, len(document.Counters)), Lists: make(map[ObjectID]*sequenceState, len(document.Lists))}
	for key, value := range document.Fields {
		clone.Fields[key] = cloneRegister(value)
	}
	for key, value := range document.Sets {
		clone.Sets[key] = cloneSet(value)
	}
	for key, value := range document.Counters {
		clone.Counters[key] = make(map[Dot]int64, len(value))
		for dot, delta := range value {
			clone.Counters[key][dot] = delta
		}
	}
	for id, sequence := range document.Lists {
		clone.Lists[id] = cloneSequence(sequence)
	}
	return clone
}
func (s *state) clone() *state {
	n := &state{Frontier: s.Frontier.Clone(), Documents: make(map[DocumentKey]*documentState, len(s.Documents)), Batches: make(map[Dot]Batch, len(s.Batches)), Hashes: make(map[Dot][32]byte, len(s.Hashes))}
	for key, document := range s.Documents {
		n.Documents[key] = cloneDoc(document)
	}
	for dot, batch := range s.Batches {
		n.Batches[dot] = batch
	}
	for dot, hash := range s.Hashes {
		n.Hashes[dot] = hash
	}
	return n
}

func (s *state) hasDependency(b Batch) bool {
	return s.Frontier.Covers(b.Dependencies) && s.Frontier[b.First.Actor]+1 == b.First.Seq
}

// apply returns an immutable next root. A known batch is a no-op, whereas a
// different batch under the same actor/sequence is an actor-fork integrity error.
func (s *state) apply(b Batch) (*state, error) {
	if err := b.Validate(); err != nil {
		return nil, err
	}
	hash, err := b.Hash()
	if err != nil {
		return nil, err
	}
	if old, exists := s.Hashes[b.First]; exists {
		if old != hash {
			return nil, ErrActorFork
		}
		return s, nil
	}
	if !s.hasDependency(b) {
		return nil, ErrCausalGap
	}
	next := s.clone()
	for _, operation := range b.Operations {
		if err := next.applyOp(operation, b.Dependencies); err != nil {
			return nil, err
		}
	}
	next.Frontier[b.First.Actor] = b.First.Seq + uint64(b.Count) - 1
	next.Batches[b.First] = b
	next.Hashes[b.First] = hash
	return next, nil
}

func (s *state) doc(key DocumentKey) *documentState {
	if document := s.Documents[key]; document != nil {
		return document
	}
	document := &documentState{Deleted: VersionVector{}, Fields: map[string]*register{}, Sets: map[string]*setState{}, Counters: map[string]map[Dot]int64{}, Lists: map[ObjectID]*sequenceState{}}
	s.Documents[key] = document
	return document
}

func pathKey(path []string) string {
	key := ""
	for _, part := range path {
		key += fmt.Sprintf("%d:%s", len(part), part)
	}
	return key
}

func removeObservedRegister(r *register, deleted VersionVector) {
	for dot := range r.Values {
		if r.Removed.Contains(dot) || deleted.Contains(dot) {
			delete(r.Values, dot)
		}
	}
}

func (document *documentState) referencedSequence(value Value) (*sequenceState, bool) {
	if value.Kind != ListRefValue && value.Kind != TextRefValue {
		return nil, true
	}
	sequence := document.Lists[value.Object]
	if sequence == nil || (value.Kind == ListRefValue && sequence.Kind != ListObject) || (value.Kind == TextRefValue && sequence.Kind != TextObject) {
		return nil, false
	}
	return sequence, true
}

func (s *state) applyOp(op Operation, dependencies VersionVector) error {
	document := s.doc(op.Document)
	if op.Action == DocumentDelete {
		document.Deleted.Join(dependencies)
		for _, register := range document.Fields {
			register.Removed.Join(dependencies)
			removeObservedRegister(register, document.Deleted)
		}
		for _, set := range document.Sets {
			for dot := range set.Adds {
				if dependencies.Contains(dot) || document.Deleted.Contains(dot) {
					set.Removed[dot] = struct{}{}
					delete(set.Adds, dot)
				}
			}
		}
		return nil
	}

	switch op.Action {
	case MakeList:
		if _, exists := document.Lists[op.Object]; exists {
			return fmt.Errorf("%w: duplicate list object", ErrCorruption)
		}
		document.Lists[op.Object] = newSequence(op.ObjectKind)
		return nil
	case ListInsert:
		sequence := document.Lists[op.Object]
		if sequence == nil {
			return fmt.Errorf("%w: unknown list object", ErrCorruption)
		}
		if sequence.Kind == TextObject {
			for _, value := range op.Values {
				if value.Kind != StringValue || !utf8.ValidString(value.Text) || len(value.Text) == 0 || len([]rune(value.Text)) != 1 {
					return fmt.Errorf("%w: text element", ErrCorruption)
				}
			}
		}
		return sequence.insert(op, dependencies)
	case ListDelete:
		sequence := document.Lists[op.Object]
		if sequence == nil {
			return fmt.Errorf("%w: unknown list object", ErrCorruption)
		}
		return sequence.remove(op, dependencies)
	}

	key := pathKey(op.Path)
	switch op.Action {
	case MapAssign:
		if _, valid := document.referencedSequence(op.Value); !valid {
			return fmt.Errorf("%w: unknown object reference", ErrCorruption)
		}
		entry := document.Fields[key]
		if entry == nil {
			entry = &register{Removed: VersionVector{}, Values: map[Dot]Value{}}
			document.Fields[key] = entry
		}
		entry.Removed.Join(dependencies)
		removeObservedRegister(entry, document.Deleted)
		if !entry.Removed.Contains(op.Dot) && !document.Deleted.Contains(op.Dot) {
			entry.Values[op.Dot] = cloneValue(op.Value)
		}
	case MapDelete:
		entry := document.Fields[key]
		if entry == nil {
			entry = &register{Removed: VersionVector{}, Values: map[Dot]Value{}}
			document.Fields[key] = entry
		}
		entry.Removed.Join(dependencies)
		removeObservedRegister(entry, document.Deleted)
	case SetAdd:
		set := document.Sets[key]
		if set == nil {
			set = &setState{Removed: map[Dot]struct{}{}, Adds: map[Dot]Value{}}
			document.Sets[key] = set
		}
		if _, removed := set.Removed[op.Dot]; !removed && !document.Deleted.Contains(op.Dot) {
			set.Adds[op.Dot] = cloneValue(op.Value)
		}
	case SetRemove:
		set := document.Sets[key]
		if set == nil {
			set = &setState{Removed: map[Dot]struct{}{}, Adds: map[Dot]Value{}}
			document.Sets[key] = set
		}
		for dot, value := range set.Adds {
			if dependencies.Contains(dot) && value.Equal(op.Value) {
				set.Removed[dot] = struct{}{}
				delete(set.Adds, dot)
			}
		}
	case CounterAdd:
		counter := document.Counters[key]
		if counter == nil {
			counter = map[Dot]int64{}
			document.Counters[key] = counter
		}
		var total int64
		for _, delta := range counter {
			if (delta > 0 && total > math.MaxInt64-delta) || (delta < 0 && total < math.MinInt64-delta) {
				return ErrCorruption
			}
			total += delta
		}
		if (op.Delta > 0 && total > math.MaxInt64-op.Delta) || (op.Delta < 0 && total < math.MinInt64-op.Delta) {
			return ErrCorruption
		}
		if !document.Deleted.Contains(op.Dot) {
			counter[op.Dot] = op.Delta
		}
	default:
		return ErrInvalidArgument
	}
	return nil
}

// DocumentView exposes a conflict-aware immutable document projection.
type DocumentView struct {
	key DocumentKey
	doc *documentState
}

func (d DocumentView) Key() DocumentKey { return d.key }
func (d DocumentView) Values(path ...string) []Value {
	if d.doc == nil {
		return nil
	}
	register := d.doc.Fields[pathKey(path)]
	if register == nil {
		return nil
	}
	dots := make([]Dot, 0, len(register.Values))
	for dot := range register.Values {
		dots = append(dots, dot)
	}
	sort.Slice(dots, func(i, j int) bool { return dots[i].Compare(dots[j]) < 0 })
	values := make([]Value, len(dots))
	for i, dot := range dots {
		values[i] = cloneValue(register.Values[dot])
	}
	return values
}
func (d DocumentView) Value(path ...string) (Value, bool) {
	values := d.Values(path...)
	if len(values) == 0 {
		return Value{}, false
	}
	return values[len(values)-1], true
}
func (d DocumentView) HasConflict(path ...string) bool { return len(d.Values(path...)) > 1 }
func (d DocumentView) Set(path ...string) []Value {
	if d.doc == nil {
		return nil
	}
	set := d.doc.Sets[pathKey(path)]
	if set == nil {
		return nil
	}
	values := make([]Value, 0, len(set.Adds))
	for _, value := range set.Adds {
		values = append(values, cloneValue(value))
	}
	sort.Slice(values, func(i, j int) bool { return values[i].Key() < values[j].Key() })
	return values
}
func (d DocumentView) List(path ...string) (ListView, bool) {
	if d.doc == nil {
		return ListView{}, false
	}
	value, ok := d.Value(path...)
	if !ok || (value.Kind != ListRefValue && value.Kind != TextRefValue) {
		return ListView{}, false
	}
	sequence, ok := d.doc.Lists[value.Object]
	if !ok || (value.Kind == ListRefValue && sequence.Kind != ListObject) || (value.Kind == TextRefValue && sequence.Kind != TextObject) {
		return ListView{}, false
	}
	return ListView{sequence: sequence}, true
}
func (d DocumentView) Counter(path ...string) int64 {
	if d.doc == nil {
		return 0
	}
	var total int64
	for _, delta := range d.doc.Counters[pathKey(path)] {
		total += delta
	}
	return total
}
func (d DocumentView) Fields() []string {
	if d.doc == nil {
		return nil
	}
	fields := make([]string, 0, len(d.doc.Fields))
	for key := range d.doc.Fields {
		fields = append(fields, key)
	}
	sort.Strings(fields)
	return fields
}

// digest deliberately hashes canonical operation history rather than physical
// files. Full-history retention in generation one makes this both complete and
// insensitive to segment rotation or snapshot timing.
func (s *state) digest(database DatabaseID) [32]byte {
	batches := make([]Batch, 0, len(s.Batches))
	for _, batch := range s.Batches {
		batches = append(batches, batch)
	}
	BatchSort(batches)
	var encoded encoder
	encoded.raw([]byte("M0DG"))
	encoded.id(ID(database))
	encoded.clock(s.Frontier)
	encoded.u(uint64(len(batches)))
	for _, batch := range batches {
		raw, _ := batch.MarshalBinary()
		encoded.bytes(raw)
	}
	return sha256.Sum256(encoded.Bytes())
}
