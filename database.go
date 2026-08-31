package mesh0

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
	"unicode/utf8"
)

// DurabilityMode defines when a successful Update is acknowledged.
type DurabilityMode uint8

const (
	DurabilitySync   DurabilityMode = iota // fsync before Update returns.
	DurabilityMemory                       // writes reach the OS but may be lost on a power failure.
)

type Options struct {
	Durability      DurabilityMode
	Logger          *slog.Logger
	MaxSegmentBytes int64
}

const defaultMaxSegmentBytes int64 = 64 << 20

type DB struct {
	path             string
	mu               sync.RWMutex
	state            *state
	manifest         manifest
	durability       DurabilityMode
	maxSegmentBytes  int64
	logger           *slog.Logger
	unlock           func() error
	closed           bool
	failed           error
	actorKeys        map[ActorID]ed25519.PublicKey
	actorWriteGrants map[ActorID]map[string]struct{}
	indexes          *equalityIndexSnapshot
	identityMu       sync.Mutex
	subs             map[uint64]chan Change
	nextSub          uint64
}

type Change struct {
	Batch      Batch
	Remote     bool
	Frontier   VersionVector
	OccurredAt time.Time
}

type Status struct {
	DatabaseID        DatabaseID
	ActorID           ActorID
	Durability        DurabilityMode
	Documents         int
	Operations        int
	KnownActors       int
	Frontier          VersionVector
	ActiveSegment     uint64
	Snapshot          string
	LogicalDigest     [32]byte
	SubscriptionCount int
}

// Open opens or creates a local Mesh0 database. It takes an exclusive process
// lock, validates the physical metadata, then causally replays retained history.
func Open(path string, options Options) (*DB, error) {
	if path == "" {
		return nil, fmt.Errorf("%w: empty database path", ErrInvalidArgument)
	}
	if options.Logger == nil {
		options.Logger = slog.Default()
	}
	if options.MaxSegmentBytes <= 0 {
		options.MaxSegmentBytes = defaultMaxSegmentBytes
	}
	if options.MaxSegmentBytes < 1024 {
		return nil, fmt.Errorf("%w: MaxSegmentBytes must be at least 1024", ErrInvalidArgument)
	}
	if err := os.MkdirAll(path, 0700); err != nil {
		return nil, err
	}
	unlock, err := platformAcquireLock(filepath.Join(path, lockName))
	if err != nil {
		return nil, err
	}
	fail := func(err error) (*DB, error) { _ = unlock(); return nil, err }

	m, err := readManifest(path)
	if errors.Is(err, os.ErrNotExist) {
		m, err = initStorage(path)
	}
	if err != nil {
		return fail(err)
	}
	if err := recoverActorRotation(path); err != nil {
		return fail(fmt.Errorf("%w: actor rotation: %v", ErrCorruption, err))
	}
	m, err = readManifest(path)
	if err != nil {
		return fail(err)
	}
	identityDB, identityActor, nextSequence, err := parseIdentityFile(path)
	if err != nil {
		return fail(err)
	}
	if identityDB != m.Database || identityActor != m.Actor {
		return fail(fmt.Errorf("%w: manifest and identity differ", ErrCorruption))
	}

	all := make([]Batch, 0)
	snapshotBatches, err := readSnapshot(path, m.Snapshot)
	if err != nil {
		return fail(fmt.Errorf("%w: snapshot: %v", ErrCorruption, err))
	}
	all = append(all, snapshotBatches...)
	segments, err := replaySegments(path, m, &all, true)
	if err != nil {
		return fail(err)
	}
	_ = segments
	root, err := replayCausally(all)
	if err != nil {
		return fail(err)
	}
	if root.Frontier[m.Actor]+1 > nextSequence {
		nextSequence = root.Frontier[m.Actor] + 1
	}
	if nextSequence > m.NextSeq {
		m.NextSeq = nextSequence
		if err := persistIdentity(path, m); err != nil {
			return fail(err)
		}
		if err := writeManifest(path, m); err != nil {
			return fail(err)
		}
	}

	bindings, err := readActorBindings(path)
	if err != nil {
		return fail(fmt.Errorf("%w: actor bindings: %v", ErrCorruption, err))
	}
	grants, err := readActorWriteGrants(path, bindings, m.Actor)
	if err != nil {
		return fail(fmt.Errorf("%w: actor write grants: %v", ErrCorruption, err))
	}

	db := &DB{
		path: path, state: root, manifest: m, durability: options.Durability,
		maxSegmentBytes: options.MaxSegmentBytes, logger: options.Logger,
		unlock: unlock, actorKeys: bindings, actorWriteGrants: grants, indexes: newEqualityIndexSnapshot(), subs: map[uint64]chan Change{},
	}
	db.logger.Info("database.open", "path", path, "actor", ID(m.Actor).String(), "documents", len(root.Documents))
	return db, nil
}

func parseIdentityFile(path string) (DatabaseID, ActorID, uint64, error) {
	bytes, err := os.ReadFile(filepath.Join(path, identityName))
	if err != nil {
		return DatabaseID{}, ActorID{}, 0, err
	}
	return parseIdentity(bytes)
}
func persistIdentity(path string, m manifest) error {
	return atomicWrite(filepath.Join(path, identityName), identityBytes(m), 0600)
}

// replaySegments applies strict corruption rules: an invalid immutable segment
// fails the open, while a malformed final active frame is truncated to the last
// checksummed transaction boundary.
func replaySegments(path string, m manifest, out *[]Batch, repairActiveTail bool) (int, error) {
	files, err := segmentFiles(path)
	if err != nil {
		return 0, err
	}
	if len(files) == 0 {
		return 0, fmt.Errorf("%w: no active segment", ErrCorruption)
	}
	var previous uint64
	for index, file := range files {
		var number uint64
		if _, err := fmt.Sscanf(filepath.Base(file), "%d.seg", &number); err != nil {
			return 0, fmt.Errorf("%w: segment filename", ErrCorruption)
		}
		if number == 0 || number > m.Active || (index == 0 && m.Snapshot == "" && number != 1) || (index > 0 && number != previous+1) {
			return 0, fmt.Errorf("%w: segment topology", ErrCorruption)
		}
		previous = number
		last, err := scanSegment(file, m.Database, number, number == m.Active, func(batch Batch) error {
			*out = append(*out, batch)
			return nil
		})
		if err != nil {
			return 0, fmt.Errorf("%w: %s: %v", ErrCorruption, file, err)
		}
		if number == m.Active {
			info, statErr := os.Stat(file)
			if statErr != nil {
				return 0, statErr
			}
			if last < info.Size() {
				if !repairActiveTail {
					return 0, fmt.Errorf("%w: malformed active tail", ErrCorruption)
				}
				if err := os.Truncate(file, last); err != nil {
					return 0, fmt.Errorf("%w: truncate active tail: %v", ErrDurability, err)
				}
			}
		}
	}
	if previous != m.Active {
		return 0, fmt.Errorf("%w: active segment missing", ErrCorruption)
	}
	return len(files), nil
}

func replayCausally(batches []Batch) (*state, error) {
	root := newState()
	remaining := append([]Batch(nil), batches...)
	for len(remaining) > 0 {
		progress := false
		next := remaining[:0]
		for _, batch := range remaining {
			candidate, err := root.apply(batch)
			if errors.Is(err, ErrCausalGap) {
				next = append(next, batch)
				continue
			}
			if err != nil {
				return nil, err
			}
			root = candidate
			progress = true
		}
		if !progress {
			return nil, fmt.Errorf("%w: retained history has a missing dependency", ErrCorruption)
		}
		remaining = next
	}
	return root, nil
}

func (db *DB) openErr() error {
	if db == nil {
		return ErrClosed
	}
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return ErrClosed
	}
	if db.failed != nil {
		return db.failed
	}
	return nil
}

// failWriteLocked fences this process after an append may have crossed the
// storage boundary. Reopening replays the authoritative WAL and re-establishes
// the actor sequence; continuing in-process could otherwise reuse a dot.
func (db *DB) failWriteLocked(cause error) error {
	if db.failed == nil {
		db.failed = fmt.Errorf("%w: close and reopen database before retrying: %v", ErrRecoveryRequired, cause)
		db.logger.Error("database.recovery_required", "error", cause)
	}
	return db.failed
}
func (db *DB) Path() string { return db.path }
func (db *DB) DatabaseID() DatabaseID {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.manifest.Database
}
func (db *DB) ActorID() ActorID { db.mu.RLock(); defer db.mu.RUnlock(); return db.manifest.Actor }

func (db *DB) Close() error {
	if db == nil {
		return nil
	}
	db.mu.Lock()
	if db.closed {
		db.mu.Unlock()
		return nil
	}
	db.closed = true
	for _, ch := range db.subs {
		close(ch)
	}
	db.subs = nil
	unlock := db.unlock
	db.mu.Unlock()
	if unlock != nil {
		return unlock()
	}
	return nil
}

// Update builds one causally coherent, durable batch and publishes it only after
// the selected durability boundary has succeeded.
func (db *DB) Update(ctx context.Context, fn func(*Tx) error) error {
	if fn == nil {
		return fmt.Errorf("%w: nil update function", ErrInvalidArgument)
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
	tx := &Tx{base: db.state, dependencies: db.state.Frontier.Clone(), actor: db.manifest.Actor, sequence: db.manifest.NextSeq}
	if err := fn(tx); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(tx.operations) == 0 {
		return nil
	}
	if len(tx.operations) > maxBatchOperations {
		return ErrResourceLimit
	}
	batch := tx.batch(db.manifest.Actor, db.manifest.NextSeq)
	return db.commitLocked(batch, false)
}

func (db *DB) commitLocked(batch Batch, remote bool) error {
	if db.closed {
		return ErrClosed
	}
	if db.failed != nil {
		return db.failed
	}
	if _, exists := db.state.Hashes[batch.First]; exists {
		candidate, err := db.state.apply(batch)
		if err != nil {
			return err
		}
		db.state = candidate
		return nil
	}
	candidate, err := db.state.apply(batch)
	if err != nil {
		return err
	}
	candidateIndexes, err := rebuildEqualityIndexes(context.Background(), candidate, cloneIndexDeclarations(db.indexes))
	if err != nil {
		return err
	}
	raw, err := batch.MarshalBinary()
	if err != nil {
		return err
	}
	if err := db.rotateForAppendLocked(int64(len(raw) + 32)); err != nil {
		return err
	}
	if err := appendFrame(segmentPath(db.path, db.manifest.Active), raw, db.durability == DurabilitySync); err != nil {
		return db.failWriteLocked(err)
	}
	if batch.First.Actor == db.manifest.Actor {
		next := db.manifest
		next.NextSeq = batch.First.Seq + uint64(batch.Count)
		if err := persistIdentity(db.path, next); err != nil {
			return db.failWriteLocked(err)
		}
		if err := writeManifest(db.path, next); err != nil {
			return db.failWriteLocked(err)
		}
		db.manifest = next
	}
	db.state = candidate
	db.indexes = candidateIndexes
	db.logger.Info("transaction.commit", "transaction", batch.ID(), "operations", batch.Count, "remote", remote)
	db.publishLocked(Change{Batch: batch, Remote: remote, Frontier: candidate.Frontier.Clone(), OccurredAt: time.Now().UTC()})
	return nil
}

func (db *DB) rotateForAppendLocked(frameBytes int64) error {
	path := segmentPath(db.path, db.manifest.Active)
	info, err := os.Stat(path)
	if err != nil {
		return err
	}
	if info.Size() <= 0 || info.Size()+frameBytes <= db.maxSegmentBytes {
		return nil
	}
	next := db.manifest
	next.Active++
	if err := ensureSegment(db.path, next); err != nil {
		return err
	}
	if err := writeManifest(db.path, next); err != nil {
		return db.failWriteLocked(fmt.Errorf("rotate active segment: %w", err))
	}
	db.manifest = next
	return nil
}

// ApplyRemote is intentionally disabled because a naked batch has no
// authenticated principal or signature. Network code must use the internal
// authenticated admission path below; applications should use Update for local
// writes rather than treating remote input as trusted local data.
func (db *DB) ApplyRemote(ctx context.Context, batch Batch) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return ErrClosed
	}
	if db.failed != nil {
		return db.failed
	}
	return ErrFeatureUnavailable
}

// applyRemoteFromPeer admits only a batch signed by the currently
// TLS-authenticated peer whose actor/key pair is durably authorized. It runs
// that defense-in-depth check before state application or WAL append.
func (db *DB) applyRemoteFromPeer(ctx context.Context, batch Batch, peerActor ActorID, peerKey ed25519.PublicKey) error {
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
	bound, authorized := db.actorKeys[peerActor]
	if batch.First.Actor != peerActor || batch.First.Actor == db.manifest.Actor || !authorized || len(peerKey) != ed25519.PublicKeySize || string(bound) != string(peerKey) {
		return fmt.Errorf("%w: remote actor provenance", ErrAuthorizationDenied)
	}
	if err := db.authorizeRemoteBatchLocked(peerActor, batch); err != nil {
		return err
	}
	return db.commitLocked(batch, true)
}

func (db *DB) publishLocked(change Change) {
	for _, channel := range db.subs {
		select {
		case channel <- change:
		default: /* bounded, coalescing caller may re-read */
		}
	}
}

// View executes against a single immutable materialized root.
func (db *DB) View(ctx context.Context, fn func(*ReadTx) error) error {
	if fn == nil {
		return fmt.Errorf("%w: nil view function", ErrInvalidArgument)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	db.mu.RLock()
	if db.closed {
		db.mu.RUnlock()
		return ErrClosed
	}
	if db.failed != nil {
		err := db.failed
		db.mu.RUnlock()
		return err
	}
	root := db.state
	db.mu.RUnlock()
	return fn(&ReadTx{state: root})
}

type ReadTx struct{ state *state }

func (tx *ReadTx) Document(collection, id string) (DocumentView, bool) {
	key := DocumentKey{Collection: collection, ID: id}
	document, ok := tx.state.Documents[key]
	if !ok {
		return DocumentView{}, false
	}
	return DocumentView{key: key, doc: document}, true
}
func (tx *ReadTx) Frontier() VersionVector { return tx.state.Frontier.Clone() }

type draftOperation struct {
	key        DocumentKey
	path       []string
	action     Action
	value      Value
	delta      int64
	object     ObjectID
	objectKind ObjectKind
	anchor     ElementID
	anchorHead bool
	values     []Value
	targets    []ElementID
}
type Tx struct {
	base         *state
	dependencies VersionVector
	actor        ActorID
	sequence     uint64
	operations   []draftOperation
}

func (tx *Tx) nextDot() Dot {
	return Dot{Actor: tx.actor, Seq: tx.sequence + uint64(len(tx.operations))}
}

func (tx *Tx) Document(collection, id string) *DocumentTx {
	return &DocumentTx{tx: tx, key: DocumentKey{Collection: collection, ID: id}}
}

type DocumentTx struct {
	tx  *Tx
	key DocumentKey
}

func (d *DocumentTx) validatePath(path []string) error {
	if err := d.key.Validate(); err != nil {
		return err
	}
	if len(path) == 0 || len(path) > maxPathParts {
		return ErrResourceLimit
	}
	for _, part := range path {
		if part == "" || len(part) > maxStringBytes {
			return fmt.Errorf("%w: path", ErrInvalidArgument)
		}
	}
	return nil
}

func (d *DocumentTx) append(action Action, path []string, value Value, delta int64) error {
	if action == DocumentDelete {
		if err := d.key.Validate(); err != nil {
			return err
		}
	} else if err := d.validatePath(path); err != nil {
		return err
	}
	if action != CounterAdd && action != DocumentDelete {
		if err := value.Validate(); err != nil {
			return err
		}
	}
	d.tx.operations = append(d.tx.operations, draftOperation{key: d.key, path: append([]string(nil), path...), action: action, value: cloneValue(value), delta: delta})
	return nil
}
func (d *DocumentTx) Set(path string, value Value) error {
	return d.SetPath([]string{path}, value)
}
func (d *DocumentTx) SetPath(path []string, value Value) error {
	return d.append(MapAssign, path, value, 0)
}
func (d *DocumentTx) Delete(path string) error {
	return d.DeletePath([]string{path})
}
func (d *DocumentTx) DeletePath(path []string) error {
	return d.append(MapDelete, path, Value{}, 0)
}
func (d *DocumentTx) DeleteDocument() error { return d.append(DocumentDelete, nil, Value{}, 0) }
func (d *DocumentTx) SetAdd(path string, value Value) error {
	return d.SetAddPath([]string{path}, value)
}
func (d *DocumentTx) SetAddPath(path []string, value Value) error {
	return d.append(SetAdd, path, value, 0)
}
func (d *DocumentTx) SetRemove(path string, value Value) error {
	return d.SetRemovePath([]string{path}, value)
}
func (d *DocumentTx) SetRemovePath(path []string, value Value) error {
	return d.append(SetRemove, path, value, 0)
}
func (d *DocumentTx) CounterAdd(path string, delta int64) error {
	return d.CounterAddPath([]string{path}, delta)
}
func (d *DocumentTx) CounterAddPath(path []string, delta int64) error {
	return d.append(CounterAdd, path, Value{}, delta)
}

// ListTx appends immutable anchored sequence operations to its surrounding
// transaction. A nil anchor denotes the distinguished HEAD anchor.
type ListTx struct {
	tx     *Tx
	key    DocumentKey
	object ObjectID
	kind   ObjectKind
}

func (list *ListTx) ObjectID() ObjectID { return list.object }

func (d *DocumentTx) createSequence(path []string, kind ObjectKind) (*ListTx, error) {
	if err := d.validatePath(path); err != nil {
		return nil, err
	}
	if !kind.valid() {
		return nil, fmt.Errorf("%w: sequence kind", ErrInvalidArgument)
	}
	object := ObjectID{Dot: d.tx.nextDot()}
	d.tx.operations = append(d.tx.operations, draftOperation{key: d.key, action: MakeList, object: object, objectKind: kind})
	reference := ListRef(object)
	if kind == TextObject {
		reference = TextRef(object)
	}
	d.tx.operations = append(d.tx.operations, draftOperation{key: d.key, path: append([]string(nil), path...), action: MapAssign, value: reference})
	return &ListTx{tx: d.tx, key: d.key, object: object, kind: kind}, nil
}

// CreateList stores a stable list reference at path and returns a transaction
// handle for anchored insert/delete operations.
func (d *DocumentTx) CreateList(path string) (*ListTx, error) {
	return d.CreateListPath([]string{path})
}
func (d *DocumentTx) CreateListPath(path []string) (*ListTx, error) {
	return d.createSequence(path, ListObject)
}

// CreateText stores a text sequence. Positions are represented by stable
// element IDs; public text insertion splits UTF-8 into Unicode code points.
func (d *DocumentTx) CreateText(path string) (*ListTx, error) {
	return d.CreateTextPath([]string{path})
}
func (d *DocumentTx) CreateTextPath(path []string) (*ListTx, error) {
	return d.createSequence(path, TextObject)
}

// List returns a handle for an existing object. The object identity must have
// been obtained from an immutable read view or created in this transaction.
func (d *DocumentTx) List(object ObjectID) *ListTx {
	return &ListTx{tx: d.tx, key: d.key, object: object}
}

// Text returns a text-specific handle for an existing text object.
func (d *DocumentTx) Text(object ObjectID) *ListTx {
	return &ListTx{tx: d.tx, key: d.key, object: object, kind: TextObject}
}

func (list *ListTx) InsertAfter(anchor *ElementID, values ...Value) ([]ElementID, error) {
	if !list.object.valid() || len(values) == 0 || len(values) > maxBatchOperations {
		return nil, fmt.Errorf("%w: list insertion", ErrInvalidArgument)
	}
	if anchor != nil && !anchor.valid() {
		return nil, fmt.Errorf("%w: list anchor", ErrInvalidArgument)
	}
	copyValues := make([]Value, len(values))
	for index, value := range values {
		if err := value.Validate(); err != nil {
			return nil, err
		}
		if list.kind == TextObject && (value.Kind != StringValue || len([]rune(value.Text)) != 1) {
			return nil, fmt.Errorf("%w: text element", ErrInvalidArgument)
		}
		copyValues[index] = cloneValue(value)
	}
	dot := list.tx.nextDot()
	draft := draftOperation{key: list.key, action: ListInsert, object: list.object, anchorHead: anchor == nil, values: copyValues}
	if anchor != nil {
		draft.anchor = *anchor
	}
	list.tx.operations = append(list.tx.operations, draft)
	ids := make([]ElementID, len(values))
	for index := range ids {
		ids[index] = ElementID{Dot: dot, Offset: uint32(index)}
	}
	return ids, nil
}

func (list *ListTx) Delete(targets ...ElementID) error {
	if !list.object.valid() || len(targets) == 0 || len(targets) > maxBatchOperations {
		return fmt.Errorf("%w: list deletion", ErrInvalidArgument)
	}
	copyTargets := append([]ElementID(nil), targets...)
	sort.Slice(copyTargets, func(left, right int) bool { return copyTargets[left].Compare(copyTargets[right]) < 0 })
	for index, target := range copyTargets {
		if !target.valid() || (index > 0 && copyTargets[index-1] == target) {
			return fmt.Errorf("%w: list deletion targets", ErrInvalidArgument)
		}
	}
	list.tx.operations = append(list.tx.operations, draftOperation{key: list.key, action: ListDelete, object: list.object, targets: copyTargets})
	return nil
}

// Replace is represented as a tombstone plus insertion after the old element.
// The old element remains an anchor, so concurrent children are not lost.
func (list *ListTx) Replace(target ElementID, value Value) ([]ElementID, error) {
	if err := list.Delete(target); err != nil {
		return nil, err
	}
	return list.InsertAfter(&target, value)
}

// InsertTextAfter inserts Unicode code points in order. It rejects invalid
// UTF-8 and is only valid for handles returned by CreateText.
func (list *ListTx) InsertTextAfter(anchor *ElementID, text string) ([]ElementID, error) {
	if list.kind != TextObject || text == "" || !utf8.ValidString(text) {
		return nil, fmt.Errorf("%w: text insertion", ErrInvalidArgument)
	}
	values := make([]Value, 0, utf8.RuneCountInString(text))
	for _, runeValue := range text {
		values = append(values, String(string(runeValue)))
	}
	return list.InsertAfter(anchor, values...)
}

func (tx *Tx) batch(actor ActorID, sequence uint64) Batch {
	operations := make([]Operation, len(tx.operations))
	for index, draft := range tx.operations {
		values := make([]Value, len(draft.values))
		for valueIndex, value := range draft.values {
			values[valueIndex] = cloneValue(value)
		}
		operations[index] = Operation{Dot: Dot{Actor: actor, Seq: sequence + uint64(index)}, Document: draft.key, Path: draft.path, Action: draft.action, Value: cloneValue(draft.value), Delta: draft.delta, Object: draft.object, ObjectKind: draft.objectKind, Anchor: draft.anchor, AnchorHead: draft.anchorHead, Values: values, Targets: append([]ElementID(nil), draft.targets...)}
	}
	return Batch{First: operations[0].Dot, Count: uint32(len(operations)), Dependencies: tx.dependencies.Clone(), Operations: operations, TimestampNanos: time.Now().UTC().UnixNano()}
}

func (db *DB) Subscribe(buffer int) (<-chan Change, func(), error) {
	if buffer < 1 || buffer > 65536 {
		return nil, nil, fmt.Errorf("%w: subscription buffer", ErrInvalidArgument)
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return nil, nil, ErrClosed
	}
	if db.failed != nil {
		return nil, nil, db.failed
	}
	id := db.nextSub
	db.nextSub++
	channel := make(chan Change, buffer)
	db.subs[id] = channel
	return channel, func() {
		db.mu.Lock()
		if channel, ok := db.subs[id]; ok {
			delete(db.subs, id)
			close(channel)
		}
		db.mu.Unlock()
	}, nil
}

func (db *DB) Status() (Status, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return Status{}, ErrClosed
	}
	if db.failed != nil {
		return Status{}, db.failed
	}
	return Status{DatabaseID: db.manifest.Database, ActorID: db.manifest.Actor, Durability: db.durability, Documents: len(db.state.Documents), Operations: len(db.state.Batches), KnownActors: len(db.state.Frontier), Frontier: db.state.Frontier.Clone(), ActiveSegment: db.manifest.Active, Snapshot: db.manifest.Snapshot, LogicalDigest: db.state.digest(db.manifest.Database), SubscriptionCount: len(db.subs)}, nil
}
func (db *DB) LogicalDigest() ([32]byte, error) {
	status, err := db.Status()
	return status.LogicalDigest, err
}

// Snapshot writes a fully validated historical checkpoint, rotates the active
// segment, then atomically points the manifest at the new snapshot.
func (db *DB) Snapshot(ctx context.Context) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return "", ErrClosed
	}
	if db.failed != nil {
		return "", db.failed
	}
	name, err := writeSnapshot(db.path, db.manifest, db.state)
	if err != nil {
		return "", err
	}
	next := db.manifest
	next.Snapshot = name
	next.Active++
	if err := ensureSegment(db.path, next); err != nil {
		return "", err
	}
	if err := writeManifest(db.path, next); err != nil {
		return "", db.failWriteLocked(fmt.Errorf("publish snapshot: %w", err))
	}
	db.manifest = next
	if err := removeObsoleteSegments(db.path, next.Active); err != nil {
		// The new snapshot and manifest are already durable; retaining obsolete
		// segments is safe and cleanup can be retried by the next checkpoint.
		db.logger.Warn("compaction.cleanup_deferred", "error", err)
	}
	db.logger.Info("snapshot.create", "file", name, "operations", len(db.state.Batches))
	return name, nil
}

func (db *DB) History(collection, id string) ([]Batch, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return nil, ErrClosed
	}
	if db.failed != nil {
		return nil, db.failed
	}
	key := DocumentKey{Collection: collection, ID: id}
	batches := make([]Batch, 0, len(db.state.Batches))
	for _, batch := range db.state.Batches {
		if collection == "" {
			batches = append(batches, batch)
			continue
		}
		for _, operation := range batch.Operations {
			if operation.Document == key {
				batches = append(batches, batch)
				break
			}
		}
	}
	BatchSort(batches)
	return batches, nil
}

func (db *DB) Verify(ctx context.Context, verifyBlobs bool) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	db.mu.RLock()
	if db.closed {
		db.mu.RUnlock()
		return ErrClosed
	}
	if db.failed != nil {
		err := db.failed
		db.mu.RUnlock()
		return err
	}
	m, expected := db.manifest, db.state.digest(db.manifest.Database)
	path := db.path
	db.mu.RUnlock()
	identityDB, identityActor, _, err := parseIdentityFile(path)
	if err != nil {
		return err
	}
	if identityDB != m.Database || identityActor != m.Actor {
		return fmt.Errorf("%w: identity mismatch", ErrCorruption)
	}
	all, err := readSnapshot(path, m.Snapshot)
	if err != nil {
		return err
	}
	if _, err := replaySegments(path, m, &all, false); err != nil {
		return err
	}
	root, err := replayCausally(all)
	if err != nil {
		return err
	}
	if root.digest(m.Database) != expected {
		return fmt.Errorf("%w: logical digest mismatch", ErrCorruption)
	}
	if verifyBlobs {
		if err := db.VerifyBlobs(); err != nil {
			return err
		}
	}
	return nil
}

// Documents returns canonical document keys for local query and export tooling.
func (db *DB) Documents(collection string) ([]DocumentKey, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return nil, ErrClosed
	}
	if db.failed != nil {
		return nil, db.failed
	}
	keys := make([]DocumentKey, 0, len(db.state.Documents))
	for key := range db.state.Documents {
		if collection == "" || key.Collection == collection {
			keys = append(keys, key)
		}
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i].String() < keys[j].String() })
	return keys, nil
}

func (db *DB) fingerprint() string {
	digest, _ := db.LogicalDigest()
	return fmt.Sprintf("%x", sha256.Sum256(digest[:]))
}
