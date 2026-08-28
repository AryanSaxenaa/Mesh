# Mesh0 Detailed Build Specification

## Executive Summary

Mesh0 is a local-first, embedded, peer-to-peer database designed so that every authorized device can remain fully readable and writable without a server, merge concurrent changes deterministically, survive crashes, synchronize efficiently after long periods offline, and preserve user ownership of the underlying data.

The product promise is:

> **Every replica is a real database, not a cache. The network is optional for reads and writes. When replicas reconnect, accepted changes converge without a central serialization authority.**

Mesh0 is implemented in Go as one self-contained executable and an importable set of packages from the same repository. The runtime module graph contains zero third-party modules. It must not wrap SQLite, RocksDB, Badger, Bolt, LevelDB, Redis, PostgreSQL, Automerge, Yjs, a CRDT package, a networking framework, a serialization framework, or an external synchronization service.

Mesh0 implements its own:

- durable append-only storage,
- crash recovery,
- snapshots,
- compaction,
- document data model,
- CRDT semantics,
- causal clocks,
- operation batching,
- secondary indexes,
- query planner,
- subscriptions,
- replication protocol,
- peer authentication,
- Merkle/range reconciliation,
- blob store,
- peer discovery profile,
- conflict inspection,
- transaction layer,
- backup/restore,
- consistency verification,
- deterministic simulation and fault testing.

The implementation uses only the Go standard library and code written in this repository.

Five principles define the system:

1. **Local authority.** Every replica serves reads and accepts authorized local writes without consulting the network.
2. **Deterministic convergence.** Replicas that eventually receive the same valid changes converge to the same logical state regardless of message order, duplication, delay, or temporary partition.
3. **Durable before acknowledged.** A committed local transaction is not reported successful until its durability contract has been satisfied.
4. **Conflicts are data.** Concurrent intent is never silently erased merely because one machine's wall clock is larger.
5. **No hidden infrastructure.** Core storage, merge, synchronization, security, and query behavior require no external database, cloud API, broker, daemon, or third-party runtime package.

---

# 1. Product Definition

## 1.1 What Mesh0 is

Mesh0 is simultaneously:

- an embedded local database,
- a CRDT document engine,
- a replication engine,
- a peer-to-peer synchronization protocol,
- a durable operation log,
- a queryable local materialized view,
- a content-addressed blob store,
- a historical change store,
- an offline collaboration substrate.

An application should be able to treat Mesh0 as its primary data layer on each device.

Conceptual application flow:

```text
user action
   │
   ▼
local transaction
   │
   ├── validate
   ├── assign operation identities
   ├── append durable batch
   ├── apply CRDT state
   ├── update indexes
   └── notify local subscribers
   │
   ▼
immediate local result

... later, when peers are reachable ...

peer synchronization
   │
   ├── authenticate
   ├── exchange causal summaries
   ├── identify missing ranges
   ├── transfer operation batches / snapshots / blobs
   ├── validate
   ├── persist durably
   ├── merge
   ├── update indexes
   └── notify subscribers
   │
   ▼
converged replicas
```

## 1.2 Primary use cases

Mesh0 should support:

- collaborative notes and documents,
- offline-capable project/task systems,
- shared personal knowledge bases,
- multi-device application state,
- collaborative forms and field data collection,
- local-first dashboards,
- small-team shared databases,
- field applications with intermittent connectivity,
- embedded desktop tools,
- peer-to-peer media metadata,
- local-first developer tools,
- applications that need audit/history and branching without a central database.

## 1.3 Primary user experience

Create a database:

```bash
mesh0 init ./data
```

Write locally:

```bash
mesh0 put ./data tasks/42 \
  title="Ship renderer" \
  done=false
```

Read locally:

```bash
mesh0 get ./data tasks/42
```

Start peer service:

```bash
mesh0 serve ./data --listen 0.0.0.0:7340
```

Connect another replica:

```bash
mesh0 peer add ./data laptop-b 192.0.2.44:7340
mesh0 sync ./data laptop-b
```

Inspect convergence:

```bash
mesh0 status ./data
```

Example:

```text
DATABASE STATUS

replica
  actor       G7P4...D2K
  durable     yes
  documents   18,492

history
  local operations     84,211
  known actors         4
  causal frontier      complete

peers
  laptop-b             synchronized
  workstation          2,184 operations behind
  field-tablet         offline

integrity
  manifest             valid
  segments             valid
  snapshot             valid
  blob store           valid
```

## 1.4 Product non-goals

Mesh0 is not:

- a globally serializable distributed SQL database,
- a consensus system,
- a replacement for consensus where a single global order is required,
- a blockchain,
- a cryptocurrency ledger,
- a cloud service,
- a mandatory central server,
- a transparent filesystem,
- a full relational database compatibility layer,
- a guarantee that concurrent business decisions can always be merged automatically.

Applications requiring invariants such as:

```text
bank balance may never go below zero globally
username must be globally unique before any offline commit
inventory must never oversell across disconnected replicas
one and only one global leader
```

cannot obtain those guarantees from availability-preserving partition-tolerant local writes alone.

Mesh0 must document when application invariants require coordination.

---

# 2. Local-First Contract

Mesh0 adopts a strict local-first architecture.

## 2.1 Local reads

A local read must not require:

- Internet access,
- a sync server,
- peer reachability,
- cloud authentication,
- a remote database round trip.

## 2.2 Local writes

An authorized local write must commit while disconnected.

Network synchronization happens after local durability, not before.

## 2.3 Replica authority

Each replica stores an authoritative copy of the application data it has chosen to retain.

A peer or relay may assist with synchronization, but it is not the source of truth by architectural necessity.

## 2.4 Network optionality

If every network service disappears, an existing replica must continue to support:

```text
read
write
query
history
export
backup
local transactions
local subscriptions
```

## 2.5 Collaboration

When network connectivity returns, peers synchronize changes and converge.

No peer is privileged merely because it happened to remain online.

---

# 3. Implementation Language and Dependency Contract

## 3.1 Language

Mesh0 is implemented in Go.

The standard library provides useful primitives for:

- files and directories,
- buffered I/O,
- binary encoding,
- CRC checksums,
- SHA-256,
- Ed25519,
- TLS,
- TCP/UDP networking,
- HTTP,
- JSON,
- compression,
- concurrency,
- runtime profiling/testing,
- sorting,
- atomic operations,
- file locking primitives through platform syscall APIs where needed.

## 3.2 Runtime module graph

`go.mod` contains no third-party requirements.

No runtime import of:

```text
golang.org/x/*
sqlite
modernc sqlite
bolt
bbolt
badger
pebble
rocksdb wrappers
leveldb
raft
etcd
grpc
protobuf
flatbuffers
msgpack
yaml libraries
toml libraries
cobra
viper
zerolog
zap
uuid libraries
CRDT libraries
Merkle-tree libraries
WebSocket libraries
QUIC libraries
noise protocol packages
JWT libraries
```

## 3.3 No external helper loophole

Mesh0 must not shell out to:

```text
sqlite3
psql
redis-cli
etcd
rsync
rclone
ssh
openssl
curl
wget
tar
gzip
zstd
jq
git
system database tools
```

for core database behavior.

The database must remain functional on a machine containing only the Go-built Mesh0 binary and ordinary operating-system facilities.

## 3.4 One executable

The main artifact:

```text
mesh0
```

supports:

- database administration,
- peer service,
- synchronization,
- integrity checks,
- backup,
- inspection,
- built-in self-tests.

The same repository exposes internal/importable Go packages for applications that compile Mesh0 into their own binary.

No external daemon is mandatory.

## 3.5 CGO policy

Canonical builds use:

```bash
CGO_ENABLED=0
```

No hidden native database or crypto library is loaded dynamically.

---

# 4. Reproducible Build Contract

Canonical build:

```bash
CGO_ENABLED=0 go build \
  -mod=readonly \
  -trimpath \
  -buildvcs=false \
  -ldflags='-buildid=' \
  -o mesh0 \
  ./cmd/mesh0
```

Build output must avoid unstable metadata:

- build timestamp,
- host path,
- hostname,
- user name,
- random build identifier,
- implicit VCS state.

Repository automation:

1. prepares two equivalent clean source trees,
2. builds both,
3. computes SHA-256,
4. fails if bytes differ.

A reproducibility claim is accepted only after this check passes.

---

# 5. Consistency Model

Mesh0 provides **causal, convergent local-first consistency**.

## 5.1 Core guarantee

If two correct replicas:

- start from compatible state,
- accept only valid operations,
- eventually receive the same set of operations,
- have no permanently missing required blob/state dependencies,

then they converge to the same logical database state.

This must hold regardless of:

- delivery order,
- duplicate delivery,
- batching differences,
- temporary disconnection,
- connection topology,
- retransmission.

## 5.2 What is not guaranteed

Mesh0 does not provide a total global operation order while replicas are partitioned.

A read on replica A does not necessarily immediately observe a write committed on disconnected replica B.

## 5.3 Causal visibility

A transaction's causal dependencies must be present before the transaction becomes visible.

If transaction B is created after observing transaction A, a receiving replica must not expose B while A is missing.

## 5.4 Atomic batch visibility

Operations created by one committed local transaction are transported and applied as one atomic visibility batch.

A remote replica either:

- has not exposed the transaction,
- or exposes all logical operations in the transaction.

This makes multi-object local updates usable without partially observed remote state.

---

# 6. Identity Model

## 6.1 Database identity

Each independent database has a cryptographically random `DatabaseID`.

Generated using:

```text
crypto/rand
```

The identifier is not derived from user data.

## 6.2 Replica identity

Each replica has an `ActorID`.

Preferred representation:

```text
32 random bytes
```

or an identifier derived from an Ed25519 public key when signed-operation mode is enabled.

Display form uses a repository-owned base encoding with checksum.

## 6.3 Operation identity

Every operation is uniquely identified by:

```text
Dot = (ActorID, Sequence)
```

where `Sequence` is a strictly increasing unsigned integer local to the actor.

Example:

```text
actor G7P4...D2K
sequence 48193
```

No wall clock is part of uniqueness.

## 6.4 Transaction identity

A transaction is identified by:

```text
TxnID = first operation dot + operation count
```

or a separate random identifier when zero-operation metadata transactions are supported.

The durable batch records explicit operation range.

---

# 7. Causal Clock

Each replica maintains a compact causal summary.

Baseline structure:

```text
VersionVector:
  ActorID -> largest contiguous accepted sequence
```

Because networks can deliver out of order, the receiver also maintains bounded gap/range information for operations above the contiguous frontier.

Conceptually:

```text
actor A:
  contiguous: 100
  received: [103, 104, 108]
```

After receiving 101 and 102:

```text
contiguous: 104
received: [108]
```

## 7.1 Transaction dependencies

A local transaction records the causal frontier visible at transaction start or commit according to API semantics.

This frontier defines "happened before."

## 7.2 Clock comparison

Two changes can be:

```text
before
after
equal
concurrent
```

Concurrency is a causal property, not a timestamp comparison.

## 7.3 Wall clocks

Wall-clock timestamps may be recorded as human-readable metadata.

They never determine convergence-critical conflict winners.

A machine with an incorrect clock must not cause data loss.

---

# 8. Logical Data Model

Mesh0 exposes databases containing collections and documents.

```text
Database
└── Collection
    └── Document
        └── Values
```

Supported value types:

```text
Null
Bool
Int64
Float64
String
Bytes reference
Timestamp metadata
Map
List
Set
Counter
BlobRef
```

Applications can build richer schemas above these primitives.

## 8.1 Value identity

Container values have stable object identifiers.

A nested map/list is not identified solely by its current path because paths may change concurrently.

`ObjectID` is normally the dot of the operation that created the object.

## 8.2 Document identity

Document IDs are application-provided bytes/strings subject to size limits.

The document ID does not change when fields are edited.

## 8.3 Schema

Mesh0 is schemaless at storage level but supports optional local schema validation.

Schema validation can enforce:

- required fields,
- type rules,
- numeric ranges,
- string limits,
- collection constraints.

Schema policy does not alter convergence rules.

If schemas differ across replicas, valid remote operations must not be silently dropped merely because a newer local schema dislikes historical data. Schema evolution requires explicit application policy.

---

# 9. Operation Model

Every mutation becomes an immutable operation.

Conceptual operation:

```text
Operation {
    Dot
    TxnID
    ObjectID
    Action
    KeyOrAnchor
    Value
    Dependencies
    Metadata
}
```

Possible actions:

```text
MakeMap
MakeList
MapAssign
MapDelete
SetAdd
SetRemove
CounterAdd
ListInsert
ListDelete
ListAssign
BlobAttach
DocumentDelete
```

Operations are immutable after durable commit.

## 9.1 Deterministic binary encoding

Operations use a repository-owned binary format.

Do not use Go `gob` for stable on-disk or network protocol state.

Encoding primitives:

```text
unsigned varint
signed zigzag varint
fixed byte arrays
length-prefixed byte strings
sorted repeated fields where order is semantic-free
```

All decoders enforce:

- maximum lengths,
- integer overflow checks,
- canonical varint encoding,
- unknown-field policy,
- duplicate-field rules.

---

# 10. Map CRDT

Map entries use observed assignment semantics.

Each key may have one or more currently visible concurrent assignments.

Example:

Replica A offline:

```text
status = "ready"
```

Replica B offline:

```text
status = "blocked"
```

After synchronization, Mesh0 must not arbitrarily erase one value using wall time.

Internal state:

```text
status:
  value "ready"   dot A:81
  value "blocked" dot B:19
```

Application read APIs choose one of:

### Conflict-aware read

Returns all concurrent values.

### Deterministic projection

Returns one stable projected value using a deterministic causal tie-break, while still preserving conflict metadata.

Projection ordering can use operation identity ordering, never host wall clock.

### Application resolver

Application supplies a pure resolver over concurrent values.

If the resolver writes a resolution transaction, that new transaction causally supersedes the observed conflicts.

## 10.1 Assignment semantics

An assignment supersedes assignments visible in its causal context.

Concurrent assignments survive.

## 10.2 Delete semantics

Map delete removes assignments observed by the deleting operation.

A concurrent unseen assignment survives.

This prevents offline updates from disappearing simply because another replica deleted an older value.

---

# 11. Set CRDT

Use an observed-remove set design.

## 11.1 Add

Each add has a unique dot.

## 11.2 Remove

Remove records the add dots it observed for the element.

Concurrent adds survive a remove that did not observe them.

Example:

```text
A adds "alice"
B removes "alice" without seeing A's add
```

After merge:

```text
"alice" remains
```

because the add and remove were concurrent.

## 11.3 Tombstone compaction

Do not retain individual removed dots forever when a causal summary can safely represent that they are dominated.

Use dotted causal context/range summaries where correctness permits.

Compaction tests must prove merge equivalence before and after compaction.

---

# 12. Counter CRDT

Counter increments are commutative operations:

```text
CounterAdd(delta int64)
```

Each operation applies once by dot identity.

Concurrent increments add naturally.

Integer overflow behavior is explicit:

- reject a local mutation that would exceed supported range,
- treat a remote operation causing impossible state as invalid/corrupt rather than wrapping.

Optional large-integer counters can be implemented using `math/big` from the standard library.

---

# 13. Sequence CRDT

Lists and collaborative text require deterministic concurrent insertion.

Mesh0 uses an anchor-based sequence CRDT.

Each inserted element has:

```text
ElementID = operation dot + local element offset
```

An insertion references an existing logical anchor.

Example:

```text
HEAD
  └── A1 "H"
      └── A2 "i"
```

Concurrent inserts after the same anchor are ordered by deterministic element identity.

## 13.1 Insert semantics

Operation:

```text
Insert(after ElementID, values...)
```

If multiple replicas insert after the same anchor concurrently, all inserts survive.

## 13.2 Delete semantics

Deletion marks observed element identities deleted.

Deleted elements may remain as structural anchors until safely compacted.

## 13.3 Assignment semantics

List element replacement is modeled as explicit value assignment to an element identity or delete+insert depending on public API.

The chosen semantics are documented and tested.

## 13.4 Stable ordering

Ordering must depend only on immutable operation identity and ancestry.

Never depend on arrival order.

## 13.5 Tombstone pressure

Sequence tombstone compaction is a dedicated research/engineering subsystem.

The baseline safe rule:

> Never remove an anchor if a valid future or offline operation may still reference it unless a compact substitute preserves the required ordering relationship.

Potential compaction representation:

```text
AnchorSummary {
    removed interval
    predecessor
    successor ordering metadata
    causal dominance
}
```

No compact representation ships until property tests prove equivalence against the uncompressed model.

---

# 14. Text CRDT

Text is built on the sequence CRDT with chunk-aware optimizations.

Do not store one Go heap object per character in mature builds.

Use:

```text
text chunks
element-ID intervals
tombstone bitmaps/ranges
UTF-8 byte storage
code-point indexes
```

Supported operations:

```text
insert text
delete range
replace range
apply mark
remove mark
```

## 14.1 Rich-text marks

Optional mark CRDT supports spans such as:

```text
bold
italic
link
comment metadata
application-defined marks
```

Marks use anchored interval semantics.

Concurrent mark operations merge without destroying text.

---

# 15. Document Deletion

Document deletion is logical and causal.

A deletion supersedes document state it observed.

Concurrent edits that were not observed by the deletion require explicit policy:

- preserve as conflict/revival,
- hide under tombstone while retaining recoverable concurrent changes.

Default should favor data preservation.

`mesh0 history` must allow inspection of deleted documents.

---

# 16. Local Transactions

Public Go API concept:

```go
err := db.Update(ctx, func(tx *mesh0.Tx) error {
    doc := tx.Document("tasks", "42")
    doc.Set("done", true)
    doc.Counter("edits").Add(1)
    return nil
})
```

## 16.1 Transaction properties

Local transaction provides:

- atomic local visibility,
- one causal dependency frontier,
- one durable operation batch,
- deterministic operation ordering,
- one subscription notification boundary.

## 16.2 Read view

Transaction reads from a stable local snapshot/frontier.

## 16.3 Write conflict

Because CRDT writes merge, local concurrent application goroutines do not necessarily need optimistic aborts for independent CRDT operations.

However, API-level compare-and-set is allowed only with clearly local/causal semantics.

Do not pretend a compare-and-set is globally exclusive across disconnected replicas.

## 16.4 Multi-document batch

One transaction may update multiple documents.

Remote replicas apply all operations in the batch atomically once dependencies and full batch are available.

This prevents half-visible application updates such as:

```text
move task from list A to list B
```

appearing remotely as two unrelated updates.

---

# 17. Coordinated Transactions

Some application invariants require coordination.

Mesh0 may offer an optional **online coordinated transaction** profile, but it must remain separate from ordinary local-first writes.

Potential semantics:

```text
quorum/leader coordination only when explicitly requested
fails unavailable during partition
```

This feature is not required for core correctness and must not contaminate the local-first path.

The API must make the availability trade-off obvious.

---

# 18. Storage Directory Layout

Example:

```text
data/
├── IDENTITY
├── MANIFEST
├── LOCK
├── actors/
├── segments/
│   ├── 00000001.seg
│   ├── 00000002.seg
│   └── ...
├── snapshots/
│   ├── snapshot-....m0s
│   └── ...
├── blobs/
│   ├── aa/
│   ├── ab/
│   └── ...
├── indexes/
├── peers/
└── tmp/
```

No user must understand this layout for normal operation.

Every file format has:

- magic bytes,
- format generation integer,
- declared header length,
- checksum where appropriate,
- explicit endianness,
- maximum supported sizes.

This integer is an on-disk format discriminator, not a product semantic-version label.

---

# 19. Write-Ahead Log

The durable source for new operations is an append-only segment log.

## 19.1 Record framing

Conceptual frame:

```text
magic/marker
record type
flags
payload length
sequence metadata
payload
CRC32C
```

Use:

```text
hash/crc32
Castagnoli polynomial
```

for corruption detection.

Cryptographic authenticity is separate.

## 19.2 Batch record

A transaction batch is encoded as a length-bounded atomic record or a begin/chunk/commit sequence whose recovery semantics guarantee incomplete batches are invisible.

Preferred approach for ordinary transaction sizes:

```text
single checksummed batch frame
```

Large transactions:

```text
BatchBegin
BatchChunk...
BatchCommit
```

Recovery ignores any batch lacking a valid commit record.

## 19.3 Commit sequence

Local durable commit:

1. validate mutation,
2. allocate operation identities,
3. encode canonical batch,
4. append,
5. flush buffered writer,
6. call file sync according to durability mode,
7. update in-memory CRDT state/indexes,
8. publish transaction to subscribers,
9. queue replication.

For the strongest durability mode, success is returned only after required fsync.

## 19.4 Durability modes

Explicit modes:

```text
sync
group
memory
```

### sync

Each transaction reaches durable storage before acknowledgment.

### group

Transactions may share a bounded fsync window. Acknowledgment occurs only after the group containing the transaction is synced.

### memory

Acknowledges before fsync.

Must be clearly labeled as losing acknowledged writes on crash.

Default should be `sync` or carefully bounded `group`.

---

# 20. Segment Rotation

Segments rotate based on:

- maximum bytes,
- maximum transaction count,
- explicit checkpoint.

A closed segment becomes immutable.

Immutable segments enable:

- checksumming,
- Merkle hashing,
- backup copying,
- corruption verification,
- safe compaction input.

---

# 21. Crash Recovery

On open:

1. acquire database lock,
2. load/validate identity,
3. load manifest,
4. locate latest valid snapshot,
5. verify snapshot footer/hash,
6. reconstruct materialized CRDT state,
7. replay later segment records,
8. detect partial tail,
9. ignore/truncate incomplete tail when writable,
10. rebuild or validate derived indexes,
11. verify actor sequence monotonicity,
12. expose database.

## 21.1 Torn writes

A partially written final record is expected after abrupt termination.

Recovery:

- detects invalid/incomplete frame,
- stops at last valid boundary,
- never interprets random suffix bytes as a record.

Corruption in the middle of an immutable segment is not treated as a harmless tail. Open should fail or enter explicit repair mode.

## 21.2 Manifest safety

Manifest update process:

1. write new temporary manifest,
2. fsync file,
3. atomic rename,
4. fsync containing directory where durability semantics require it.

Never overwrite the only known-good manifest in place.

---

# 22. Snapshots

A snapshot contains the complete mergeable logical CRDT state at a causal frontier.

It is not merely application values.

It includes metadata required for future correct merges:

```text
object identities
visible assignments
causal context
sequence anchors/tombstones required for future ordering
set causal summaries
counter state
document tombstones
actor frontiers
index seed metadata when desired
```

## 22.1 Canonical snapshot encoding

Ordering is deterministic.

Never serialize Go maps directly without sorting keys.

Canonical order examples:

```text
collections by byte key
documents by byte key
objects by ObjectID
map fields by byte key
actors by ActorID
operations/metadata by dot order
```

## 22.2 Snapshot integrity

Snapshot contains:

- content length,
- SHA-256,
- section hashes where useful,
- causal frontier,
- format generation,
- footer marker.

## 22.3 Snapshot atomicity

Write to temporary file, fsync, rename atomically, fsync directory where required.

Only after manifest points to new valid snapshot may obsolete files become cleanup candidates.

---

# 23. Compaction

Compaction reduces:

- operation log size,
- redundant overwritten map assignments,
- dominated causal entries,
- removed-set dots,
- index fragmentation,
- obsolete snapshots.

Correctness rule:

> Compaction may change physical representation but must not change the result of any valid future merge permitted by the retention policy.

## 23.1 Compaction verification

For randomized histories:

```text
state A = uncompact history
state B = compact history

merge same future operations into A and B

assert logical state identical
assert conflict state identical
assert causal frontier compatible
```

This property test is mandatory.

## 23.2 History retention modes

Possible policy:

```text
full
window
checkpoint
```

### full

Retain all accepted operations.

### window

Retain full history for configured recent range and older mergeable snapshots.

### checkpoint

Retain mergeable snapshot plus later operations.

No mode may discard metadata required for convergence with an authorized long-offline replica.

## 23.3 Peer retirement

Explicitly retiring a known peer can permit more aggressive garbage collection.

Retirement is an administrative action, never inferred merely because a peer has been offline for a long time.

---

# 24. State-Based Recovery Sync

A peer far behind should not require replaying years of operation history if a mergeable state snapshot is available.

Sync can choose:

```text
operation delta
snapshot/state transfer
hybrid
```

The receiving peer must preserve its own unshared valid local operations before replacing materialized state.

Safe high-level sequence:

1. exchange causal summaries,
2. send operations unique to lagging peer if remote needs them,
3. establish compatible merged frontier,
4. transfer snapshot representing known state,
5. replay operations beyond snapshot frontier,
6. verify convergence digest.

No peer overwrites another replica's unique offline work by blindly copying a newer snapshot.

---

# 25. Manifest

Manifest records current physical database topology:

```text
database identity
active segment
immutable segments
selected snapshot
index generations
blob roots
last durable actor sequence
compaction state
```

Manifest is physical metadata.

Logical state must be recoverable from snapshot + retained log even if derived index files are deleted.

---

# 26. Database Locking

Baseline supports one writer process per database directory.

Multiple goroutines inside one process are supported.

Use operating-system file locking through a small platform-specific standard-library/syscall layer.

Lock metadata may include:

```text
PID
random process nonce
start timestamp for diagnostics
```

Do not trust PID alone to prove lock ownership.

Read-only diagnostic mode may be allowed with careful semantics.

---

# 27. In-Memory State Engine

Materialized state should be optimized for:

- fast local reads,
- incremental application,
- stable object identity,
- conflict inspection,
- deterministic snapshot traversal.

Avoid one huge generic `map[string]any` representation.

Use typed structures:

```text
MapObject
ListObject
SetObject
CounterObject
DocumentState
```

## 27.1 Memory ownership

A committed state visible to readers is immutable or protected by clear locking/epoch rules.

Prefer copy-on-write at object/subtree granularity for read snapshots rather than locking the entire database for long queries.

---

# 28. Read Transactions

Conceptual API:

```go
err := db.View(ctx, func(tx *mesh0.ReadTx) error {
    doc, ok := tx.Document("tasks", "42")
    ...
    return nil
})
```

A read transaction sees a stable local frontier.

Concurrent local/remote commits do not produce half-updated objects inside the read view.

Long-lived read views may delay reclamation; expose metrics/warnings.

---

# 29. Secondary Indexes

Indexes are **derived local state** unless explicitly declared as synchronized application metadata.

The canonical CRDT data remains source of truth.

Supported initial indexes:

```text
hash equality
ordered scalar
prefix string
compound tuple
full-text profile
```

## 29.1 Index declaration

Example conceptual API:

```text
collection tasks
index by_status on $.status
index by_due on $.due
```

Index declarations can be stored in local configuration or synchronized schema metadata.

## 29.2 Index updates

Each committed transaction computes index delta from old/new projected values.

Remote merges update indexes through the same deterministic path.

## 29.3 Conflicts and indexes

A field with concurrent values can be indexed using one of explicit policies:

```text
all visible values
deterministic projected value
conflicted values excluded
```

Default should preserve discoverability by indexing all visible values where reasonable.

---

# 30. Ordered Index

Implement an in-repository B+ tree or immutable sorted-run structure.

Requirements:

- deterministic key ordering,
- prefix/range scan,
- bounded node sizes,
- crash-safe persistence if stored separately,
- rebuildable from canonical data.

Because indexes are derived, a corrupt index should trigger rebuild rather than logical data loss.

---

# 31. Full-Text Index

Optional local full-text index:

```text
tokenization
case folding profile
inverted postings
document frequency
BM25-style ranking
```

Implement with standard-library Unicode primitives and in-repository tokenization.

No language stemmer dependency.

Language-specific stemming is optional and must be explicitly identified.

Query example:

```bash
mesh0 query ./data notes --text "distributed database"
```

Full-text index does not affect convergence.

---

# 32. Query Language

Provide a deliberately small query language.

Example:

```text
from tasks
where status == "open"
  and due < "2026-09-01"
order by due asc
limit 50
```

Parser is handwritten.

Alternative JSON machine form is supported.

## 32.1 Query grammar

Support:

```text
collection source
field paths
comparisons
and/or/not
in
exists
prefix
range
ordering
limit
projection
text search profile
```

No arbitrary code execution in query expressions.

## 32.2 Query planner

Planner chooses:

```text
direct document lookup
equality index
ordered range index
compound index
full scan
full-text index
intersection
```

`mesh0 query --explain` shows plan.

Example:

```text
PLAN

collection: tasks
index: by_status_due

seek:
  status = "open"
  due < "2026-09-01"

post-filter:
  assignee exists

estimated candidates:
  184
```

---

# 33. Reactive Subscriptions

Applications can subscribe to:

```text
document
collection query
index range
conflict state
sync state
peer state
```

Subscription callback receives transaction boundary:

```text
added
removed
updated
conflicted
resolved
```

Remote transaction batch causes one logical notification.

Callbacks must not execute while holding internal storage locks.

Backpressure policy is explicit.

---

# 34. Change History

Mesh0 retains inspectable causal history according to retention policy.

Commands:

```bash
mesh0 history ./data tasks/42
mesh0 history ./data --actor laptop-a
mesh0 history ./data --since <frontier>
```

History entry displays:

```text
operation identity
transaction
actor
causal parents/frontier
fields changed
human timestamp metadata
signature status
source peer
```

Wall-clock time is informational only.

---

# 35. Conflict Inspection

First-class command:

```bash
mesh0 conflicts ./data
```

Example:

```text
CONFLICT

document:
  tasks/42

path:
  $.status

values:

  "ready"
    actor: laptop-a
    operation: A:81

  "blocked"
    actor: field-tablet
    operation: B:19

relationship:
  concurrent
```

Resolve:

```bash
mesh0 resolve ./data tasks/42 status="blocked"
```

The resolution is a new causal write that observes and supersedes the conflicting assignments.

Mesh0 never mutates history to pretend the conflict did not exist.

---

# 36. Branching

Optional branch support creates named local frontiers.

```bash
mesh0 branch create experiment
mesh0 branch checkout experiment
mesh0 branch merge main experiment
```

A branch is:

```text
name -> causal frontier / state root
```

Branches do not require copying the entire database.

Merge is simply CRDT state/history reconciliation between frontiers.

Application conflicts remain visible.

---

# 37. Time Travel

Read state at a historical frontier:

```bash
mesh0 get ./data tasks/42 --at <frontier>
```

Efficient implementation:

- choose nearest retained snapshot not after target frontier,
- replay required retained operations,
- materialize temporary read view.

If retention policy has discarded required detailed history, return an explicit error rather than fabricating approximate state.

---

# 38. Blob Store

Large binary data is stored outside CRDT operation records.

## 38.1 Content addressing

Blob identity:

```text
SHA256(content)
```

Small blobs may be stored whole.

Large blobs are split into chunks.

## 38.2 Chunking

Baseline uses fixed-size chunks for simplicity and determinism.

Optional content-defined chunking can be added using an in-repository rolling-hash algorithm after correctness/performance review.

## 38.3 Blob manifest

Large blob:

```text
BlobRoot
  total length
  chunk size policy
  ordered chunk hashes
  optional Merkle root
```

Document stores `BlobRef`.

## 38.4 Deduplication

Identical chunks share storage.

## 38.5 Lazy synchronization

Metadata can synchronize before large blob content.

Policy:

```text
eager
on-demand
metadata-only
```

A UI can display document metadata before a multi-gigabyte attachment arrives.

---

# 39. Blob Integrity

Every stored chunk is SHA-256 verified.

Read path:

1. locate chunk by hash,
2. read bounded length,
3. optionally verify hash according to cache/trust policy,
4. assemble stream.

`mesh0 verify --blobs` rehashes all content.

Corrupt content is never returned as if valid.

---

# 40. Replication Architecture

Mesh0 synchronization is peer-to-peer.

Topology can be:

```text
A <-> B
A <-> C
B <-> D
```

No particular peer must be globally designated primary.

A relay is optional convenience infrastructure and can be implemented by the same Mesh0 binary, but correctness must not require it.

---

# 41. Transport

Baseline transport is TCP protected by TLS.

Use:

```text
net
crypto/tls
crypto/x509
crypto/ed25519
crypto/rand
```

No custom cipher.

No custom key exchange.

Use standard-library cryptographic primitives.

## 41.1 Connection stages

```text
TCP
 ↓
TLS
 ↓
Mesh0 handshake
 ↓
database/peer authentication
 ↓
capability negotiation
 ↓
sync frames
```

## 41.2 TLS profile

Set explicit minimum TLS policy appropriate to supported Go runtime.

Do not enable insecure certificate skipping as a normal mode.

Peer trust is based on pinned/authorized identity, not public Web PKI necessity.

---

# 42. Peer Cryptographic Identity

Each replica generates an Ed25519 key pair.

Private key:

- generated from `crypto/rand`,
- stored with restrictive file permissions,
- never transmitted.

Public key identifies authenticated peer.

Peer name such as `laptop-a` is display metadata, not the trust anchor.

## 42.1 Pairing

Possible pairing:

```bash
mesh0 peer invite ./data
```

Outputs a short-lived invitation containing:

```text
database ID
peer public-key fingerprint
address hints
random invitation secret
expiry metadata
```

Second peer:

```bash
mesh0 peer accept ./data <invite>
```

Both sides pin identity after explicit confirmation.

Invitation parsing is bounded and authenticated.

---

# 43. Operation Authentication

For high-assurance collaborative databases, operation batches can be signed.

Signature covers canonical:

```text
DatabaseID
ActorID
transaction identity
causal dependencies hash
operation bytes hash
```

Use Ed25519.

A peer cannot rewrite another actor's operation without signature failure.

Unsigned local-only mode may be supported, but peer-authenticated mode should be the serious default for multi-user replication.

---

# 44. Authorization

Authentication proves peer identity.

Authorization defines what that identity may change.

Baseline capability model:

```text
database read
collection read
collection write
document read profile
document write profile
blob read/write
administrative peer management
```

Authorization rules are explicit.

## 44.1 Authorization point

Validate remote operation authorization **before** accepting it into durable canonical history.

Rejected operations are recorded in security diagnostics but do not become logical state.

## 44.2 Offline revocation reality

Revocation cannot make a peer forget data it already received while authorized.

Documentation must state this plainly.

A revoked peer may also create operations while offline using authority it believed it had. Acceptance of those operations after reconnection follows explicit revocation semantics.

Default high-assurance rule:

- authorization certificates carry a causal/administrative epoch,
- operations outside current accepted authority are rejected,
- revocation is not retroactive data erasure.

---

# 45. Sync Wire Format

Use a repository-owned binary framing protocol.

Frame:

```text
length varint
type varint
request/stream identifier
flags
payload
```

TLS provides transport integrity/confidentiality.

Payload decoders enforce per-frame and per-session limits.

No protobuf/MessagePack dependency.

## 45.1 Frame families

```text
Hello
Auth
DatabaseOpen
ClockSummary
RangeDigest
RangeRequest
TxnBatch
SnapshotOffer
SnapshotChunk
BlobWant
BlobChunk
Ack
Error
Ping
Goodbye
```

Unknown optional frame types can be skipped only when their length is valid and negotiation allows it.

Unknown required semantics cause protocol error.

---

# 46. Protocol Negotiation

Do not encode product semantic versions into behavior assumptions.

Handshake exchanges explicit feature/capability bits and a small wire-format generation integer.

Example capabilities:

```text
signed-batches
snapshot-transfer
blob-lazy-sync
range-digest
compression-gzip
query-subscription profile
```

Peers proceed only if required features overlap safely.

---

# 47. Anti-Entropy Sync

Synchronization must avoid sending the entire history on every connection.

## 47.1 Actor range summary

Because operations are actor-sequenced, peers first exchange:

```text
ActorID -> contiguous sequence + sparse ranges
```

This immediately identifies many missing operation ranges.

Example:

```text
Peer A:
  actor X: 1..1000
  actor Y: 1..400

Peer B:
  actor X: 1..700
  actor Y: 1..400

B requests:
  X:701..1000
```

## 47.2 Range digest tree

For large/divergent histories, each actor's immutable operation blocks have hashes.

Build hierarchical range digest:

```text
actor X
  root
   ├── 1..4096
   │    ├── 1..2048
   │    └── 2049..4096
   └── ...
```

Peers compare hashes to locate divergent/missing ranges logarithmically.

Hash:

```text
SHA256(domain separator || actor || range || child hashes / canonical batch hashes)
```

Never concatenate ambiguous raw fields without lengths/domain separation.

## 47.3 Duplicate delivery

Operation/batch identity makes retransmission idempotent.

A duplicate valid batch is acknowledged without reapplying effects.

---

# 48. Causal Dependency Transfer

If a peer receives transaction T but lacks causal dependency ranges:

```text
hold T pending
request missing dependencies
do not expose T
```

Pending transaction memory/disk queues are bounded.

An attacker cannot force unlimited pending dependency storage.

---

# 49. Durable Remote Acknowledgment

Ack levels:

```text
received
validated
durable
applied
```

Sync protocol uses explicit level.

For replication safety, "durable" means transaction bytes have crossed the receiver's durability boundary.

A sender may discard retry state only according to selected acknowledgment policy.

---

# 50. Convergence Digest

After sync, peers can exchange logical state digest.

Do not hash raw local physical storage because compaction/layout can differ.

Canonical digest includes:

```text
database logical identity
causal frontier
canonical CRDT state root
```

If frontiers match but logical digests differ:

```text
INVARIANT FAILURE
```

Do not continue pretending synchronization succeeded.

`mesh0 verify-convergence` exposes this mechanism.

---

# 51. Merkle State Tree

For large databases, maintain a canonical logical Merkle tree:

```text
root
├── collection A
│   ├── doc 1
│   ├── doc 2
│   └── ...
└── collection B
```

Document hash covers canonical mergeable CRDT state, not presentation-only projection.

Benefits:

- integrity checking,
- sync diagnostics,
- targeted state reconciliation,
- backup comparison.

Tree is derived and rebuildable.

---

# 52. Compression

Network/storage compression uses standard-library codecs only.

Supported baseline:

```text
gzip/DEFLATE profile
```

Compression is negotiated.

Never compress already-compressed/encrypted blob data blindly.

Decompression has hard output limits.

---

# 53. Peer Discovery

Core correctness never depends on automatic discovery.

Supported discovery modes:

```text
manual address
saved address book
LAN discovery profile
optional relay
```

## 53.1 LAN discovery

Implement with UDP multicast/broadcast using `net`.

Advertisements contain only minimal metadata.

Do not broadcast document names or user data.

Peer identity is still authenticated cryptographically after connection.

Discovery packets are hints, not trust.

---

# 54. Optional Relay

The same binary can run:

```bash
mesh0 relay --listen :7340
```

A relay:

- assists peers behind inconvenient network topology,
- stores/forwards encrypted or authenticated protocol traffic according to mode,
- is not authoritative for database state,
- can disappear without making existing replicas unusable.

Core local database behavior must not require a relay.

---

# 55. Sync Scheduling

Background scheduler uses:

```text
peer priority
recent connectivity
pending operation bytes
blob demand
backoff
battery/network hints supplied by application
```

Backoff includes randomized jitter from `crypto/rand` or non-security PRNG as appropriate.

Sync should coalesce many tiny changes.

Foreground APIs can force:

```bash
mesh0 sync ./data peer-name
```

---

# 56. Backpressure

Bound:

```text
outgoing queued bytes
incoming frame bytes
pending dependency bytes
unapplied transaction count
concurrent blob streams
concurrent peers
```

Slow peer must not exhaust memory.

Use streaming readers/writers.

Large blob chunks are never buffered entire-database-in-memory.

---

# 57. Network Partition Simulation

Built-in simulator can run multiple logical replicas in one process with a deterministic virtual network.

Configurable events:

```text
drop message
duplicate message
delay
reorder
partition groups
heal partition
disconnect peer
corrupt unauthenticated frame before TLS simulation layer
crash replica
restart replica
```

Seed is printed.

Any failure can be reproduced from seed + scenario.

---

# 58. Deterministic Convergence Testing

Core property:

```text
Given valid operation set O

for many permutations P(O):
    apply P(O) to fresh replica

all resulting canonical states must be identical
```

Extend with:

```text
duplicates
batch boundary changes
partial sync rounds
snapshot transfer
compaction
restart
```

This is one of the strongest release gates.

---

# 59. Exhaustive Small-State Model Checking

Build an in-repository model explorer for small histories.

Example dimensions:

```text
2-3 actors
1-3 documents
few fields
few sequence elements
insert/delete/assign
network partitions
```

Enumerate operation interleavings and verify:

- convergence,
- idempotence,
- causal visibility,
- conflict preservation,
- transaction atomicity.

The simple model should be intentionally slower and clearer than production structures.

Compare production engine result against model.

---

# 60. Crash Fault Injection

Every durability-sensitive storage step can expose test-only fault points:

```text
after frame header
mid payload
after payload
before checksum
after checksum
before fsync
after fsync
before manifest rename
after manifest rename
before directory fsync
during snapshot
during compaction
```

Fault harness:

1. fork test process,
2. trigger operation,
3. kill at chosen point,
4. reopen database,
5. verify committed/uncommitted contract,
6. verify no impossible partial transaction.

No external fault-testing framework is required.

---

# 61. Corruption Testing

Mutate copies of:

```text
segment header
record length
record payload
CRC
snapshot section
snapshot hash
manifest
blob
index
```

Expected response is classified:

```text
recoverable tail
derived index rebuild
hard corruption
repairable with peer
```

Never silently skip arbitrary corrupted canonical history.

---

# 62. Repair

`mesh0 repair` must be conservative.

Possible repairs:

```text
truncate known partial active tail
rebuild indexes
rebuild Merkle trees
restore manifest from valid snapshot/log topology
refetch corrupt blob from trusted peer
refetch immutable segment/range from trusted peer when hashes/signatures prove identity
```

Any operation that could discard logical data requires explicit operator confirmation and produces a repair report.

---

# 63. Backup

Backup is local and self-contained.

Modes:

```text
consistent directory snapshot
portable archive
logical export
```

## 63.1 Portable archive

Use standard-library archive/compression code.

Archive contains:

- manifest,
- selected snapshot,
- required later segments,
- blob content according to policy,
- identity metadata excluding private peer key unless explicitly requested.

## 63.2 Online backup

Capture a consistent frontier without blocking ordinary writes for the whole copy duration.

Possible design:

1. choose immutable snapshot/segment frontier,
2. rotate active segment,
3. pin required immutable files,
4. copy/archive them,
5. release pins.

---

# 64. Restore

Restore into a new empty destination.

Validate:

- archive paths,
- checksums,
- database identity,
- duplicate entries,
- size limits.

Do not overwrite an existing database accidentally.

---

# 65. Export and Import

Logical export formats:

```text
JSON
NDJSON
canonical binary
```

JSON projection must preserve conflicts explicitly when requested.

Example:

```json
{
  "status": {
    "$conflict": [
      {"actor": "A", "value": "ready"},
      {"actor": "B", "value": "blocked"}
    ]
  }
}
```

A lossy projected export is clearly labeled.

---

# 66. At-Rest Encryption

Optional at-rest encryption may be added without inventing cryptographic primitives.

Use standard-library:

```text
AES
GCM
crypto/rand
HMAC/SHA where needed
```

Encryption design must specify:

- key generation,
- key IDs,
- nonce uniqueness,
- authenticated metadata,
- rotation,
- backup handling.

A raw randomly generated key file is the simplest high-assurance baseline.

Passphrase-derived keys require a separately documented KDF construction and strong interoperability/test vectors.

Never invent a cipher.

---

# 67. Transport Security

TLS provides:

- confidentiality,
- integrity,
- replay-resistant secure channel properties appropriate to TLS,
- authenticated peer transport after key pinning/verification.

Mesh0 protocol additionally verifies database identity and authorized peer identity.

Do not disable TLS verification for convenience.

Development-only insecure mode, if present, must be impossible to confuse with production defaults.

---

# 68. Secret Handling

Private peer keys:

- file mode restricted,
- never logged,
- never included in diagnostics,
- never transmitted,
- zeroed from temporary byte buffers where practical while recognizing Go GC limitations.

Invitations/temporary pairing secrets:

- high entropy,
- short-lived,
- single-use where possible.

---

# 69. Denial-of-Service Controls

Untrusted peer input is bounded.

Limits include:

```text
frame size
actors per summary
sparse ranges per actor
transaction operations
transaction bytes
object depth
string bytes
list insertion batch
map fields
snapshot bytes
pending dependencies
blob chunk bytes
parallel streams
signature verification rate
handshake time
idle time
```

Apply limits before allocation.

---

# 70. Wire Parser Hardening

Every decoder:

- uses checked integer conversions,
- rejects non-canonical varints where required,
- bounds lengths,
- bounds nesting,
- rejects impossible enum values,
- never trusts remote count before multiplication,
- never panics on malformed input.

Fuzz each frame decoder independently.

---

# 71. On-Disk Parser Hardening

Storage corruption may come from:

- abrupt crash,
- disk failure,
- malicious local modification.

All file readers are hostile-input parsers.

Never use `unsafe` casting of byte slices to structs for persistent format.

Decode explicitly with `encoding/binary`.

---

# 72. Query Isolation

A pathological query must not monopolize the database indefinitely.

Support:

```text
context cancellation
candidate scan limit
memory budget
execution deadline
result limit
```

Query planner emits warning before full scan of huge collection when interactive CLI is used.

Applications can allow full scans deliberately.

---

# 73. Subscription Backpressure

Subscription options:

```text
block producer
bounded queue + disconnect slow subscriber
coalesce by document/query
latest-state only
```

Never allow an unbounded channel per subscriber.

Callbacks are outside internal mutexes.

---

# 74. Concurrency Architecture

Core goroutines:

```text
storage writer
replication scheduler
one session per peer
subscription dispatcher
compaction worker
blob workers
optional index builder
```

Shared mutable state is minimized.

## 74.1 Single durable writer

One goroutine owns ordering of local/remote durable batch appends.

This simplifies:

- fsync grouping,
- actor sequence allocation,
- manifest updates,
- crash semantics.

Reads are concurrent.

## 74.2 Lock hierarchy

Document lock ordering must be explicit if fine-grained locks are used.

Prefer immutable state snapshots + atomic root publication where practical.

Never acquire network locks while holding storage locks.

---

# 75. In-Memory Publication

A commit constructs a new affected state and publishes atomically after durable storage according to mode.

Remote batch:

```text
validate
durably append
apply to private working state
update derived indexes
publish new roots
notify
```

If in-memory apply fails due to internal invariant after durable append, database enters fatal recovery-required state. Do not acknowledge a state that code cannot materialize.

---

# 76. File Descriptor Discipline

Long-running peer/database processes must bound open descriptors.

Use:

- segment handle cache,
- blob handle lifecycle,
- peer connection limits.

On close, all goroutines and FDs must terminate.

Tests repeatedly open/close database and inspect descriptor count.

---

# 77. Resource Accounting

Expose metrics through API/CLI without requiring a metrics framework.

Counters:

```text
documents
objects
operations retained
segments
snapshot bytes
blob bytes
index bytes
pending sync bytes
peer sessions
conflicts
WAL fsync latency
query scans
compaction reclaimed bytes
```

Optional `mesh0 serve --metrics` can expose a simple standard-library HTTP endpoint authored in-repository.

No external monitoring service is required.

---

# 78. Observability

Use `log/slog`.

Structured events:

```text
database.open
transaction.commit
transaction.remote_apply
sync.connect
sync.auth
sync.range_request
sync.converged
snapshot.create
compaction.start
compaction.finish
integrity.error
peer.reject
```

Never log:

- private keys,
- raw secret fields,
- blob contents,
- full document values by default.

---

# 79. Integrity Verification

Command:

```bash
mesh0 verify ./data
```

Checks:

```text
identity
manifest
segment framing
segment CRC
transaction atomicity
actor sequence rules
snapshot hashes
snapshot causal summaries
blob hashes optional/full
index correspondence optional
Merkle roots
logical convergence digest self-consistency
```

Modes:

```text
quick
full
paranoid
```

`paranoid` rebuilds derived state independently and compares.

---

# 80. Self-Test

`mesh0 selftest` uses internal temporary replicas.

Expected output concept:

```text
MESH0 SELF-TEST

[PASS] local durable commit
[PASS] crash-tail recovery
[PASS] duplicate operation idempotence
[PASS] reordered delivery convergence
[PASS] partitioned map conflict preservation
[PASS] observed-remove semantics
[PASS] concurrent sequence insertion
[PASS] transaction atomicity
[PASS] snapshot reload
[PASS] compaction equivalence
[PASS] peer authentication
[PASS] TLS sync
[PASS] blob hash verification
[PASS] offline edit and reconnect
[PASS] canonical state digest

CORE INVARIANTS PASSED
```

No optional external process is needed except self-reexec when crash testing.

---

# 81. Go API Surface

Conceptual public API:

```go
db, err := mesh0.Open(path, mesh0.Options{})
defer db.Close()

err = db.Update(ctx, func(tx *mesh0.Tx) error {
    doc := tx.Document("tasks", "42")
    doc.Set("title", "Ship")
    doc.Set("done", false)
    return nil
})

err = db.View(ctx, func(tx *mesh0.ReadTx) error {
    doc, ok := tx.Document("tasks", "42")
    ...
    return nil
})
```

Replication:

```go
peer, err := db.Connect(ctx, mesh0.PeerConfig{...})
```

Subscriptions:

```go
sub, err := db.Subscribe(query)
```

API uses `context.Context` for cancellation.

---

# 82. CLI Surface

Primary commands:

```text
mesh0 init
mesh0 put
mesh0 get
mesh0 delete
mesh0 query
mesh0 history
mesh0 conflicts
mesh0 resolve
mesh0 serve
mesh0 sync
mesh0 peer
mesh0 status
mesh0 verify
mesh0 compact
mesh0 backup
mesh0 restore
mesh0 export
mesh0 import
mesh0 branch
mesh0 selftest
mesh0 doctor
```

No third-party CLI framework.

Use `flag` plus small internal command routing.

---

# 83. Doctor

`mesh0 doctor` checks:

```text
filesystem sync behavior assumptions
atomic rename support
file locking
available free space
maximum open files
TLS support
supported architecture
clock diagnostics informational
network bind availability
directory permissions
```

Do not shell out to system commands.

---

# 84. Error Model

Typed error classes:

```text
invalid argument
not found
conflict present
authorization denied
peer untrusted
protocol incompatible
network unavailable
context canceled
durability failure
corruption
resource limit
schema violation
query error
internal invariant
```

Errors preserve actionable context without leaking secrets.

CLI exit classes are stable and documented.

---

# 85. Standard-Library Substitution Ledger

`STDLIB.md` must explain substitutions in detail.

| Common dependency/tool | Mesh0 implementation |
|---|---|
| SQLite/RocksDB/Badger/Bolt | append-only segments, snapshots, indexes, compaction |
| Automerge/Yjs/CRDT library | in-repository map/set/counter/sequence CRDT engine |
| protobuf/MessagePack | canonical binary codec using `encoding/binary` |
| gRPC | framed protocol over `net` + `crypto/tls` |
| UUID package | `crypto/rand` plus internal ID encoding |
| Merkle library | `crypto/sha256` plus internal range/state trees |
| CRC package | `hash/crc32` |
| compression library | `compress/gzip`, `compress/flate` |
| crypto/signature library | `crypto/ed25519` |
| TLS library | `crypto/tls` |
| x509 library | `crypto/x509` |
| networking framework | `net`, `net/http` where used |
| Cobra | `flag` plus internal command router |
| logging package | `log/slog` |
| query parser | handwritten lexer/parser |
| B-tree/index package | in-repository index engine |
| full-text library | in-repository inverted index/ranking |
| fsnotify package | platform syscall watcher only if needed, otherwise polling/event hooks |
| test framework | `testing` |
| fuzz framework | Go `testing` fuzz support |
| JSON library | `encoding/json` |
| archive library | `archive/tar` / `archive/zip` as selected |

Each entry includes:

- what is being replaced,
- the standard-library primitive used,
- what Browser/application behavior differs,
- performance/complexity trade-off,
- test coverage.

---

# 86. Dependency Proof

Generate:

```text
deps-proof.txt
```

At minimum:

```text
go list -m all
```

Repository test rejects any imported package outside:

- standard library,
- the root module.

Build uses:

```text
-mod=readonly
```

so accidental module additions fail.

---

# 87. Repository Structure

```text
mesh0/
├── README.md
├── ARCHITECTURE.md
├── CONSISTENCY.md
├── CRDT.md
├── STORAGE.md
├── SYNC.md
├── SECURITY.md
├── REPRODUCIBILITY.md
├── STDLIB.md
├── go.mod
├── deps-proof.txt
├── cmd/
│   └── mesh0/
│       └── main.go
├── internal/
│   ├── actor/
│   ├── blob/
│   ├── clock/
│   ├── codec/
│   ├── compact/
│   ├── crdt/
│   │   ├── counter/
│   │   ├── mapcrdt/
│   │   ├── sequence/
│   │   └── setcrdt/
│   ├── database/
│   ├── digest/
│   ├── index/
│   ├── manifest/
│   ├── query/
│   ├── replica/
│   ├── security/
│   ├── snapshot/
│   ├── storage/
│   │   ├── segment/
│   │   └── wal/
│   ├── subscription/
│   ├── sync/
│   │   ├── protocol/
│   │   ├── reconcile/
│   │   └── transport/
│   └── platform/
├── testdata/
└── tests/
    ├── adversarial/
    ├── convergence/
    ├── crash/
    ├── integration/
    ├── model/
    ├── protocol/
    └── reproducible/
```

Package boundaries follow logical trust/correctness boundaries.

---

# 88. Internal Dependency Rules

## CRDT layer

Must not import networking.

## Storage layer

Must not know application query syntax.

## Sync layer

Transfers validated canonical data but does not decide application conflict resolution.

## Query layer

Reads materialized/indexed state but does not mutate canonical history.

## Index layer

Derived and rebuildable.

## Blob layer

Content-addressed and independent of document mutation semantics.

## Security layer

Owns identity/signature/authorization primitives and must not depend on CLI.

These constraints prevent architecture collapse.

---

# 89. Fuzzing Strategy

Use Go standard fuzzing.

Mandatory targets:

```text
segment decoder
WAL frame decoder
snapshot decoder
manifest decoder
operation codec
transaction codec
causal clock codec
range-set codec
sync frame decoder
handshake decoder
invitation decoder
query lexer
query parser
blob manifest decoder
canonical ID decoder
index page decoder
logical export importer
```

CRDT fuzz targets:

```text
map operation application
set add/remove
counter deduplication
sequence insert/delete
conflict projection
snapshot merge
compaction transform
```

All malformed input must fail without panic or unbounded allocation.

---

# 90. Property Tests

Core mathematical properties:

## Idempotence

```text
apply(op)
apply(op)
==
apply(op)
```

## Commutativity for concurrent valid operations

```text
apply(A); apply(B)
==
apply(B); apply(A)
```

where A and B are causally independent and operation semantics permit reordering.

## Associativity of state merge

```text
merge(merge(A,B),C)
==
merge(A,merge(B,C))
```

## Convergence

Same valid operation set yields same canonical state.

## Compaction equivalence

Future merge behavior is unchanged by representation compaction.

## Snapshot equivalence

```text
replay(history)
==
load(snapshot_at_F) + replay(history_after_F)
```

## Replication idempotence

Repeated complete sync does not change state.

---

# 91. Adversarial Sync Tests

Test peers that:

- duplicate valid frames,
- reorder frames,
- send operation before dependency,
- send huge declared lengths,
- send invalid signature,
- reuse actor sequence with different bytes,
- send impossible causal dependency,
- send unauthorized collection write,
- send malformed sparse ranges,
- request nonexistent history repeatedly,
- stall mid-frame,
- rapidly connect/disconnect,
- advertise fake LAN identity,
- send valid TLS but wrong database identity.

The database must remain consistent.

---

# 92. Byzantine Scope

Mesh0 can detect many malicious peer behaviors through:

- signatures,
- actor sequence uniqueness,
- canonical encoding,
- authorization,
- causal validation,
- content hashes.

It does not claim full Byzantine consensus.

A malicious authorized writer may submit semantically harmful but structurally valid application data.

Application validation remains necessary.

---

# 93. Actor Fork Detection

Dangerous case:

```text
same ActorID
same Sequence
different operation bytes
```

This indicates:

- cloned replica identity used concurrently,
- corrupted history,
- malicious equivocation.

Treat as actor fork.

Default:

```text
halt acceptance for that actor
record both hashes
surface integrity error
require administrative resolution
```

Never choose one silently.

---

# 94. Replica Cloning

Copying a database directory to create another writable device must not reuse the same actor identity.

Provide:

```bash
mesh0 clone-replica source destination
```

or:

```bash
mesh0 actor rotate
```

A copied backup restored as a new writable replica generates a new actor identity unless explicitly restoring the same physical replica.

This prevents actor-sequence forks.

---

# 95. Causal Clock Scalability

A naive vector clock grows with every actor ever seen.

Mitigations:

- actors are per durable replica, not per transaction,
- retired actors can be summarized after safe administrative rules,
- actor tables use compact integer local indexes in physical storage,
- canonical wire encoding still preserves stable ActorID.

Do not invent unsafe clock truncation.

---

# 96. Large Collaboration Sets

For thousands of actors:

- exchange compressed actor tables,
- range summaries only for relevant database scope,
- avoid O(all actors) work per operation,
- cache causal dominance structures,
- checkpoint old retired actor information.

Performance work must preserve exact causality.

---

# 97. Selective Synchronization

A replica may choose to retain only selected collections/documents.

Selection policy:

```text
collection include
collection exclude
query-based profile
blob policy
```

Selective replicas must still receive causality metadata necessary to validate/merge subscribed data.

Do not pretend a partial replica has the same complete database root as a full replica.

Use scope-specific convergence digests.

---

# 98. Privacy of Partial Replication

A peer should not learn names/IDs of collections it is unauthorized to access through ordinary inventory exchange.

Handshake first establishes authorization scope.

Reconciliation then operates only on permitted namespaces.

Merkle hashes should be domain-separated by authorized scope to reduce metadata leakage.

---

# 99. Ephemeral Presence

Presence is not durable CRDT state.

Examples:

```text
cursor
typing
online status
selection
current viewport
```

Provide a separate best-effort ephemeral channel.

Presence messages:

- are not appended to canonical WAL,
- may be dropped,
- expire,
- do not affect convergence.

This keeps durable database history clean.

---

# 100. Derived Local State

Some state should remain local:

```text
window position
device preferences
recently opened documents
cached thumbnails
query indexes
sync scheduling hints
```

Mesh0 supports local-only collections that never replicate.

Their IDs live in a reserved local namespace.

---

# 101. Coordination-Aware Data Types

Certain invariants can be improved without full consensus using escrow/bounded-counter techniques.

Potential advanced type:

```text
BoundedCounter
```

Rights are partitioned among replicas.

A replica can decrement only within locally held rights.

Rights transfers require synchronization.

This allows some offline invariant preservation.

Such types must ship only after formalized invariants and model tests.

---

# 102. Migration and Format Evolution

Persistent and wire formats need evolution without product semantic-version labels.

Use:

```text
format generation integer
feature bits
optional tagged fields
```

Rules:

- new readers can detect old generation,
- migration is explicit and crash-safe,
- migration never rewrites the only copy in place,
- backups remain readable according to documented support window,
- unknown required features cause refusal, not misinterpretation.

`mesh0 doctor` reports format capabilities.

---

# 103. Migration Process

For a physical format migration:

1. open old data read-only,
2. verify integrity,
3. create new temporary physical representation,
4. copy/transform canonically,
5. verify logical digest equality,
6. fsync,
7. atomically switch manifest/root,
8. retain rollback material until successful reopen.

No destructive in-place mass rewrite.

---

# 104. Performance Targets

Targets are engineering goals, never unmeasured claims.

Optimize for:

- single-document read latency,
- local transaction latency,
- batched write throughput,
- sync catch-up throughput,
- memory per document/object,
- snapshot load time,
- compaction throughput,
- query index efficiency,
- blob dedup/sync throughput.

Publish results only with:

```text
machine
OS
filesystem
Go toolchain
durability mode
dataset
peer count
network characteristics
```

---

# 105. Benchmark Corpus

Include:

```text
100k small documents
1m map fields
long collaborative text
high-conflict scalar workload
sequence insert/delete workload
large set workload
counter workload
10GB blob corpus profile
many actors
long-offline catch-up
heavy query indexes
snapshot restore
compaction
```

Network tests:

```text
LAN low latency
100ms latency
1% loss simulated above reliable stream where applicable
bandwidth constrained
frequent disconnect
```

---

# 106. Efficient Operation Representation

Avoid storing full causal vector with every operation when batch/frontier compression can safely represent dependencies.

A transaction batch can carry:

```text
one dependency frontier
actor base sequence
operation count
compact object/key table
string dictionary profile
```

Operations inside batch use delta encoding.

Compression is an optimization only after canonical logical semantics are proven.

---

# 107. Memory-Efficient Sequences

Production sequence structure should avoid linked-node heap overhead.

Potential design:

```text
chunked immutable blocks
element-ID intervals
tombstone bitsets
child insertion indexes
order-statistics tree
```

Operations can locate logical index without scanning all tombstones.

Maintain reference implementation for correctness comparison.

---

# 108. State Digest Canonicalization

Canonical hash must not depend on:

- Go map iteration,
- memory address,
- segment filename,
- snapshot choice,
- compaction timing,
- peer arrival order,
- local actor table integer assignment.

Hash logical identities and canonical values only.

Conflict sets sorted by stable operation identity.

Floating-point values use canonical IEEE representation with explicit NaN policy.

---

# 109. Numeric Semantics

`Float64` requires deterministic handling:

- normalize negative zero policy,
- define NaN representation,
- reject signaling/ambiguous NaN payload variants or canonicalize,
- binary encode IEEE bits explicitly.

For exact financial values, applications should store:

```text
integer minor units
decimal string
structured decimal
```

rather than expecting binary floating-point exactness.

---

# 110. Timestamps

Timestamp value is application data.

Recommend:

```text
UTC nanoseconds since epoch + optional timezone metadata
```

Timestamp comparison is value comparison only.

It is not used as a universal conflict-resolution oracle.

---

# 111. Schema Validation

Optional schema file could use a small repository-owned grammar.

Example:

```text
collection tasks {
  title: string required max 4096
  done: bool required
  priority: int min 0 max 5
}
```

Parser is handwritten.

Remote operation that violates local schema:

- cannot simply disappear,
- can be quarantined at application projection layer,
- is reported explicitly.

Collaborative schema migration is an application-level protocol.

---

# 112. Hooks

Do not allow arbitrary shell hooks in the core database.

Application code using the Go API can react to subscriptions.

CLI hooks, if added, are clearly application features and not required for database correctness.

This keeps the storage engine independent from external executables.

---

# 113. Network API

Optional HTTP endpoint for local application access can be implemented using `net/http`.

Example:

```bash
mesh0 serve ./data --http 127.0.0.1:7341
```

Only bind loopback by default.

Remote HTTP requires explicit TLS/auth configuration.

No Web framework.

Protocol API can expose:

```text
GET document
POST transaction
GET query
GET changes stream profile
```

This is optional convenience; embedded Go API remains primary.

---

# 114. Streaming Query Results

Large queries return iterators/streams.

Do not materialize entire result set unless requested.

Pagination tokens:

- opaque,
- bound to query/index state,
- validated,
- never trust client-provided raw internal offsets blindly.

---

# 115. Reindexing

Index rebuild is online where possible.

Sequence:

1. choose stable read frontier,
2. build new index from snapshot,
3. track transaction deltas after frontier,
4. apply deltas,
5. atomically publish index generation.

Canonical data writes continue.

---

# 116. Compaction Scheduling

Compaction considers:

```text
obsolete bytes
segment count
snapshot age
disk free space
read amplification
active sync pins
backup pins
```

Compaction is cancellable.

Never delete a file still pinned by:

- active reader,
- backup,
- sync range transfer,
- snapshot builder.

---

# 117. Disk-Full Behavior

Before critical writes, attempt reasonable space checks but never trust free-space estimates as guarantees.

If append/fsync fails:

- transaction is not reported committed,
- database remains consistent,
- partial tail recovered later,
- error identifies disk condition.

Compaction must not delete the last valid source before replacement is durable.

---

# 118. File Permission Model

On creation:

```text
database directory: user private by default
identity/private keys: restrictive
segments/snapshots: user private by default
```

Respect explicit operator configuration carefully.

Warn if private key is group/world-readable.

---

# 119. Cross-Platform Scope

Primary reference target:

```text
Linux x86-64
Linux arm64
macOS arm64
Windows x86-64
```

Storage correctness may require platform-specific implementations for:

- file locking,
- directory sync behavior,
- atomic replacement,
- file permissions.

Platform differences must be isolated in `internal/platform`.

Do not claim a durability guarantee on a platform until crash tests pass there.

---

# 120. Documentation Requirements

## README.md

Must explain immediately:

- database works offline,
- every replica is writable,
- how conflicts work,
- how to run two peers,
- what guarantees are not provided.

## CONSISTENCY.md

Formal-ish definitions of:

```text
operation identity
causality
concurrency
atomic transaction
convergence
conflict
projection
```

## CRDT.md

Exact merge rules for each data type.

## STORAGE.md

File layout, WAL, fsync, snapshots, recovery, compaction.

## SYNC.md

Wire protocol, reconciliation, authorization, state transfer.

## SECURITY.md

Threat model, peer trust, signatures, TLS, resource limits.

## STDLIB.md

Complete substitution ledger.

## REPRODUCIBILITY.md

Binary and logical-state canonicalization.

---

# 121. Threat Model

Assume:

- network is hostile,
- unknown peer may connect,
- authorized peer may send malformed bytes,
- local disk may contain truncated/corrupt data,
- database contents are untrusted application data,
- blob contents may be malicious,
- system clock may be wrong,
- network messages may duplicate/reorder/delay,
- process may die at any storage instruction boundary.

Trust:

- Go runtime/toolchain,
- operating-system kernel/filesystem according to documented assumptions,
- cryptographic primitives in standard library,
- properly protected local private key.

Not protected against:

- kernel compromise,
- malicious authorized application logic,
- theft of already authorized plaintext from an authorized replica,
- physical compromise of unlocked machine.

---

# 122. Security Review Questions

Every feature must answer:

1. Which bytes are untrusted?
2. What maximum size is accepted?
3. Can a length overflow allocation arithmetic?
4. Is an identity authenticated?
5. Is authorization checked before durable acceptance?
6. Can replay apply an operation twice?
7. Can operation order change the result?
8. Can a malicious peer create an actor fork?
9. Can a peer make pending dependency state grow unbounded?
10. Can corruption be mistaken for a valid tail?
11. Does compaction remove future merge information?
12. Does a backup include private keys unexpectedly?
13. Can a partial replica infer unauthorized metadata?
14. Can clock skew change convergence?
15. Is an error fail-open or fail-closed?
16. What test proves the claim?

---

# 123. CRDT Review Questions

For every data type:

1. What is the immutable operation identity?
2. What is the causal context?
3. What does a concurrent operation mean?
4. Are effects idempotent?
5. Are concurrent effects commutative?
6. What metadata survives deletion?
7. When can metadata be compacted?
8. How is conflict exposed?
9. What deterministic projection is used?
10. Can an old offline replica still merge after compaction?
11. What model/property tests prove convergence?

---

# 124. Storage Review Questions

For every persistent change:

1. What bytes are written?
2. What checksum protects them?
3. When is success acknowledged?
4. What if process dies before write?
5. Mid-write?
6. After write before fsync?
7. After fsync before manifest update?
8. After rename before directory sync?
9. What is recovery behavior?
10. Is the only good copy ever overwritten in place?

---

# 125. Sync Review Questions

For every frame:

1. Maximum frame size?
2. Authentication state required?
3. Authorization scope?
4. Replay behavior?
5. Duplicate behavior?
6. Missing dependency behavior?
7. Backpressure?
8. Timeout?
9. Can malformed input panic?
10. Can logical state diverge while digest says synchronized?

---

# 126. Workstreams

Work should be dependency-ordered.

## Workstream A: identities and canonical codec

Deliver:

- IDs,
- varints,
- canonical binary codec,
- actor sequence,
- causal vector/ranges,
- fuzzing.

## Workstream B: reference CRDT engine

Deliver:

- map,
- set,
- counter,
- sequence,
- conflict model,
- deterministic projection,
- simple in-memory reference implementation.

## Workstream C: durable log

Deliver:

- segments,
- frames,
- checksums,
- atomic batch,
- fsync modes,
- recovery,
- crash injection.

## Workstream D: snapshots

Deliver:

- canonical state serialization,
- reload equivalence,
- state digests,
- atomic creation.

## Workstream E: production state engine

Deliver:

- memory-efficient objects,
- read transactions,
- write transactions,
- indexes,
- subscriptions.

Continuously compare against reference CRDT model.

## Workstream F: synchronization

Deliver:

- TLS transport,
- peer identity,
- handshake,
- actor summaries,
- missing-range sync,
- durable ack,
- convergence digest.

## Workstream G: advanced reconciliation

Deliver:

- range Merkle tree,
- snapshot/state transfer,
- long-offline peer catch-up.

## Workstream H: security

Deliver:

- pairing,
- signatures,
- authorization,
- peer rejection,
- resource limits,
- actor-fork detection.

## Workstream I: blobs

Deliver:

- SHA-256 blob store,
- chunking,
- dedup,
- lazy sync,
- repair.

## Workstream J: query engine

Deliver:

- parser,
- equality/ordered indexes,
- planner,
- subscriptions,
- explain output.

## Workstream K: compaction

Deliver:

- safe physical compaction,
- tombstone/causal summarization,
- equivalence tests,
- retention modes.

## Workstream L: tooling

Deliver:

- status,
- history,
- conflicts,
- verify,
- backup,
- restore,
- selftest,
- doctor.

## Workstream M: hardening

Deliver:

- fuzz corpus,
- deterministic network simulator,
- crash tests,
- corruption tests,
- exhaustive small-state explorer,
- cross-platform durability suite,
- reproducible build proof.

---

# 127. Definition of Done

Mesh0 is not complete because two maps happened to merge in a demo.

Release-quality requires all of the following.

## Dependency integrity

- one standard Go toolchain build,
- no third-party modules,
- no database helper executable,
- no sync helper executable,
- no external cloud/service requirement,
- CGO disabled,
- reproducible binary verified.

## Storage

- durable commit semantics documented and tested,
- partial-tail recovery tested,
- immutable-segment corruption detected,
- snapshot equivalence tested,
- manifest atomicity tested,
- disk-full behavior tested,
- compaction never destroys last valid source.

## CRDT

- map convergence property passes,
- set convergence property passes,
- counter idempotence passes,
- sequence convergence passes,
- delete/update concurrency tests pass,
- conflict preservation passes,
- deterministic projection passes,
- snapshot merge passes,
- compaction equivalence passes.

## Transactions

- local atomic visibility,
- remote batch atomicity,
- stable read views,
- causal dependency holdback,
- duplicate transaction idempotence.

## Sync

- offline edits converge,
- reordered frames converge,
- duplicate frames harmless,
- missing ranges recovered,
- long-offline peer catches up,
- snapshot transfer preserves unique local work,
- convergence digest matches.

## Security

- TLS active,
- peer identity pinned,
- invalid signatures rejected,
- unauthorized operations rejected before canonical acceptance,
- actor fork detected,
- malformed frames bounded,
- private keys protected,
- no insecure network default.

## Query

- indexes rebuild from canonical data,
- conflict indexing semantics tested,
- query plans explainable,
- subscriptions respect transaction boundaries.

## Blobs

- hashes verified,
- duplicate content deduplicated,
- lazy sync works,
- corrupt blob recoverable from peer when available.

## Quality

- `go test ./...` passes,
- `go test -race ./...` passes where compatible,
- `go vet ./...` passes,
- fuzz targets maintained,
- simulator failures reproducible by seed,
- no correctness claim lacks a test.

---

# 128. Demonstration Scenario: Two Offline Laptops

Initial shared document:

```text
project/launch

title = "Launch"
status = "draft"
members = {"alice"}
notes = "Start"
```

Disconnect both replicas.

Laptop A:

```text
status = "ready"
members add "bob"
notes insert " from design"
```

Laptop B:

```text
status = "blocked"
members add "carol"
notes insert " carefully"
```

Reconnect.

Expected:

```text
members:
  alice
  bob
  carol

notes:
  deterministic merged sequence containing both inserts

status:
  CONFLICT
    "ready"
    "blocked"
```

Both replicas produce identical canonical state and conflict set.

Then resolve status on one replica:

```text
status = "ready"
```

after observing both concurrent values.

Sync again.

Conflict disappears everywhere because the resolution causally supersedes both values.

---

# 129. Demonstration Scenario: No Server

Start three replicas.

```text
A <-> B <-> C
```

Synchronize.

Stop B completely.

Connect A directly to C.

Continue editing/synchronizing.

Expected:

- database remains fully functional,
- no "primary unavailable" state,
- no central server required,
- later B can return and catch up.

---

# 130. Demonstration Scenario: Crash During Commit

Start write loop.

Force process termination at random WAL fault point.

Restart.

Expected for each transaction:

```text
either fully committed
or fully absent
```

Never:

```text
half document update
half transaction
corrupt state silently accepted
```

Run thousands of seeded fault iterations.

---

# 131. Demonstration Scenario: Long Offline Replica

Replica C goes offline.

A/B produce a large history and compact.

C creates its own offline edits.

Months-equivalent simulation later, reconnect C.

Expected:

- C's unique edits preserved,
- A/B state preserved,
- synchronization may use state snapshot instead of replaying entire old history,
- all replicas converge.

This scenario is a release gate for compaction/state-transfer design.

---

# 132. Demonstration Scenario: Large Blob

Two replicas share document referencing a large file.

Replica B has an older mostly identical blob.

Sync uses chunk hashes.

Expected:

```text
metadata arrives first
only missing chunks transfer
final SHA-256 verifies
```

No entire-database retransmission.

---

# 133. Demonstration Scenario: Malicious Peer

Peer with valid network reach attempts:

```text
wrong database identity
bad signature
actor fork
unauthorized collection write
oversized frame
invalid varint
missing-dependency bomb
repeated duplicate batch
```

Expected:

- invalid state never enters canonical database,
- connection/session bounded,
- database stays available to honest peers.

---

# 134. Demonstration Scenario: Query During Sync

Replica receives thousands of remote transactions while application continuously queries:

```text
open tasks ordered by due
```

Expected:

- each read sees stable frontier,
- no partially applied remote transaction,
- index stays logically equivalent to canonical data,
- subscription updates at transaction boundaries.

---

# 135. Demonstration Scenario: Deterministic Replay

Capture operation history.

Create replicas by applying same transactions in many different legal orders.

Compute:

```text
canonical logical SHA-256
```

Expected all identical.

This is the most important public proof of convergence.

---

# 136. Product-Differentiating Capabilities

Mesh0 should be differentiated by more than "CRDT inside a file."

## 136.1 Every replica is durable and writable

Not an offline cache.

## 136.2 Conflicts are inspectable

No timestamp-based silent data loss.

## 136.3 Atomic causal transactions

Multi-document local actions arrive as coherent remote batches.

## 136.4 Long-offline synchronization

Efficient state/range reconciliation rather than requiring permanent central history.

## 136.5 Built-in content-addressed blobs

Documents and large attachments share one replication substrate.

## 136.6 Structural integrity proofs

Canonical state digests make divergence detectable.

## 136.7 Deterministic fault simulator

The database ships with tools to prove behavior under partitions/reorder/duplication.

## 136.8 No required server

A relay can help, but it is never the authority that makes data real.

## 136.9 No third-party runtime code

Storage, synchronization, CRDT logic, query, and crypto composition are inspectable in one codebase.

---

# 137. Critical Engineering Principles

### Never use wall-clock last-write-wins as the universal merge rule

Causality and conflict preservation are more honest.

### Never acknowledge durability before the selected durability boundary

A successful commit must mean what documentation says.

### Never treat a snapshot as merely current JSON

It must preserve merge metadata required for future offline reconciliation.

### Never garbage-collect causality metadata because peers "probably will not return"

Retirement/retention policy must be explicit.

### Never let arrival order decide logical state

Network scheduling is not application semantics.

### Never make a relay authoritative

Local replicas remain useful without it.

### Never silently discard unauthorized or schema-disliked remote state

Reject before canonical acceptance or surface quarantine explicitly.

### Never trust an actor sequence conflict

Same actor+sequence with different bytes is an integrity event.

### Never treat derived indexes as source of truth

Rebuild them.

### Never claim global invariants that partitioned local writes cannot provide

Document coordination requirements.

### Never make the user trust convergence by intuition

Compute canonical digests and test permutations.

---

# 138. Reference Implementation vs Production Engine

Maintain two CRDT implementations:

## Simple reference model

- straightforward maps/slices,
- maximally readable,
- intentionally slower,
- used by property/model tests.

## Production engine

- compact operation tables,
- chunked sequences,
- optimized indexes,
- incremental state.

For randomized histories:

```text
reference logical state
==
production logical state
```

This dramatically reduces risk while optimizing complex distributed data structures.

---

# 139. Formal Invariant Notebook

Repository includes a human-readable invariant document mapping each invariant to tests.

Example:

```text
INV-CRDT-001
Same operation set => same map conflict state

proof strategy:
  causal observed-assignment semantics

tests:
  TestMapPermutationConvergence
  FuzzMapConvergence
  ModelMapThreeActors
```

Categories:

```text
identity
causality
CRDT
transaction
durability
snapshot
compaction
sync
authorization
query
blob
```

No important invariant lives only in an engineer's head.

---

# 140. Technical References

The implementation team should study these sources as conceptual and standards references while writing all runtime code in-repository:

1. Local-first software: You own your data, in spite of the cloud  
   https://www.inkandswitch.com/essay/local-first/

2. Automerge documentation, core concepts and synchronization model  
   https://automerge.org/docs/hello/  
   https://automerge.org/docs/tutorial/concepts/

3. PushPin: Towards Production-Quality Peer-to-Peer Collaboration  
   https://www.inkandswitch.com/pushpin/

4. Conflict-Free Replicated Data Types research literature, including state-based and operation-based CRDT foundations.

5. Go standard library `os`  
   https://pkg.go.dev/os

6. Go standard library `io`  
   https://pkg.go.dev/io

7. Go standard library `bufio`  
   https://pkg.go.dev/bufio

8. Go standard library `encoding/binary`  
   https://pkg.go.dev/encoding/binary

9. Go standard library `hash/crc32`  
   https://pkg.go.dev/hash/crc32

10. Go standard library `crypto/sha256`  
    https://pkg.go.dev/crypto/sha256

11. Go standard library `crypto/ed25519`  
    https://pkg.go.dev/crypto/ed25519

12. Go standard library `crypto/tls`  
    https://pkg.go.dev/crypto/tls

13. Go standard library `crypto/x509`  
    https://pkg.go.dev/crypto/x509

14. Go standard library `net`  
    https://pkg.go.dev/net

15. Go standard library `net/http`  
    https://pkg.go.dev/net/http

16. Go standard library `compress/gzip`  
    https://pkg.go.dev/compress/gzip

17. Go standard library `testing`  
    https://pkg.go.dev/testing

The goal of reading existing systems is to understand the problem space, failure modes, and established distributed-systems theory, not to copy or vendor their runtime implementation.

---

# 141. Final Engineering Standard

Mesh0 should be judged internally by five questions.

> **Can this replica accept useful work with every network cable unplugged?**

If not, the system is not local-first.

> **If two replicas accept concurrent valid work and later exchange everything, do they converge without silently erasing intent?**

If not, the merge model is incomplete.

> **If power disappears at any storage instruction boundary, does the database recover to a state consistent with its acknowledgment contract?**

If not, durability is incomplete.

> **Can a long-offline authorized replica return after snapshots and compaction without destroying either side's unique work?**

If not, lifecycle semantics are incomplete.

> **Can the team prove those answers using deterministic tests, state digests, crash injection, and model comparison while the runtime module graph remains empty of third-party code?**

If not, the implementation is not finished.

The finished system should feel like a database built for the reality that networks fail, devices disappear, people work concurrently, servers are optional, and data deserves to remain usable under the user's control. Every important behavior should be explicit: causal history, merge semantics, durability, synchronization, conflicts, security, resource limits, and failure recovery.
