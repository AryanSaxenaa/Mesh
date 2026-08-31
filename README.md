# Mesh0

Mesh0 is a standard-library-only, embedded, local-first document database for Go. Every replica reads and writes its own durable state while offline. When trusted replicas reconnect, they exchange causally identified transaction batches and converge without a primary server.

## Quick start

```powershell
$env:CGO_ENABLED=0
go build -mod=readonly -trimpath -buildvcs=false -ldflags='-buildid=' -o mesh0.exe ./cmd/mesh0
./mesh0.exe init ./data
./mesh0.exe put ./data tasks/42 title='"Ship renderer"' done=false
./mesh0.exe get ./data tasks/42
./mesh0.exe status ./data
```

A local transaction is fsync-durable by default. Reads do not require a network connection. Concurrent assignments are retained as inspectable conflicts, rather than resolved by wall-clock last-write-wins.

## Two peers

Each replica has a persistent Ed25519 transport identity and a distinct actor ID
(`mesh0 status PATH`). Exchange both values out of band, then explicitly bind
that actor to the peer key:

```powershell
./mesh0.exe peer add ./laptop-b laptop-a <laptop-a-actor-id> <laptop-a-public-key-hex>
./mesh0.exe peer grant ./laptop-b <laptop-a-actor-id> tasks
./mesh0.exe serve ./laptop-b --listen 0.0.0.0:7340
./mesh0.exe sync ./laptop-a 192.0.2.44:7340 <laptop-b-public-key-hex> laptop-b
```

TLS 1.3 protects the TCP stream; a peer key is pinned and authorized to author
batches for exactly one actor before any Mesh0 frame can become durable. Actor
bindings have no implicit data-write authority: grant each collection explicitly
with `peer grant`, and use `peer revoke` to deny future remote writes. Revoking
a grant cannot erase data previously disclosed to that peer. Both peers remain
usable if the network or the serving peer disappears.

## Queries and local indexes

`DB.Query` provides bounded, conflict-aware local document queries. For repeated scalar equality queries, declare a process-local derived index with `EnsureEqualityIndex(context, mesh0.EqualityIndex{Collection: "tasks", Path: "status"})`. Indexes are rebuilt from canonical documents on every accepted local or remote transaction, are not replicated or stored in WALs/snapshots/backups, and must be re-declared after reopening a database. A concurrent register value is indexed in every matching equality bucket; `DB.ExplainQuery` reports whether a query uses an equality index or the canonical full-scan fallback.

## Guarantees and boundaries

Mesh0 currently supplies causal, conflict-preserving map registers, observed-remove sets, additive counters, anchored list/text sequences, atomic transaction batches, append-only WAL recovery, snapshots, content-addressed blobs, verified backup/restore, and direct TLS replication. It does **not** provide global serializability, global uniqueness, a global leader, or offline enforcement of arbitrary distributed business invariants. Applications requiring those properties need explicit coordination.

See [CONSISTENCY.md](CONSISTENCY.md), [CRDT.md](CRDT.md), [STORAGE.md](STORAGE.md), [SYNC.md](SYNC.md), and [SECURITY.md](SECURITY.md).

## Zero-dependency receipt

Mesh0 enters Track D, Data & Storage. Its runtime module graph contains only
this module, as recorded in [deps-proof.txt](deps-proof.txt). The implementation
ledger is [STDLIB.md](STDLIB.md), and the byte-identical build receipt is
[REPRODUCIBILITY.md](REPRODUCIBILITY.md).
