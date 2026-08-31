# Mesh0 Current Build Specification

This document describes the functionality that ships in this repository. It is
not a roadmap or a list of planned features.

## Product contract

Mesh0 is an embedded, local-first document database for Go. A replica can read
and write its own durable data while offline. Trusted replicas may later connect
directly and exchange accepted transaction batches. If replicas accept the same
valid batch set, they converge to the same canonical logical state.

Every replica is a real database, not a cache. There is no required cloud
service, database server, broker, relay, or third-party runtime dependency.

## Shipped capabilities

- Document collections with typed scalar values, map registers, observed-remove
  sets, additive counters, anchored lists, and text sequences.
- Conflict-preserving concurrent scalar writes. Conflicts can be listed and
  deliberately resolved through the CLI.
- Atomic local transaction batches and causal dependency holdback for remote
  batches.
- Append-only WAL storage, crash recovery, snapshots, verification, backup, and
  restore.
- SHA-256 content-addressed local blobs with verification on read.
- Local, derived equality indexes and bounded document queries.
- Direct TCP synchronization protected by TLS 1.3 and pinned Ed25519 peer keys.
- Durable actor-to-key bindings and explicit per-collection remote write grants.
- A standalone CLI for administration, inspection, backup, verification,
  serving, synchronization, and self-test.

## Direct synchronization model

Mesh0 synchronizes directly connected trusted peers. A peer must be paired with
an explicit public-key pin and actor binding before it can send transaction
batches. It must also receive an explicit write grant for every collection it
changes.

The current protocol exchanges bounded ranges only for the directly connected
actor. It paginates transaction batches and ends with a canonical state-digest
comparison. A mismatch at equal frontiers is an invariant failure.

Mesh0 does not currently relay another actor's history, install mergeable state
snapshots, or provide automatic peer discovery. Those are deliberately outside
this build's contract.

## Consistency boundaries

Mesh0 provides causal, convergent local-first consistency. It does not provide
global serializability, global uniqueness, a global leader, or offline
enforcement of arbitrary distributed business invariants. Applications that
need globally exclusive reservations, payment settlement, or a single universal
order need explicit coordination.

## Durability contract

The CLI opens databases with `DurabilitySync`: a successful local write is
fsync-durable by default. On open, Mesh0 validates identity and manifest data,
loads a verified snapshot, and replays retained WAL segments. A malformed
trailing frame in the active segment may be truncated; corruption in immutable
segments fails open rather than being silently ignored.

## Zero-dependency contract

The runtime module graph contains only `github.com/mesh0/mesh0`. Canonical
builds use `CGO_ENABLED=0` and `-mod=readonly` and are verified byte-identical
across Go 1.22 and Go 1.27; CI re-runs this check on every push. See
[STDLIB.md](STDLIB.md), [deps-proof.txt](deps-proof.txt), and
[REPRODUCIBILITY.md](REPRODUCIBILITY.md) for the verifiable receipt.

## Authoritative detail

- [README.md](README.md): product story and runnable CLI introduction.
- [ARCHITECTURE.md](ARCHITECTURE.md): package and writer-path structure.
- [CONSISTENCY.md](CONSISTENCY.md): convergence and conflict semantics.
- [CRDT.md](CRDT.md): data-type merge semantics.
- [STORAGE.md](STORAGE.md): persistence and recovery behavior.
- [SYNC.md](SYNC.md): direct-peer protocol and authorization boundaries.
- [SECURITY.md](SECURITY.md): threat model and transport profile.
- [INVARIANTS.md](INVARIANTS.md): invariants mapped to tests.
