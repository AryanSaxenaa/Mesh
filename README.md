# Mesh0

**A local database that keeps working when the network does not, then merges
trusted devices without a central server.**

Imagine two field workers updating the same inspection checklist from different
laptops with no reliable connection. With a typical database, one device is
offline, waits for a server, or overwrites the other person's work later.
With Mesh0, each laptop writes to its own durable local database immediately.
When the laptops reconnect, they exchange changes directly. Independent edits
merge; competing edits are retained as visible conflicts instead of one being
silently discarded.

Mesh0 is an embedded, local-first document database for Go. It is a working
storage engine, not a synchronization demo: it includes a durable write-ahead
log, crash recovery, snapshots, backups, content-addressed blobs, queries,
and authenticated direct replication.

## Why it matters

Most applications depend on an always-reachable database server to decide what
is true. That breaks down for mobile teams, disaster response, remote sites,
peer-to-peer tools, and privacy-sensitive local software.

Mesh0 makes a different trade-off:

- **Write now.** A local write is fsync-durable before the command returns.
- **Work offline.** Reads and writes do not need a server or internet access.
- **Sync later.** Trusted replicas converge after reconnecting, with no
  primary database.
- **Do not hide collisions.** Concurrent scalar edits remain inspectable,
  rather than using last-write-wins based on unreliable wall-clock time.
- **Trust explicitly.** Replication uses TLS 1.3, pinned Ed25519 keys, and
  per-collection write grants.

## See it work in one minute

Build the standalone CLI, create a local database, write a document, and read
it back. No database server, package installation, or network connection is
needed.

```powershell
$env:CGO_ENABLED=0
go build -mod=readonly -trimpath -buildvcs=false -ldflags='-buildid=' -o mesh0.exe ./cmd/mesh0

./mesh0.exe init ./data
./mesh0.exe put ./data tasks/42 title='"Ship renderer"' done=false
./mesh0.exe get ./data tasks/42
./mesh0.exe status ./data
```

Then run `./mesh0.exe selftest` to verify a durable commit, restart recovery,
and canonical state verification in one command.

## Two devices, no central server

Each database has a persistent actor ID and transport key. Pair both replicas
out of band, permit the other replica to write only the collection it needs,
then sync directly:

```powershell
# On each device, inspect its database ID, actor ID, and public key.
./mesh0.exe status ./laptop-a
./mesh0.exe peer identity ./laptop-a

# On laptop-b, trust laptop-a's key and permit it to write tasks.
./mesh0.exe peer add ./laptop-b laptop-a <laptop-a-actor-id> <laptop-a-public-key-hex>
./mesh0.exe peer grant ./laptop-b <laptop-a-actor-id> tasks

# Make laptop-b reachable, then pull changes from laptop-a.
./mesh0.exe serve ./laptop-b --listen 0.0.0.0:7340
./mesh0.exe sync ./laptop-a 192.0.2.44:7340 <laptop-b-public-key-hex> laptop-b
```

If both devices change `tasks/42.title` while disconnected, Mesh0 retains both
values. Use `./mesh0.exe conflicts ./data` to inspect them and
`./mesh0.exe resolve ./data tasks/42 title='"Chosen title"'` to make an
intentional resolution.

## What Mesh0 is, and is not

Mesh0 is a strong fit when each device must keep its own usable copy of data
and eventual, conflict-aware convergence is preferable to blocking work.

It is **not** a replacement for a globally coordinated payment ledger,
inventory reservation system, or any workflow requiring one universal order,
global uniqueness, or arbitrary distributed business rules during a network
partition. Those still require explicit coordination.

## Under the hood

Mesh0 provides causal, conflict-preserving map registers, observed-remove sets,
additive counters, anchored list/text sequences, atomic transaction batches,
append-only WAL recovery, snapshots, verified backup/restore, and direct TLS
replication. Equality indexes are local, derived, and rebuilt after opening the
database.

For design and operational detail, read [CONSISTENCY.md](CONSISTENCY.md),
[CRDT.md](CRDT.md), [STORAGE.md](STORAGE.md), [SYNC.md](SYNC.md), and
[SECURITY.md](SECURITY.md).

## Zero-dependency receipt

Mesh0 enters Track D, Data & Storage. Its runtime module graph contains only
this module, as recorded in [deps-proof.txt](deps-proof.txt). The implementation
ledger is [STDLIB.md](STDLIB.md), and the byte-identical build receipt is
[REPRODUCIBILITY.md](REPRODUCIBILITY.md).
