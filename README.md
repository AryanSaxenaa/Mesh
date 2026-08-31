# Mesh0

[![ci](https://github.com/AryanSaxenaa/Mesh/actions/workflows/ci.yml/badge.svg)](https://github.com/AryanSaxenaa/Mesh/actions/workflows/ci.yml)
[![license: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![zero third-party deps](https://img.shields.io/badge/dependencies-zero-brightgreen.svg)](deps-proof.txt)

**Mesh0 is an embedded, local-first document database for Go.** Every device
can write durable data while offline, then synchronize directly with trusted
peers when a network route becomes available.

Imagine two field workers updating the same inspection checklist from laptops
with unreliable connectivity. Instead of waiting for a server, each laptop
writes to its own local database immediately. When the laptops reconnect,
Mesh0 exchanges their missing changes directly. Independent edits merge;
competing edits remain visible conflicts instead of being silently overwritten.

It is a working storage engine, not a synchronization mock-up: durable
write-ahead logging, crash recovery, snapshots, backups, local queries,
content-addressed blobs, and authenticated peer-to-peer replication are all
included.

## Peer-to-peer by design

Mesh0 has no central server, primary replica, or cloud account. A trusted peer
starts `serve`; another trusted peer runs `sync`. The connection uses TLS 1.3
with pinned Ed25519 public keys, and each peer receives explicit write grants
for only the collections it is allowed to modify.

## Watch the walkthrough

[Watch the Mesh0 walkthrough on YouTube](https://www.youtube.com/watch?v=YOUR_VIDEO_ID)

> Replace `YOUR_VIDEO_ID` with the published YouTube video ID.

## Try peer-to-peer sync on one laptop

No second laptop is required to demonstrate peer synchronization. Create two
replica folders, make an offline update in one, then use `serve` and `sync` to
prove the update appears in the other. The complete operator flow is below in
[Two devices, no central server](#two-devices-no-central-server), and the
project story is in [DEMO.md](DEMO.md).

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

## What you can do today

| Need | Mesh0 command |
| --- | --- |
| Store or update a document | `mesh0 put PATH COLLECTION/ID field=value` |
| Find matching documents | `mesh0 query PATH COLLECTION --where field=value` |
| Search a text prefix | `mesh0 query PATH COLLECTION --prefix field=text` |
| Audit a record | `mesh0 history PATH COLLECTION/ID` |
| Verify and preserve data | `mesh0 verify PATH`, `snapshot`, `backup`, `restore` |
| Work across trusted devices | `replica create`, `peer add`, `peer grant`, `serve`, `sync` |

Mesh0's CLI is intentionally small, but it operates a full embedded storage
engine: writes are durable, queries run locally, and replication reconciles
authorized history directly between peers.

## Hackathon context

Mesh0 was built for the Zero Dependency Hackathon, Track D: Data & Storage.
The project deliberately uses Go's standard library only, with the runtime
module graph recorded in [deps-proof.txt](deps-proof.txt).

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

## CLI exit codes and errors

Every command writes its result to stdout on success and returns exit code
`0`. On failure, the CLI writes a single `mesh0: <message>` line to stderr and
returns a non-zero exit code: `2` for a malformed invocation (missing
arguments, bad flags, invalid hex/IDs - anything matching `mesh0.ErrInvalidArgument`),
and `1` for every other failure (not found, corruption, authorization denied,
I/O errors, and so on). A non-zero exit with a `mesh0:`-prefixed stderr line is
expected, documented behavior, not a bug in the demo.

## Two devices, no central server

Sync is direct, authenticated, and opt-in: Mesh0 does not discover devices or
send data to a cloud service. Replicas must share the same **database ID**, so
do not run `init` independently on both devices. Create the second replica
from the first one instead. The `replica create` command makes a portable copy
with a new actor ID and transport key:

```powershell
# This can be done on one machine for a demo, or the resulting laptop-b folder
# can be copied once to the second device by a trusted transfer method.
./mesh0.exe init ./laptop-a
./mesh0.exe put ./laptop-a tasks/42 title='"Initial task"' done=false
./mesh0.exe replica create ./laptop-a ./laptop-b

# On each device, record its actor ID and public key out of band.
./mesh0.exe status ./laptop-a
./mesh0.exe peer identity ./laptop-a
./mesh0.exe status ./laptop-b
./mesh0.exe peer identity ./laptop-b

# Pair in both directions. Replace each placeholder with the value recorded
# from the other device. Grant only collections that peer may write.
./mesh0.exe peer add ./laptop-a laptop-b <laptop-b-actor-id> <laptop-b-public-key-hex>
./mesh0.exe peer grant ./laptop-a <laptop-b-actor-id> tasks
./mesh0.exe peer add ./laptop-b laptop-a <laptop-a-actor-id> <laptop-a-public-key-hex>
./mesh0.exe peer grant ./laptop-b <laptop-a-actor-id> tasks

# Make laptop-b reachable on the LAN/VPN, then sync from laptop-a. The public
# key pins the TLS connection to laptop-b; no certificate authority is needed.
./mesh0.exe serve ./laptop-b --listen 0.0.0.0:7340
./mesh0.exe sync ./laptop-a 192.0.2.44:7340 <laptop-b-public-key-hex> laptop-b
```

For two physical devices, an alternative is `backup` on laptop-a, copy the
archive to laptop-b by a trusted method, then run `restore` and
`replica rotate ./laptop-b` on laptop-b before pairing. Once paired, either
device can initiate `sync` whenever the other is reachable; a sync exchanges
both directions' permitted changes in one connection.

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

For design and operational detail, read [ARCHITECTURE.md](ARCHITECTURE.md),
[CONSISTENCY.md](CONSISTENCY.md), [CRDT.md](CRDT.md), [STORAGE.md](STORAGE.md),
[SYNC.md](SYNC.md), [SECURITY.md](SECURITY.md), and [INVARIANTS.md](INVARIANTS.md)
(every documented invariant mapped to the test that proves it). The full
current-scope contract, including what is deliberately out of scope, is
[Mesh0_Detailed_Build_Spec.md](Mesh0_Detailed_Build_Spec.md).

The [semantic-review/](semantic-review/) directory is our own internal review
history for the sync/security surface, kept in the repo rather than deleted.
Every `NEEDS_CHANGES` verdict in there identifies a real, confirmed finding
against an earlier revision, and every finding was fixed in a subsequent
commit with regression coverage; none describe the current `main`. We publish
them because "we found this ourselves and closed it" is a more convincing
signal than an empty history, not because anything in them is still open.

## Zero-dependency receipt

Its runtime module graph contains only this module, as recorded in
[deps-proof.txt](deps-proof.txt). The implementation ledger is
[STDLIB.md](STDLIB.md), and the byte-identical build receipt is
[REPRODUCIBILITY.md](REPRODUCIBILITY.md).

## Verify it yourself

Every claim above is checkable in under a minute, and the same commands run
in CI on every push (see the badge at the top):

```powershell
go list -m all                # -> github.com/mesh0/mesh0 (nothing else)
go vet ./...                  # -> no diagnostics
go test ./... -v              # -> full suite, including fuzz targets
```

For the byte-identical reproducible build check, see
[REPRODUCIBILITY.md](REPRODUCIBILITY.md).

## License

[MIT](LICENSE).
