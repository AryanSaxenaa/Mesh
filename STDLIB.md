# Standard-library substitution ledger

| Common dependency | Mesh0 implementation | Trade-off / verification |
|---|---|---|
| SQLite/Bolt/Badger | Append-only WAL segments, snapshots, and rebuildable scans | Smaller feature surface; recovery/snapshot tests |
| Automerge/Yjs/CRDT library | In-repository observed registers, OR-sets, counters | Fewer data types; permutation and conflict tests |
| Protobuf/MessagePack | Canonical varint codec using `encoding/binary` | Manual schema work; decoder fuzz target |
| gRPC | Bounded frames over `net` and `crypto/tls` | Direct peer protocol only; TLS sync tests |
| UUID package | `crypto/rand` 32-byte IDs | Longer IDs; actor/dot tests |
| Merkle/crypto package | `crypto/sha256`, `crypto/ed25519`, `crypto/tls` | Narrow profile; digest and pinning tests |
| CLI framework | `flag`-compatible router | Less generated help; command self-test |
| Blob store | SHA-256 content-addressed local files | Fixed single-file chunks initially; blob verification test |
| Archive package | `archive/zip` | ZIP only; checked backup/restore test |
| Structured logging package | `log` with explicit key/value fields in the CLI and database lifecycle paths | No log-level routing or pluggable sinks; standard error remains machine-separable from command output |

## Package Killer: `github.com/google/uuid`

Mesh0 deliberately does not import a UUID package for actor and operation
identity. `crypto/rand` creates 32-byte opaque identifiers, while the
repository's canonical codec serializes and validates them. The larger,
non-UUID identifier is intentional: it gives the protocol a uniform fixed-size
identity rather than a display-oriented UUID string, and avoids a runtime
dependency for a capability supplied by Go's standard library.

This is a scoped replacement, not a claim of API compatibility with
`github.com/google/uuid`: Mesh0 uses opaque random identifiers internally and
does not expose the UUID package's parser, formatter, or UUID versions.

Runtime imports are restricted to the Go standard library plus this module.
