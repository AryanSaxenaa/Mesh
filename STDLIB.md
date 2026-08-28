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

Runtime imports are restricted to the Go standard library plus this module.