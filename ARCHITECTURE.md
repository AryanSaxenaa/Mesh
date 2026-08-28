# Architecture

`mesh0` is the public embedded API. It owns typed values, immutable operations, CRDT state, storage, snapshotting, blob storage, backup, and peer synchronization. `cmd/mesh0` is a thin `flag`-compatible command router.

The dependency direction is intentional: codec/identity/clock primitives are leaves; CRDT state depends only on those primitives; storage serializes canonical batches but does not parse queries; query and export only read published immutable state; sync sends validated batches through `DB.ApplyRemote`; and the CLI depends only on the public package. No storage, CRDT, or query code imports networking.

A `DB` has one mutex-protected durable writer path. `Update` first constructs and validates a private next state, appends a checksummed WAL batch, crosses the configured durability boundary, persists actor metadata, atomically publishes the next state root, then notifies bounded subscribers. `View` snapshots the current immutable root and therefore never sees a partial transaction.