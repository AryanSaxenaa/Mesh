# Invariant notebook

| ID | Invariant | Evidence |
|---|---|---|
| INV-ID-001 | One dot identifies one immutable operation | `TestActorForkIsRejected` |
| INV-CRDT-001 | Same concurrent map operation set preserves the same conflict set | `TestObservedAssignmentConvergesAndResolves` |
| INV-CRDT-002 | Removing one observed set member does not remove another | `TestObservedRemovePreservesOtherMembers` |
| INV-CRDT-003 | Arrival order cannot change canonical state digest | `TestConcurrentPermutationDigest` |
| INV-TXN-001 | A batch is materialized only after causal dependencies | `state.apply`, recovery/sync holdback |
| INV-STORAGE-001 | A snapshot/restart preserves logical state | `TestSnapshotRestartAndBlobVerification` |
| INV-BLOB-001 | Blob bytes are SHA-256 verified before use | `TestSnapshotRestartAndBlobVerification` |
| INV-SYNC-001 | Direct trusted peers converge after concurrent offline edits | `TestObservedAssignmentConvergesAndResolves` |
| INV-PARSER-001 | Malformed batch input does not panic | `FuzzBatchDecoderNeverPanics` |

Generation one uses full history retention. No causal metadata is garbage-collected, so long-offline replicas can merge retained operation history without unsafe clock truncation.