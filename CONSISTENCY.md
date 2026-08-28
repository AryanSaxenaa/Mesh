# Consistency model

Mesh0 provides causal, convergent local-first consistency. An operation has immutable identity `(ActorID, Sequence)`. A transaction is a contiguous sequence of dots with one dependency version vector. A batch is visible only after its dependencies and every operation in the batch are present.

Two operations are concurrent when neither dependency vector covers the other's dot. Concurrent map assignments coexist. The regular `Value` read projects the greatest dot deterministically for convenience, while `Values` exposes every concurrent assignment. Projection is never used to delete the conflict metadata.

If replicas admit the same valid batch set, their canonical logical digest is equal regardless of transfer order or duplicate delivery. The digest covers database identity, frontier, and canonical retained batches—not segment names, arrival order, or snapshot timing.

The model intentionally does not offer global total order, globally exclusive compare-and-set, globally unique names, or arbitrary cross-replica invariants during a network partition.