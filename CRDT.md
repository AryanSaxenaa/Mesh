# CRDT semantics

## Map registers
A `MapAssign` records one dot/value. It removes only assignments in its dependency vector. Therefore assignments made concurrently survive and form an explicit conflict set. `MapDelete` uses the same observed-only rule.

## Sets
A set add is identified by its operation dot. A remove carries the element value plus the transaction's observed frontier; it removes only matching element adds visible to that frontier. A concurrent add of the same element survives. This is an observed-remove set.

## Counters
A counter is the sum of deduplicated `CounterAdd(dot, delta)` operations. Counter overflow is rejected; it never wraps.

## Documents
Document deletion causally removes fields and set adds that it observed. Concurrent unseen updates survive, favoring data preservation. Every rule is independent of wall-clock time and network arrival order.


## Anchored lists and text
A list/text container has an immutable `ObjectID`, the dot of its `MakeList` operation; a document field stores a stable list or text reference rather than using a mutable path as identity. Each `ListInsert` creates one or more `ElementID`s from its operation dot and local offset. The first element follows the specified observed anchor, and later values in the same operation form a chain, preserving local run order.

Concurrent children of the same anchor are sorted solely by `ElementID`, never arrival order or timestamps. `ListDelete` tombstones exact observed element IDs. Tombstoned elements remain structural anchors, so a concurrent or later causal child survives even when its predecessor is hidden. Replacement is delete plus insertion after the old element. Text uses the same sequence semantics with validated UTF-8 Unicode code-point elements; its initial implementation favors correctness and canonical replay over chunk-level memory optimization.
