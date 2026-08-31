# Security model

Network frames and persistent files are hostile input. Decoders use checked canonical varints and fixed resource limits; payloads are bounded before allocation. Segment frames use CRC32C for accidental corruption. Snapshots, identity files, and manifests carry hashes or checksums. Blob reads rehash content before returning it.

TLS 1.3 provides transport confidentiality and integrity. Mesh0 uses self-signed Ed25519 certificates plus exact public-key pinning, so `InsecureSkipVerify` is only used on the client to bypass public-Web-PKI hostname validation while a mandatory `VerifyPeerCertificate` callback checks the expected pinned Ed25519 key. This is not a trust-all mode.

A distinct batch with the same `(ActorID, Sequence)` is an actor fork and is rejected. Private identity files use user-only creation permissions. An authorized peer can still write semantically harmful application data; Mesh0 is not Byzantine consensus and applications remain responsible for domain validation.

## Peer pairing and revocation

`TrustAndBindPeerActor` durably journals its intent (name, actor, key) before writing either the actor binding or the TLS trust pin, and clears the journal only after both steps succeed. `Open` replays that journal: if the binding never became durable, the pairing is discarded; if it did, recovery finishes writing the trust pin. This makes a crash or a concurrent pairing call during that two-step sequence resolve deterministically rather than leaving an ambiguous half-trusted key. `UntrustPeer` removes a peer's TLS trust pin (idempotently) without touching its actor binding, and the CLI exposes this as `peer remove PATH PUBLIC_KEY`.

## Known, disclosed limitations

- **No automated recovery for a durably-trusted key whose actor binding was deliberately never granted.** If an administrator calls `TrustPeer` directly (bypassing `TrustAndBindPeerActor`) and never binds an actor, that key can pass TLS admission but every batch it sends is still rejected at `authorizeRemoteBatchLocked` before reaching the WAL. This is fail-closed by construction, not a gap, but it means `peer list` can show a trusted key with no write authority; use `peer remove` to clean it up.
- **`peer remove` and `RotateActor` are independent operations by design.** Removing trust for a key does not retire its actor binding, and rotating the local actor does not automatically untrust remote peers. An administrator who suspects a peer key is compromised should call `peer remove` for that key; if the concern is the *local* replica's own key, use `RotateActor` and re-pair remote replicas explicitly.