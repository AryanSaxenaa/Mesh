# Security model

Network frames and persistent files are hostile input. Decoders use checked canonical varints and fixed resource limits; payloads are bounded before allocation. Segment frames use CRC32C for accidental corruption. Snapshots, identity files, and manifests carry hashes or checksums. Blob reads rehash content before returning it.

TLS 1.3 provides transport confidentiality and integrity. Mesh0 uses self-signed Ed25519 certificates plus exact public-key pinning, so `InsecureSkipVerify` is only used on the client to bypass public-Web-PKI hostname validation while a mandatory `VerifyPeerCertificate` callback checks the expected pinned Ed25519 key. This is not a trust-all mode.

A distinct batch with the same `(ActorID, Sequence)` is an actor fork and is rejected. Private identity files use user-only creation permissions. An authorized peer can still write semantically harmful application data; Mesh0 is not Byzantine consensus and applications remain responsible for domain validation.