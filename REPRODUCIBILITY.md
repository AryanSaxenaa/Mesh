# Reproducibility

Canonical build:

```powershell
$env:CGO_ENABLED=0
go build -mod=readonly -trimpath -buildvcs=false -ldflags='-buildid=' -o mesh0.exe ./cmd/mesh0
```

The repository has no third-party module requirements. Durable and wire formats use repository-owned canonical encoding: fixed identities, sorted version-vector actors, bounded length-prefixed values, and explicit IEEE-754 float encoding. Canonical logical digests never depend on Go map iteration, memory addresses, segment filenames, snapshot choices, compaction timing, or message arrival order.

To verify a reproducible build, build two clean working trees with the command above and compare the SHA-256 hashes of the executables.