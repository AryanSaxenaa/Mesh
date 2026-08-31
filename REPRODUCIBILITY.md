# Reproducibility

Canonical build:

```powershell
$env:CGO_ENABLED=0
go build -mod=readonly -trimpath -buildvcs=false -ldflags='-buildid=' -o mesh0.exe ./cmd/mesh0
```

The repository has no third-party module requirements. Durable and wire formats use repository-owned canonical encoding: fixed identities, sorted version-vector actors, bounded length-prefixed values, and explicit IEEE-754 float encoding. Canonical logical digests never depend on Go map iteration, memory addresses, segment filenames, snapshot choices, compaction timing, or message arrival order.

## Verified receipt

On Windows/amd64, two builds from this revision using the command above
produced byte-identical executables. This was verified independently on two
Go toolchain versions, which also confirms the build is stable across
toolchains rather than pinned to one compiler's output:

```
Go 1.22, Windows/amd64:
82A4D61418B13FAA31464A687016B906D053FB832493BE9D15A7898C824874A5  mesh0.exe (build A)
82A4D61418B13FAA31464A687016B906D053FB832493BE9D15A7898C824874A5  mesh0.exe (build B)

Go 1.27.0, Windows/amd64:
82A4D61418B13FAA31464A687016B906D053FB832493BE9D15A7898C824874A5  mesh0.exe (build A)
82A4D61418B13FAA31464A687016B906D053FB832493BE9D15A7898C824874A5  mesh0.exe (build B)
```

Both toolchains produced the identical hash. CI (see `.github/workflows/ci.yml`)
re-runs this exact two-build comparison on every push and fails the build if
the hashes ever diverge, so this receipt does not go stale silently.

Reproduce the comparison in two clean output directories (or run the same
commands from two clean working trees):

```powershell
$env:CGO_ENABLED=0
go build -mod=readonly -trimpath -buildvcs=false -ldflags='-buildid=' -o .\build-a\mesh0.exe ./cmd/mesh0
go build -mod=readonly -trimpath -buildvcs=false -ldflags='-buildid=' -o .\build-b\mesh0.exe ./cmd/mesh0
Get-FileHash .\build-a\mesh0.exe -Algorithm SHA256
Get-FileHash .\build-b\mesh0.exe -Algorithm SHA256
```

The hashes must match. The two target directories are intentionally separate so
the comparison validates the artifact, not a reused executable.
