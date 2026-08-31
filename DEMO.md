# Mesh0 demo guide

## The story in 90 seconds

Mesh0 demonstrates a local-first database, not a remote server with an
offline cache. Each device has a durable local database and remains usable
without a network. When two explicitly trusted replicas reconnect, they
exchange only the history the other replica is missing. Concurrent scalar
edits are not silently overwritten: they are retained as conflicts for an
application or operator to resolve deliberately.

The one-laptop demo uses two folders as two logical devices. That exercises
the same database identities, actor identities, TLS handshake, key pinning,
authorization checks, and network protocol used between computers.

Use the commands in the README's [two-device guide](README.md#two-devices-no-central-server)
to create and pair replicas. The workflow leaves both replica folders available
for inspection.

Expected ending:

```text
MESH0 PEER-SYNC DEMO PASSED
Device A received device B's offline update over authenticated localhost TLS.
```

Show the synced document from device A:

```powershell
.\mesh0.exe get .\video-demo\device-a tasks/2
```

## What to point out during a demo

1. Device A begins with a durable document.
2. `replica create` provisions device B with the same database history but a
   different actor ID and transport key. Independent `init` commands are not
   replicas and are intentionally refused by sync.
3. Device B writes `tasks/2` while device A is disconnected.
4. Each device explicitly pins the other's Ed25519 public key and grants it
   write permission for `tasks` only.
5. A single `sync` connection transfers B's missing transaction to A. There
   is no cloud account, central database, or last-write-wins overwrite.

## Two physical computers

The desktop CLI works wherever Go builds the project. Create a replica once,
then transfer it to the second computer through a trusted channel such as an
encrypted USB drive, a private file share, or an administrator-controlled
deployment package:

```powershell
# On device A
.\mesh0.exe replica create .\laptop-a .\laptop-b
```

Copy the resulting `laptop-b` directory to device B. On both devices, obtain
the actor ID (`status`) and public key (`peer identity`), then run `peer add`
and `peer grant` in both directions as shown in the README.

To connect, device B starts a listener:

```powershell
.\mesh0.exe serve .\laptop-b --listen 0.0.0.0:7340
```

Device A syncs to B's reachable LAN or VPN address:

```powershell
.\mesh0.exe sync .\laptop-a <device-b-ip>:7340 <device-b-public-key> device-b
```

The public key is the trust anchor for TLS; the address only tells Mesh0 where
to connect. The port must be reachable through the operating-system firewall
and network. Mesh0 does not perform device discovery, NAT traversal, or
background cloud synchronization: a replica initiates `sync` when it has a
route to its trusted peer.

## Trust boundary

Pairing and connection are separate on purpose:

- **Pinned key:** proves the connected peer is the one approved out of band.
- **Actor binding:** proves which replica may author received operations.
- **Collection grant:** limits the collections that peer may write.
- **Database ID check:** prevents accidentally merging unrelated databases.

This makes the demo honest about what Mesh0 provides: durable, direct,
permissioned synchronization for a known group of replicas—not an automatic
consumer cloud-sync service.
