package mesh0

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func signedTestBatch(t *testing.T, actor ActorID) Batch {
	t.Helper()
	return Batch{First: Dot{Actor: actor, Seq: 1}, Count: 1, Dependencies: VersionVector{}, Operations: []Operation{{Dot: Dot{Actor: actor, Seq: 1}, Document: DocumentKey{"secure", "one"}, Path: []string{"value"}, Action: MapAssign, Value: String("accepted")}}}
}

func signedCollectionBatch(t *testing.T, actor ActorID) Batch {
	t.Helper()
	return Batch{
		First:        Dot{Actor: actor, Seq: 1},
		Count:        2,
		Dependencies: VersionVector{},
		Operations: []Operation{
			{Dot: Dot{Actor: actor, Seq: 1}, Document: DocumentKey{"allowed", "one"}, Path: []string{"value"}, Action: MapAssign, Value: String("permitted")},
			{Dot: Dot{Actor: actor, Seq: 2}, Document: DocumentKey{"forbidden", "one"}, Path: []string{"value"}, Action: MapAssign, Value: String("denied")},
		},
	}
}

func TestSignedBatchRejectsTamperingAndWrongDatabase(t *testing.T) {
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	actorID, err := newID()
	if err != nil {
		t.Fatal(err)
	}
	databaseID, err := newID()
	if err != nil {
		t.Fatal(err)
	}
	identity := peerIdentity{private: private, public: public}
	payload, err := signBatch(identity, DatabaseID(databaseID), signedTestBatch(t, ActorID(actorID)))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := decodeSignedBatch(payload, public, DatabaseID(databaseID)); err != nil {
		t.Fatalf("valid signed batch rejected: %v", err)
	}
	wrongDatabase, err := newID()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := decodeSignedBatch(payload, public, DatabaseID(wrongDatabase)); !errors.Is(err, ErrAuthorizationDenied) {
		t.Fatalf("wrong database error = %v, want authorization denial", err)
	}
	tampered := append([]byte(nil), payload...)
	tampered[len(tampered)-1] ^= 0x80
	if _, err := decodeSignedBatch(tampered, public, DatabaseID(databaseID)); !errors.Is(err, ErrInvalidSignature) {
		t.Fatalf("tampered signature error = %v, want invalid signature", err)
	}
}

func TestRemoteAdmissionRequiresDurableActorKeyBinding(t *testing.T) {
	db := newTestDB(t)
	remoteID, err := newID()
	if err != nil {
		t.Fatal(err)
	}
	remoteActor := ActorID(remoteID)
	remoteKey, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	batch := signedTestBatch(t, remoteActor)
	before, err := db.Status()
	if err != nil {
		t.Fatal(err)
	}
	segment := segmentPath(db.path, db.manifest.Active)
	info, err := os.Stat(segment)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.applyRemoteFromPeer(context.Background(), batch, remoteActor, remoteKey); !errors.Is(err, ErrAuthorizationDenied) {
		t.Fatalf("unbound actor error = %v, want authorization denial", err)
	}
	if err := db.TrustPeer("remote", remoteKey); err != nil {
		t.Fatal(err)
	}
	if err := db.applyRemoteFromPeer(context.Background(), batch, remoteActor, remoteKey); !errors.Is(err, ErrAuthorizationDenied) {
		t.Fatalf("trusted but unbound actor error = %v, want authorization denial", err)
	}
	if err := db.BindPeerActor(remoteActor, remoteKey); err != nil {
		t.Fatal(err)
	}
	if err := db.GrantPeerCollectionWrite(remoteActor, "secure"); err != nil {
		t.Fatal(err)
	}
	wrongKey, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.applyRemoteFromPeer(context.Background(), batch, remoteActor, wrongKey); !errors.Is(err, ErrAuthorizationDenied) {
		t.Fatalf("wrong actor key error = %v, want authorization denial", err)
	}
	afterDenial, err := db.Status()
	if err != nil {
		t.Fatal(err)
	}
	afterInfo, err := os.Stat(segment)
	if err != nil {
		t.Fatal(err)
	}
	if before.Frontier.Compare(afterDenial.Frontier) != ClockEqual || before.LogicalDigest != afterDenial.LogicalDigest || info.Size() != afterInfo.Size() {
		t.Fatal("authorization denial changed canonical state or WAL")
	}
	if err := db.ApplyRemote(context.Background(), batch); !errors.Is(err, ErrFeatureUnavailable) {
		t.Fatalf("unsigned direct remote admission error = %v, want feature unavailable", err)
	}
	if err := db.applyRemoteFromPeer(context.Background(), batch, remoteActor, remoteKey); err != nil {
		t.Fatalf("bound remote actor rejected: %v", err)
	}
	if err := db.View(context.Background(), func(read *ReadTx) error {
		document, ok := read.Document("secure", "one")
		if !ok {
			t.Fatal("accepted remote document absent")
		}
		value, ok := document.Value("value")
		if !ok || !value.Equal(String("accepted")) {
			t.Fatalf("accepted remote value = %#v, %t", value, ok)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestCollectionWriteAuthorizationIsAtomicAndPersistent(t *testing.T) {
	db := newTestDB(t)
	remoteID, err := newID()
	if err != nil {
		t.Fatal(err)
	}
	remoteActor := ActorID(remoteID)
	remoteKey, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.TrustPeer("remote", remoteKey); err != nil {
		t.Fatal(err)
	}
	if err := db.BindPeerActor(remoteActor, remoteKey); err != nil {
		t.Fatal(err)
	}
	if err := db.GrantPeerCollectionWrite(remoteActor, "allowed"); err != nil {
		t.Fatal(err)
	}
	batch := signedCollectionBatch(t, remoteActor)
	before, err := db.Status()
	if err != nil {
		t.Fatal(err)
	}
	segment := segmentPath(db.path, db.manifest.Active)
	info, err := os.Stat(segment)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.applyRemoteFromPeer(context.Background(), batch, remoteActor, remoteKey); !errors.Is(err, ErrAuthorizationDenied) {
		t.Fatalf("mixed-scope authorization error = %v, want authorization denial", err)
	}
	after, err := db.Status()
	if err != nil {
		t.Fatal(err)
	}
	afterInfo, err := os.Stat(segment)
	if err != nil {
		t.Fatal(err)
	}
	if before.Frontier.Compare(after.Frontier) != ClockEqual || before.LogicalDigest != after.LogicalDigest || info.Size() != afterInfo.Size() {
		t.Fatal("denied mixed-scope batch changed state, frontier, digest, or WAL")
	}
	if err := db.View(context.Background(), func(read *ReadTx) error {
		if _, exists := read.Document("allowed", "one"); exists {
			t.Fatal("allowed operation from denied batch became visible")
		}
		if _, exists := read.Document("forbidden", "one"); exists {
			t.Fatal("forbidden operation from denied batch became visible")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.GrantPeerCollectionWrite(remoteActor, "forbidden"); err != nil {
		t.Fatal(err)
	}
	if err := db.applyRemoteFromPeer(context.Background(), batch, remoteActor, remoteKey); err != nil {
		t.Fatalf("fully granted mixed-scope batch rejected: %v", err)
	}
	if err := db.View(context.Background(), func(read *ReadTx) error {
		want := map[string]Value{"allowed": String("permitted"), "forbidden": String("denied")}
		for collection, expected := range want {
			document, exists := read.Document(collection, "one")
			if !exists {
				t.Fatalf("granted collection %q missing", collection)
			}
			if value, exists := document.Value("value"); !exists || !value.Equal(expected) {
				t.Fatalf("granted collection %q has unexpected value %#v", collection, value)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.RevokePeerCollectionWrite(remoteActor, "allowed"); err != nil {
		t.Fatal(err)
	}
	if grants := db.PeerWriteCollections(remoteActor); len(grants) != 1 || grants[0] != "forbidden" {
		t.Fatalf("grant revocation result = %#v", grants)
	}
}

func TestSyncPairNeverCreatesCollectionWriteGrants(t *testing.T) {
	ctx := context.Background()
	left := newTestDB(t)
	mustSet(t, left, "tasks", "one", "value", String("left"))
	archive := filepath.Join(t.TempDir(), "seed.zip")
	if err := left.Backup(ctx, archive, false); err != nil {
		t.Fatal(err)
	}
	rightPath := filepath.Join(t.TempDir(), "right")
	if err := Restore(archive, rightPath); err != nil {
		t.Fatal(err)
	}
	right, err := Open(rightPath, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer right.Close()
	if _, err := right.RotateActor(); err != nil {
		t.Fatal(err)
	}
	// This write is absent from the clone and must cross the wire; SyncPair may
	// establish trust/bindings but must not invent its required tasks grant.
	mustSet(t, left, "tasks", "two", "value", String("after-clone"))
	leftKey, err := left.PeerPublicKey()
	if err != nil {
		t.Fatal(err)
	}
	rightKey, err := right.PeerPublicKey()
	if err != nil {
		t.Fatal(err)
	}
	if err := left.TrustAndBindPeerActor("right", right.ActorID(), rightKey); err != nil {
		t.Fatal(err)
	}
	if err := right.TrustAndBindPeerActor("left", left.ActorID(), leftKey); err != nil {
		t.Fatal(err)
	}
	if err := SyncPair(ctx, left, right); err == nil {
		t.Fatal("SyncPair accepted a history-bearing peer without an explicit grant")
	}
	if grants := left.PeerWriteCollections(right.ActorID()); len(grants) != 0 {
		t.Fatalf("SyncPair created left-side grants: %#v", grants)
	}
	if grants := right.PeerWriteCollections(left.ActorID()); len(grants) != 0 {
		t.Fatalf("SyncPair created right-side grants: %#v", grants)
	}
}

func TestActorBindingsPersistAndAreOneToOne(t *testing.T) {
	path := t.TempDir()
	db, err := Open(path, Options{})
	if err != nil {
		t.Fatal(err)
	}
	localKey, err := db.PeerPublicKey()
	if err != nil {
		t.Fatal(err)
	}
	remoteID, err := newID()
	if err != nil {
		t.Fatal(err)
	}
	otherID, err := newID()
	if err != nil {
		t.Fatal(err)
	}
	remoteActor, otherActor := ActorID(remoteID), ActorID(otherID)
	remoteKey, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	otherKey, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.BindPeerActor(remoteActor, remoteKey); !errors.Is(err, ErrPeerUntrusted) {
		t.Fatalf("untrusted bind error = %v, want peer untrusted", err)
	}
	if err := db.TrustPeer("remote", remoteKey); err != nil {
		t.Fatal(err)
	}
	if err := db.TrustPeer("other", otherKey); err != nil {
		t.Fatal(err)
	}
	if err := db.BindPeerActor(remoteActor, remoteKey); err != nil {
		t.Fatal(err)
	}
	if err := db.GrantPeerCollectionWrite(remoteActor, "allowed"); err != nil {
		t.Fatal(err)
	}
	if err := db.BindPeerActor(remoteActor, otherKey); !errors.Is(err, ErrAuthorizationDenied) {
		t.Fatalf("actor key replacement error = %v, want authorization denial", err)
	}
	if err := db.BindPeerActor(otherActor, remoteKey); !errors.Is(err, ErrAuthorizationDenied) {
		t.Fatalf("key actor reuse error = %v, want authorization denial", err)
	}
	if err := db.BindPeerActor(db.ActorID(), otherKey); !errors.Is(err, ErrAuthorizationDenied) {
		t.Fatalf("local actor binding error = %v, want authorization denial", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(path, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	bindings := db.ActorBindings()
	foundLocal, foundRemote := false, false
	for _, binding := range bindings {
		switch binding.Actor {
		case db.ActorID():
			foundLocal = string(binding.PublicKey) == string(localKey)
		case remoteActor:
			foundRemote = string(binding.PublicKey) == string(remoteKey)
		}
	}
	if !foundLocal || !foundRemote {
		t.Fatalf("persisted bindings missing local=%t remote=%t: %#v", foundLocal, foundRemote, bindings)
	}
	if grants := db.PeerWriteCollections(remoteActor); len(grants) != 1 || grants[0] != "allowed" {
		t.Fatalf("persisted collection write grants = %#v", grants)
	}
}

func TestRotateActorRotatesCopiedPeerIdentityAndRePairs(t *testing.T) {
	ctx := context.Background()
	source := newTestDB(t)
	originalActor := source.ActorID()
	originalKey, err := source.PeerPublicKey()
	if err != nil {
		t.Fatal(err)
	}
	archive := filepath.Join(t.TempDir(), "copy.zip")
	if err := source.Backup(ctx, archive, false); err != nil {
		t.Fatal(err)
	}
	copyPath := filepath.Join(t.TempDir(), "copy")
	if err := Restore(archive, copyPath); err != nil {
		t.Fatal(err)
	}
	// A filesystem copy retains the private peer identity, unlike a portable
	// backup. Recreate that condition explicitly.
	identityBytes, err := os.ReadFile(filepath.Join(source.path, peerIdentityName))
	if err != nil {
		t.Fatal(err)
	}
	if err := atomicWrite(filepath.Join(copyPath, peerIdentityName), identityBytes, 0600); err != nil {
		t.Fatal(err)
	}
	copyDB, err := Open(copyPath, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer copyDB.Close()
	if key, err := copyDB.PeerPublicKey(); err != nil || string(key) != string(originalKey) {
		t.Fatalf("copied peer identity = %x, %v; want retained key", key, err)
	}
	rotatedActor, err := copyDB.RotateActor()
	if err != nil {
		t.Fatal(err)
	}
	rotatedKey, err := copyDB.PeerPublicKey()
	if err != nil {
		t.Fatal(err)
	}
	if rotatedActor == originalActor || string(rotatedKey) == string(originalKey) {
		t.Fatal("actor rotation retained an actor or peer key")
	}
	bindings := copyDB.ActorBindings()
	oldBound, newBound := false, false
	for _, binding := range bindings {
		switch binding.Actor {
		case originalActor:
			oldBound = string(binding.PublicKey) == string(originalKey)
		case rotatedActor:
			newBound = string(binding.PublicKey) == string(rotatedKey)
		}
	}
	if !oldBound || !newBound {
		t.Fatalf("rotation did not preserve old and new bindings: %#v", bindings)
	}
	if err := SyncPair(ctx, source, copyDB); err != nil {
		t.Fatalf("re-paired copied replica did not synchronize: %v", err)
	}
}

func TestActorRotationRecoveryFinishesInstalledPeerIdentity(t *testing.T) {
	for _, persistNewIdentity := range []bool{false, true} {
		name := "before-identity"
		if persistNewIdentity {
			name = "before-manifest"
		}
		t.Run(name, func(t *testing.T) {
			path := t.TempDir()
			db, err := Open(path, Options{})
			if err != nil {
				t.Fatal(err)
			}
			oldIdentity, err := db.peerIdentity()
			if err != nil {
				t.Fatal(err)
			}
			oldActor := db.ActorID()
			newID, err := newID()
			if err != nil {
				t.Fatal(err)
			}
			newActor := ActorID(newID)
			freshPublic, freshPrivate, err := ed25519.GenerateKey(rand.Reader)
			if err != nil {
				t.Fatal(err)
			}
			bindings := copyActorBindings(db.actorKeys)
			bindings[oldActor] = oldIdentity.public
			bindings[newActor] = freshPublic
			rotation := actorRotation{from: oldActor, to: newActor, old: oldIdentity.public, fresh: freshPublic}
			if err := writeActorRotation(path, rotation); err != nil {
				t.Fatal(err)
			}
			if err := writeActorBindings(path, bindings); err != nil {
				t.Fatal(err)
			}
			if err := atomicWrite(filepath.Join(path, peerIdentityName), peerIdentityBytes(freshPrivate), 0600); err != nil {
				t.Fatal(err)
			}
			if persistNewIdentity {
				next := db.manifest
				next.Actor, next.NextSeq = newActor, 1
				if err := persistIdentity(path, next); err != nil {
					t.Fatal(err)
				}
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			recovered, err := Open(path, Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer recovered.Close()
			if recovered.ActorID() != newActor {
				t.Fatalf("recovered actor = %v, want %v", recovered.ActorID(), newActor)
			}
			key, err := recovered.PeerPublicKey()
			if err != nil || string(key) != string(freshPublic) {
				t.Fatalf("recovered peer key = %x, %v; want installed key", key, err)
			}
			if _, err := os.Stat(filepath.Join(path, actorRotationName)); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("rotation journal still present or unreadable: %v", err)
			}
		})
	}
}

func TestRemoteAdmissionNeverAcceptsLocalActor(t *testing.T) {
	db := newTestDB(t)
	localKey, err := db.PeerPublicKey()
	if err != nil {
		t.Fatal(err)
	}
	localActor := db.ActorID()
	batch := signedTestBatch(t, localActor)
	if err := db.applyRemoteFromPeer(context.Background(), batch, localActor, localKey); !errors.Is(err, ErrAuthorizationDenied) {
		t.Fatalf("remote local-actor batch error = %v, want authorization denial", err)
	}
	status, err := db.Status()
	if err != nil {
		t.Fatal(err)
	}
	if status.Frontier[localActor] != 0 {
		t.Fatal("remote local-actor batch advanced local frontier")
	}
}

func TestSyncRejectsTrustedUnboundActorAtHandshake(t *testing.T) {
	ctx := context.Background()
	left := newTestDB(t)
	leftKey, err := left.PeerPublicKey()
	if err != nil {
		t.Fatal(err)
	}
	archive := filepath.Join(t.TempDir(), "seed.zip")
	if err := left.Backup(ctx, archive, false); err != nil {
		t.Fatal(err)
	}
	rightPath := filepath.Join(t.TempDir(), "right")
	if err := Restore(archive, rightPath); err != nil {
		t.Fatal(err)
	}
	// This deliberately simulates a legacy/restored replica whose durable actor
	// authorization file has not been administratively provisioned.
	if err := os.Remove(filepath.Join(rightPath, actorBindingsName)); err != nil {
		t.Fatal(err)
	}
	right, err := Open(rightPath, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer right.Close()
	if _, err := right.RotateActor(); err != nil {
		t.Fatal(err)
	}
	rightKey, err := right.PeerPublicKey()
	if err != nil {
		t.Fatal(err)
	}
	if err := left.TrustPeer("right", rightKey); err != nil {
		t.Fatal(err)
	}
	if err := right.TrustPeer("left", leftKey); err != nil {
		t.Fatal(err)
	}
	leftIdentity, err := left.peerIdentity()
	if err != nil {
		t.Fatal(err)
	}
	rightIdentity, err := right.peerIdentity()
	if err != nil {
		t.Fatal(err)
	}
	leftConfig, err := left.clientTLSConfig(leftIdentity, rightKey)
	if err != nil {
		t.Fatal(err)
	}
	rightConfig, err := right.serverTLSConfig(rightIdentity)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := tls.Listen("tcp", "127.0.0.1:0", rightConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	serverResult := make(chan error, 1)
	go func() {
		connection, err := listener.Accept()
		if err != nil {
			serverResult <- err
			return
		}
		defer connection.Close()
		serverResult <- right.syncConnection(ctx, connection.(*tls.Conn), rightIdentity, nil)
	}()
	connection, err := tls.Dial("tcp", listener.Addr().String(), leftConfig)
	if err != nil {
		t.Fatal(err)
	}
	clientErr := left.syncConnection(ctx, connection, leftIdentity, &rightKey)
	_ = connection.Close()
	serverErr := <-serverResult
	if !errors.Is(clientErr, ErrAuthorizationDenied) && !errors.Is(serverErr, ErrAuthorizationDenied) {
		t.Fatalf("unbound actor handshake errors = client %v, server %v; want authorization denial", clientErr, serverErr)
	}
}

func TestTrustAndBindPeerActorRollsBackNewTrustOnBindingFailure(t *testing.T) {
	db := newTestDB(t)
	key, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.TrustAndBindPeerActor("invalid", db.ActorID(), key); !errors.Is(err, ErrAuthorizationDenied) {
		t.Fatalf("invalid local actor pairing error = %v, want authorization denial", err)
	}
	peers, err := db.TrustedPeers()
	if err != nil {
		t.Fatal(err)
	}
	if len(peers) != 0 {
		t.Fatalf("failed pairing retained TLS trust: %#v", peers)
	}
}

func TestSyncRequiresPersistedPeerTrust(t *testing.T) {
	db := newTestDB(t)
	public, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Sync(context.Background(), PeerConfig{Address: "127.0.0.1:1", PublicKey: public}); !errors.Is(err, ErrPeerUntrusted) {
		t.Fatalf("untrusted sync error = %v, want peer untrusted", err)
	}
}
