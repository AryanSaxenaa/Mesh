package mesh0

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

const (
	actorRotationName       = "ACTOR_ROTATION"
	actorRotationGeneration = 1
)

type actorRotation struct {
	from, to   ActorID
	old, fresh ed25519.PublicKey
}

func actorRotationBytes(rotation actorRotation) []byte {
	var encoded encoder
	encoded.raw([]byte("M0AR"))
	encoded.u(actorRotationGeneration)
	encoded.id(ID(rotation.from))
	encoded.id(ID(rotation.to))
	encoded.bytes(rotation.old)
	encoded.bytes(rotation.fresh)
	payload := encoded.Bytes()
	hash := sha256.Sum256(payload)
	return append(payload, hash[:]...)
}

func readActorRotation(root string) (actorRotation, bool, error) {
	var rotation actorRotation
	data, err := os.ReadFile(filepath.Join(root, actorRotationName))
	if errors.Is(err, os.ErrNotExist) {
		return rotation, false, nil
	}
	if err != nil || len(data) < 32 {
		return rotation, false, ErrCorruption
	}
	hash := sha256.Sum256(data[:len(data)-32])
	if string(hash[:]) != string(data[len(data)-32:]) {
		return rotation, false, ErrCorruption
	}
	decoded := decoder{b: data[:len(data)-32]}
	magic, err := decoded.raw(4)
	if err != nil || string(magic) != "M0AR" {
		return rotation, false, ErrCorruption
	}
	generation, err := decoded.u()
	if err != nil || generation != actorRotationGeneration {
		return rotation, false, ErrProtocolIncompatible
	}
	from, err := decoded.id()
	if err != nil || ID(from).IsZero() {
		return rotation, false, ErrCorruption
	}
	to, err := decoded.id()
	if err != nil || ID(to).IsZero() || from == to {
		return rotation, false, ErrCorruption
	}
	old, err := decoded.bytes(ed25519.PublicKeySize)
	if err != nil || len(old) != ed25519.PublicKeySize {
		return rotation, false, ErrCorruption
	}
	fresh, err := decoded.bytes(ed25519.PublicKeySize)
	if err != nil || len(fresh) != ed25519.PublicKeySize || string(old) == string(fresh) || decoded.done() != nil {
		return rotation, false, ErrCorruption
	}
	rotation.from = ActorID(from)
	rotation.to = ActorID(to)
	rotation.old = clonePublicKey(ed25519.PublicKey(old))
	rotation.fresh = clonePublicKey(ed25519.PublicKey(fresh))
	return rotation, true, nil
}

func writeActorRotation(root string, rotation actorRotation) error {
	return atomicWrite(filepath.Join(root, actorRotationName), actorRotationBytes(rotation), 0600)
}

func clearActorRotation(root string) error {
	err := os.Remove(filepath.Join(root, actorRotationName))
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return syncDir(root)
}

// recoverActorRotation resolves a durable local actor/key rotation before Open
// compares IDENTITY with MANIFEST. The journal makes every crash point either
// roll back to the old local identity or finish the new one; it never silently
// associates an actor with an unexpected signing key.
func recoverActorRotation(root string) error {
	rotation, exists, err := readActorRotation(root)
	if err != nil || !exists {
		return err
	}
	current, err := readManifest(root)
	if err != nil {
		return err
	}
	identityDB, identityActor, _, err := parseIdentityFile(root)
	if err != nil || identityDB != current.Database {
		return ErrCorruption
	}
	bindings, err := readActorBindings(root)
	if err != nil {
		return err
	}
	identity, err := readPeerIdentity(filepath.Join(root, peerIdentityName))
	if err != nil {
		return ErrCorruption
	}
	if binding, ok := bindings[rotation.to]; ok && string(binding) != string(rotation.fresh) {
		return ErrCorruption
	}
	if binding, ok := bindings[rotation.from]; ok && string(binding) != string(rotation.old) {
		return ErrCorruption
	}

	switch {
	case current.Actor == rotation.to && identityActor == rotation.to && string(identity.public) == string(rotation.fresh):
		if string(bindings[rotation.to]) != string(rotation.fresh) {
			return ErrCorruption
		}
		return clearActorRotation(root)
	case current.Actor == rotation.from && identityActor == rotation.from && string(identity.public) == string(rotation.old):
		// The new peer identity was not durably installed. Discard its staged
		// binding and return to the previous local identity.
		if _, staged := bindings[rotation.to]; staged {
			delete(bindings, rotation.to)
			if err := writeActorBindings(root, bindings); err != nil {
				return err
			}
		}
		return clearActorRotation(root)
	case current.Actor == rotation.from && identityActor == rotation.from && string(identity.public) == string(rotation.fresh):
		// The new peer key is durable, so finish the actor/manifest transition.
		if string(bindings[rotation.to]) != string(rotation.fresh) {
			return ErrCorruption
		}
		next := current
		next.Actor, next.NextSeq = rotation.to, 1
		if err := persistIdentity(root, next); err != nil {
			return err
		}
		if err := writeManifest(root, next); err != nil {
			return err
		}
		return clearActorRotation(root)
	case current.Actor == rotation.from && identityActor == rotation.to && string(identity.public) == string(rotation.fresh):
		// IDENTITY was published but MANIFEST was not. The target binding and
		// peer key were made durable first, so publishing the manifest completes
		// the same transition without reusing or changing any key material.
		if string(bindings[rotation.to]) != string(rotation.fresh) {
			return ErrCorruption
		}
		next := current
		next.Actor, next.NextSeq = rotation.to, 1
		if err := writeManifest(root, next); err != nil {
			return err
		}
		return clearActorRotation(root)
	default:
		return ErrCorruption
	}
}

// RotateActor turns a restored/copied database into a distinct writable
// replica without changing its database identity or retained causal history.
// If the copy retains a peer key, it creates a fresh peer key as part of a
// journaled transition so old signed history retains its original binding and
// the new actor can only be used after explicit re-pairing by remote replicas.
func (db *DB) RotateActor() (ActorID, error) {
	db.identityMu.Lock()
	defer db.identityMu.Unlock()
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return ActorID{}, ErrClosed
	}
	if db.failed != nil {
		return ActorID{}, db.failed
	}
	id, err := newID()
	if err != nil {
		return ActorID{}, err
	}
	next := db.manifest
	next.Actor, next.NextSeq = ActorID(id), 1

	oldIdentity, err := readPeerIdentity(filepath.Join(db.path, peerIdentityName))
	if errors.Is(err, os.ErrNotExist) {
		if err := persistIdentity(db.path, next); err != nil {
			return ActorID{}, db.failWriteLocked(fmt.Errorf("rotate identity: %w", err))
		}
		if err := writeManifest(db.path, next); err != nil {
			return ActorID{}, db.failWriteLocked(fmt.Errorf("rotate manifest: %w", err))
		}
		db.manifest = next
		return next.Actor, nil
	}
	if err != nil {
		return ActorID{}, err
	}
	if existing, ok := db.actorKeys[db.manifest.Actor]; ok && string(existing) != string(oldIdentity.public) {
		return ActorID{}, fmt.Errorf("%w: local actor binding differs from peer identity", ErrCorruption)
	}
	for actor, key := range db.actorKeys {
		if actor != db.manifest.Actor && string(key) == string(oldIdentity.public) {
			return ActorID{}, fmt.Errorf("%w: local peer key belongs to another actor", ErrCorruption)
		}
	}
	freshPublic, freshPrivate, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return ActorID{}, err
	}
	bindings := copyActorBindings(db.actorKeys)
	bindings[db.manifest.Actor] = clonePublicKey(oldIdentity.public)
	bindings[next.Actor] = clonePublicKey(freshPublic)
	rotation := actorRotation{from: db.manifest.Actor, to: next.Actor, old: oldIdentity.public, fresh: freshPublic}
	if err := writeActorRotation(db.path, rotation); err != nil {
		return ActorID{}, err
	}
	if err := writeActorBindings(db.path, bindings); err != nil {
		return ActorID{}, err
	}
	if err := atomicWrite(filepath.Join(db.path, peerIdentityName), peerIdentityBytes(freshPrivate), 0600); err != nil {
		return ActorID{}, db.failWriteLocked(fmt.Errorf("rotate peer identity: %w", err))
	}
	if err := persistIdentity(db.path, next); err != nil {
		return ActorID{}, db.failWriteLocked(fmt.Errorf("rotate identity: %w", err))
	}
	if err := writeManifest(db.path, next); err != nil {
		return ActorID{}, db.failWriteLocked(fmt.Errorf("rotate manifest: %w", err))
	}
	db.manifest = next
	db.actorKeys = bindings
	if err := clearActorRotation(db.path); err != nil {
		return ActorID{}, db.failWriteLocked(fmt.Errorf("finalize actor rotation: %w", err))
	}
	return next.Actor, nil
}
