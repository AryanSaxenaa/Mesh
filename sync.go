package mesh0

import (
	"bufio"
	"container/heap"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

const (
	peerIdentityName                   = "PEER_IDENTITY"
	actorBindingsName                  = "ACTOR_BINDINGS"
	actorWriteGrantsName               = "ACTOR_WRITE_GRANTS"
	peersDir                           = "peers"
	actorBindingGeneration             = 1
	actorWriteGrantGeneration          = 1
	maxActorBindings                   = 4096
	maxWriteCollectionsPerActor        = 256
	maxActorWriteGrants                = 4096
	maxSyncRanges                      = 4096
	maxSyncRangeSpan            uint64 = 1 << 20
	maxWireFrame                       = maxBatchBytes + 4096
	maxPendingBatches                  = 1024
	maxPendingBytes                    = 64 << 20
	maxSyncResponseBatches             = maxPendingBatches
	maxSyncResponseBytes               = maxPendingBytes
	maxSyncRounds                      = 4096
	wireHello                   byte   = 1
	wireClock                   byte   = 2
	wireBatch                   byte   = 3
	wireDone                    byte   = 4
	wireDigest                  byte   = 5
	wireError                   byte   = 6
	wireRanges                  byte   = 7
)

type PeerConfig struct {
	Name      string
	Address   string
	PublicKey ed25519.PublicKey
}
type Peer struct {
	Name, Fingerprint string
	PublicKey         ed25519.PublicKey
}

// ActorBinding pins an actor to a peer public key. WriteCollections is the
// explicit collection-write capability set for that actor; an empty set is
// deliberately default-deny.
type ActorBinding struct {
	Actor            ActorID
	PublicKey        ed25519.PublicKey
	WriteCollections []string
}

func clonePublicKey(key ed25519.PublicKey) ed25519.PublicKey {
	return append(ed25519.PublicKey(nil), key...)
}

func actorBindingsBytes(bindings map[ActorID]ed25519.PublicKey) ([]byte, error) {
	if len(bindings) > maxActorBindings {
		return nil, ErrResourceLimit
	}
	actors := make([]ActorID, 0, len(bindings))
	for actor, key := range bindings {
		if ID(actor).IsZero() || len(key) != ed25519.PublicKeySize {
			return nil, ErrInvalidArgument
		}
		actors = append(actors, actor)
	}
	sort.Slice(actors, func(i, j int) bool { return idCompare(ID(actors[i]), ID(actors[j])) < 0 })
	var encoded encoder
	encoded.raw([]byte("M0AK"))
	encoded.u(actorBindingGeneration)
	encoded.u(uint64(len(actors)))
	for _, actor := range actors {
		encoded.id(ID(actor))
		encoded.bytes(bindings[actor])
	}
	payload := encoded.Bytes()
	hash := sha256.Sum256(payload)
	return append(payload, hash[:]...), nil
}

func decodeActorBindings(data []byte) (map[ActorID]ed25519.PublicKey, error) {
	if len(data) < 32 {
		return nil, ErrCorruption
	}
	hash := sha256.Sum256(data[:len(data)-32])
	if string(hash[:]) != string(data[len(data)-32:]) {
		return nil, ErrCorruption
	}
	decoded := decoder{b: data[:len(data)-32]}
	magic, err := decoded.raw(4)
	if err != nil || string(magic) != "M0AK" {
		return nil, ErrCorruption
	}
	generation, err := decoded.u()
	if err != nil || generation != actorBindingGeneration {
		return nil, ErrProtocolIncompatible
	}
	count, err := decoded.u()
	if err != nil || count > maxActorBindings {
		return nil, ErrCorruption
	}
	bindings := make(map[ActorID]ed25519.PublicKey, int(count))
	keys := make(map[string]ActorID, int(count))
	var previous ID
	for index := uint64(0); index < count; index++ {
		id, err := decoded.id()
		if err != nil || ID(id).IsZero() || (index > 0 && idCompare(previous, id) >= 0) {
			return nil, ErrCorruption
		}
		key, err := decoded.bytes(ed25519.PublicKeySize)
		if err != nil || len(key) != ed25519.PublicKeySize {
			return nil, ErrCorruption
		}
		actor := ActorID(id)
		if _, exists := keys[string(key)]; exists {
			return nil, ErrCorruption
		}
		bindings[actor] = clonePublicKey(ed25519.PublicKey(key))
		keys[string(key)] = actor
		previous = id
	}
	if err := decoded.done(); err != nil {
		return nil, ErrCorruption
	}
	return bindings, nil
}

func readActorBindings(root string) (map[ActorID]ed25519.PublicKey, error) {
	data, err := os.ReadFile(filepath.Join(root, actorBindingsName))
	if errors.Is(err, os.ErrNotExist) {
		return make(map[ActorID]ed25519.PublicKey), nil
	}
	if err != nil {
		return nil, err
	}
	return decodeActorBindings(data)
}

func writeActorBindings(root string, bindings map[ActorID]ed25519.PublicKey) error {
	data, err := actorBindingsBytes(bindings)
	if err != nil {
		return err
	}
	return atomicWrite(filepath.Join(root, actorBindingsName), data, 0600)
}

func copyActorBindings(bindings map[ActorID]ed25519.PublicKey) map[ActorID]ed25519.PublicKey {
	result := make(map[ActorID]ed25519.PublicKey, len(bindings))
	for actor, key := range bindings {
		result[actor] = clonePublicKey(key)
	}
	return result
}

func validWriteCollection(collection string) bool {
	return collection != "" && len(collection) <= 1024
}

func copyActorWriteGrants(grants map[ActorID]map[string]struct{}) map[ActorID]map[string]struct{} {
	result := make(map[ActorID]map[string]struct{}, len(grants))
	for actor, collections := range grants {
		copy := make(map[string]struct{}, len(collections))
		for collection := range collections {
			copy[collection] = struct{}{}
		}
		result[actor] = copy
	}
	return result
}

func sortedWriteCollections(collections map[string]struct{}) []string {
	result := make([]string, 0, len(collections))
	for collection := range collections {
		result = append(result, collection)
	}
	sort.Strings(result)
	return result
}

func actorWriteGrantsBytes(grants map[ActorID]map[string]struct{}) ([]byte, error) {
	if len(grants) > maxActorBindings {
		return nil, ErrResourceLimit
	}
	actors := make([]ActorID, 0, len(grants))
	total := 0
	for actor, collections := range grants {
		if ID(actor).IsZero() || len(collections) == 0 || len(collections) > maxWriteCollectionsPerActor {
			return nil, ErrInvalidArgument
		}
		total += len(collections)
		if total > maxActorWriteGrants {
			return nil, ErrResourceLimit
		}
		for collection := range collections {
			if !validWriteCollection(collection) {
				return nil, ErrInvalidArgument
			}
		}
		actors = append(actors, actor)
	}
	sort.Slice(actors, func(i, j int) bool { return idCompare(ID(actors[i]), ID(actors[j])) < 0 })
	var encoded encoder
	encoded.raw([]byte("M0AG"))
	encoded.u(actorWriteGrantGeneration)
	encoded.u(uint64(len(actors)))
	for _, actor := range actors {
		collections := sortedWriteCollections(grants[actor])
		encoded.id(ID(actor))
		encoded.u(uint64(len(collections)))
		for _, collection := range collections {
			encoded.str(collection)
		}
	}
	payload := encoded.Bytes()
	if len(payload) > maxBatchBytes {
		return nil, ErrResourceLimit
	}
	hash := sha256.Sum256(payload)
	return append(payload, hash[:]...), nil
}

func decodeActorWriteGrants(data []byte) (map[ActorID]map[string]struct{}, error) {
	if len(data) < 32 || len(data) > maxBatchBytes+32 {
		return nil, ErrCorruption
	}
	hash := sha256.Sum256(data[:len(data)-32])
	if string(hash[:]) != string(data[len(data)-32:]) {
		return nil, ErrCorruption
	}
	decoded := decoder{b: data[:len(data)-32]}
	magic, err := decoded.raw(4)
	if err != nil || string(magic) != "M0AG" {
		return nil, ErrCorruption
	}
	generation, err := decoded.u()
	if err != nil || generation != actorWriteGrantGeneration {
		return nil, ErrProtocolIncompatible
	}
	count, err := decoded.u()
	if err != nil || count > maxActorBindings {
		return nil, ErrCorruption
	}
	grants := make(map[ActorID]map[string]struct{}, int(count))
	var previous ID
	total := uint64(0)
	for index := uint64(0); index < count; index++ {
		id, err := decoded.id()
		if err != nil || ID(id).IsZero() || (index > 0 && idCompare(previous, id) >= 0) {
			return nil, ErrCorruption
		}
		collectionCount, err := decoded.u()
		if err != nil || collectionCount == 0 || collectionCount > maxWriteCollectionsPerActor || total+collectionCount > maxActorWriteGrants {
			return nil, ErrCorruption
		}
		collections := make(map[string]struct{}, int(collectionCount))
		previousCollection := ""
		for collectionIndex := uint64(0); collectionIndex < collectionCount; collectionIndex++ {
			collection, err := decoded.str(1024)
			if err != nil || !validWriteCollection(collection) || (collectionIndex > 0 && previousCollection >= collection) {
				return nil, ErrCorruption
			}
			collections[collection] = struct{}{}
			previousCollection = collection
		}
		grants[ActorID(id)] = collections
		previous = id
		total += collectionCount
	}
	if err := decoded.done(); err != nil {
		return nil, ErrCorruption
	}
	return grants, nil
}

func readActorWriteGrants(root string, bindings map[ActorID]ed25519.PublicKey, localActor ActorID) (map[ActorID]map[string]struct{}, error) {
	path := filepath.Join(root, actorWriteGrantsName)
	info, err := os.Stat(path)
	if errors.Is(err, os.ErrNotExist) {
		return make(map[ActorID]map[string]struct{}), nil
	}
	if err != nil {
		return nil, err
	}
	if info.Size() < 0 || info.Size() > maxBatchBytes+32 {
		return nil, ErrResourceLimit
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	grants, err := decodeActorWriteGrants(data)
	if err != nil {
		return nil, err
	}
	for actor := range grants {
		if actor == localActor {
			return nil, ErrCorruption
		}
		if _, bound := bindings[actor]; !bound {
			return nil, ErrCorruption
		}
	}
	return grants, nil
}

func writeActorWriteGrants(root string, grants map[ActorID]map[string]struct{}) error {
	data, err := actorWriteGrantsBytes(grants)
	if err != nil {
		return err
	}
	return atomicWrite(filepath.Join(root, actorWriteGrantsName), data, 0600)
}

// GrantPeerCollectionWrite grants one bound remote actor permission to create,
// update, or delete documents in collection. Grants are local administrative
// metadata and are not replicated as CRDT operations.
func (db *DB) GrantPeerCollectionWrite(actor ActorID, collection string) error {
	if ID(actor).IsZero() || !validWriteCollection(collection) {
		return fmt.Errorf("%w: collection write grant", ErrInvalidArgument)
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return ErrClosed
	}
	if db.failed != nil {
		return db.failed
	}
	if actor == db.manifest.Actor || db.actorKeys[actor] == nil {
		return fmt.Errorf("%w: remote actor binding", ErrAuthorizationDenied)
	}
	if _, granted := db.actorWriteGrants[actor][collection]; granted {
		return nil
	}
	next := copyActorWriteGrants(db.actorWriteGrants)
	if next[actor] == nil {
		next[actor] = make(map[string]struct{})
	}
	next[actor][collection] = struct{}{}
	if err := writeActorWriteGrants(db.path, next); err != nil {
		return err
	}
	db.actorWriteGrants = next
	db.logger.Info("peer.collection_write_granted", "actor", ID(actor).String(), "collection", collection)
	return nil
}

// RevokePeerCollectionWrite removes a remote actor's collection-write grant.
// Revocation cannot erase data already disclosed to that peer or undo batches
// accepted while the grant was active.
func (db *DB) RevokePeerCollectionWrite(actor ActorID, collection string) error {
	if ID(actor).IsZero() || !validWriteCollection(collection) {
		return fmt.Errorf("%w: collection write grant", ErrInvalidArgument)
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return ErrClosed
	}
	if db.failed != nil {
		return db.failed
	}
	if actor == db.manifest.Actor || db.actorKeys[actor] == nil {
		return fmt.Errorf("%w: remote actor binding", ErrAuthorizationDenied)
	}
	if _, granted := db.actorWriteGrants[actor][collection]; !granted {
		return nil
	}
	next := copyActorWriteGrants(db.actorWriteGrants)
	delete(next[actor], collection)
	if len(next[actor]) == 0 {
		delete(next, actor)
	}
	if err := writeActorWriteGrants(db.path, next); err != nil {
		return err
	}
	db.actorWriteGrants = next
	db.logger.Info("peer.collection_write_revoked", "actor", ID(actor).String(), "collection", collection)
	return nil
}

// PeerWriteCollections returns the sorted collection-write grants for actor.
func (db *DB) PeerWriteCollections(actor ActorID) []string {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return sortedWriteCollections(db.actorWriteGrants[actor])
}

func (db *DB) authorizeRemoteBatchLocked(actor ActorID, batch Batch) error {
	grants := db.actorWriteGrants[actor]
	for _, operation := range batch.Operations {
		if _, granted := grants[operation.Document.Collection]; !granted {
			db.logger.Warn("sync.authorization_denied", "actor", ID(actor).String(), "collection", operation.Document.Collection, "transaction", batch.ID())
			return fmt.Errorf("%w: actor has no write grant for collection %q", ErrAuthorizationDenied, operation.Document.Collection)
		}
	}
	return nil
}

func (db *DB) actorBoundToKey(actor ActorID, public ed25519.PublicKey) bool {
	if len(public) != ed25519.PublicKeySize {
		return false
	}
	db.mu.RLock()
	defer db.mu.RUnlock()
	bound, ok := db.actorKeys[actor]
	return ok && string(bound) == string(public)
}

func (db *DB) bindLocalActor(public ed25519.PublicKey) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return ErrClosed
	}
	if db.failed != nil {
		return db.failed
	}
	actor := db.manifest.Actor
	if bound, exists := db.actorKeys[actor]; exists {
		if string(bound) != string(public) {
			return fmt.Errorf("%w: local actor binding differs from peer identity", ErrCorruption)
		}
		return nil
	}
	for boundActor, key := range db.actorKeys {
		if string(key) == string(public) && boundActor != actor {
			return fmt.Errorf("%w: local peer key belongs to another actor", ErrCorruption)
		}
	}
	next := copyActorBindings(db.actorKeys)
	next[actor] = clonePublicKey(public)
	if err := writeActorBindings(db.path, next); err != nil {
		return err
	}
	db.actorKeys = next
	return nil
}

// BindPeerActor explicitly authorizes a previously trusted peer key to author
// batches for one actor. It is deliberately not trust-on-first-use: the actor
// and key must be confirmed through an administrative pairing step.
func (db *DB) BindPeerActor(actor ActorID, public ed25519.PublicKey) error {
	return db.bindPeerActor(actor, public, true)
}

// bindPeerActor permits TrustAndBindPeerActor to stage an actor binding before
// transport trust is made durable. A staged binding alone cannot pass TLS
// admission, so a process interruption remains fail-closed.
func (db *DB) bindPeerActor(actor ActorID, public ed25519.PublicKey, requireTrust bool) error {
	if ID(actor).IsZero() || len(public) != ed25519.PublicKeySize {
		return fmt.Errorf("%w: actor binding", ErrInvalidArgument)
	}
	if requireTrust && !db.peerTrusted(public) {
		return ErrPeerUntrusted
	}
	local, err := db.PeerPublicKey()
	if err != nil {
		return err
	}
	if string(local) == string(public) {
		return fmt.Errorf("%w: local peer key", ErrAuthorizationDenied)
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return ErrClosed
	}
	if db.failed != nil {
		return db.failed
	}
	if actor == db.manifest.Actor {
		return fmt.Errorf("%w: local actor", ErrAuthorizationDenied)
	}
	if current, exists := db.actorKeys[actor]; exists {
		if string(current) == string(public) {
			return nil
		}
		return fmt.Errorf("%w: actor already has a different key", ErrAuthorizationDenied)
	}
	for boundActor, key := range db.actorKeys {
		if string(key) == string(public) && boundActor != actor {
			return fmt.Errorf("%w: peer key already belongs to another actor", ErrAuthorizationDenied)
		}
	}
	next := copyActorBindings(db.actorKeys)
	next[actor] = clonePublicKey(public)
	if err := writeActorBindings(db.path, next); err != nil {
		return err
	}
	db.actorKeys = next
	return nil
}

// TrustAndBindPeerActor persists actor ownership before its TLS trust pin. The
// staged binding is inert without the pin, so an interrupted or failed pairing
// cannot expand transport admission; the final pin is written only after the
// binding validates and becomes durable.
func (db *DB) TrustAndBindPeerActor(name string, actor ActorID, public ed25519.PublicKey) error {
	if err := db.bindPeerActor(actor, public, false); err != nil {
		return err
	}
	return db.TrustPeer(name, public)
}

// ActorBindings returns a stable snapshot of the authorized actor-to-key map.
func (db *DB) ActorBindings() []ActorBinding {
	db.mu.RLock()
	defer db.mu.RUnlock()
	bindings := make([]ActorBinding, 0, len(db.actorKeys))
	for actor, key := range db.actorKeys {
		bindings = append(bindings, ActorBinding{Actor: actor, PublicKey: clonePublicKey(key), WriteCollections: sortedWriteCollections(db.actorWriteGrants[actor])})
	}
	sort.Slice(bindings, func(i, j int) bool { return idCompare(ID(bindings[i].Actor), ID(bindings[j].Actor)) < 0 })
	return bindings
}

type peerIdentity struct {
	private ed25519.PrivateKey
	public  ed25519.PublicKey
}

func peerIdentityBytes(private ed25519.PrivateKey) []byte {
	var encoded encoder
	encoded.raw([]byte("M0PK"))
	encoded.u(uint64(formatGeneration))
	encoded.bytes(private)
	payload := encoded.Bytes()
	hash := sha256.Sum256(payload)
	return append(payload, hash[:]...)
}
func readPeerIdentity(path string) (peerIdentity, error) {
	var result peerIdentity
	bytes, err := os.ReadFile(path)
	if err != nil {
		return result, err
	}
	if len(bytes) < 32 {
		return result, ErrCorruption
	}
	hash := sha256.Sum256(bytes[:len(bytes)-32])
	if string(hash[:]) != string(bytes[len(bytes)-32:]) {
		return result, ErrCorruption
	}
	decoded := decoder{b: bytes[:len(bytes)-32]}
	magic, err := decoded.raw(4)
	if err != nil || string(magic) != "M0PK" {
		return result, ErrCorruption
	}
	generation, err := decoded.u()
	if err != nil || generation != formatGeneration {
		return result, ErrCorruption
	}
	private, err := decoded.bytes(ed25519.PrivateKeySize)
	if err != nil || len(private) != ed25519.PrivateKeySize {
		return result, ErrCorruption
	}
	if err := decoded.done(); err != nil {
		return result, err
	}
	result.private = ed25519.PrivateKey(private)
	result.public = result.private.Public().(ed25519.PublicKey)
	return result, nil
}
func (db *DB) peerIdentity() (peerIdentity, error) {
	if err := db.openErr(); err != nil {
		return peerIdentity{}, err
	}
	db.identityMu.Lock()
	defer db.identityMu.Unlock()
	path := filepath.Join(db.path, peerIdentityName)
	identity, err := readPeerIdentity(path)
	if err == nil {
		if err := db.bindLocalActor(identity.public); err != nil {
			return peerIdentity{}, err
		}
		return identity, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return peerIdentity{}, err
	}
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return peerIdentity{}, err
	}
	if err := atomicWrite(path, peerIdentityBytes(private), 0600); err != nil {
		return peerIdentity{}, err
	}
	identity = peerIdentity{private: private, public: public}
	if err := db.bindLocalActor(identity.public); err != nil {
		return peerIdentity{}, err
	}
	return identity, nil
}
func (db *DB) PeerPublicKey() (ed25519.PublicKey, error) {
	identity, err := db.peerIdentity()
	return append(ed25519.PublicKey(nil), identity.public...), err
}
func peerFingerprint(key ed25519.PublicKey) string {
	hash := sha256.Sum256(key)
	return hex.EncodeToString(hash[:])
}

func peerPath(root string, key ed25519.PublicKey) string {
	return filepath.Join(root, peersDir, peerFingerprint(key)+".peer")
}
func (db *DB) TrustPeer(name string, public ed25519.PublicKey) error {
	if len(public) != ed25519.PublicKeySize || name == "" || strings.ContainsAny(name, `\\/:*?"<>|`) {
		return fmt.Errorf("%w: peer identity", ErrInvalidArgument)
	}
	if err := os.MkdirAll(filepath.Join(db.path, peersDir), 0700); err != nil {
		return err
	}
	var encoded encoder
	encoded.raw([]byte("M0PR"))
	encoded.u(uint64(formatGeneration))
	encoded.str(name)
	encoded.bytes(public)
	return atomicWrite(peerPath(db.path, public), encoded.Bytes(), 0600)
}
func decodePeer(data []byte) (Peer, error) {
	decoded := decoder{b: data}
	magic, err := decoded.raw(4)
	if err != nil || string(magic) != "M0PR" {
		return Peer{}, ErrCorruption
	}
	generation, err := decoded.u()
	if err != nil || generation != formatGeneration {
		return Peer{}, ErrCorruption
	}
	name, err := decoded.str(1024)
	if err != nil {
		return Peer{}, err
	}
	key, err := decoded.bytes(ed25519.PublicKeySize)
	if err != nil || len(key) != ed25519.PublicKeySize {
		return Peer{}, ErrCorruption
	}
	if err := decoded.done(); err != nil {
		return Peer{}, err
	}
	return Peer{Name: name, PublicKey: ed25519.PublicKey(key), Fingerprint: peerFingerprint(key)}, nil
}
func (db *DB) TrustedPeers() ([]Peer, error) {
	entries, err := os.ReadDir(filepath.Join(db.path, peersDir))
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	peers := make([]Peer, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".peer") {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(db.path, peersDir, entry.Name()))
		if err != nil {
			return nil, err
		}
		peer, err := decodePeer(raw)
		if err != nil {
			return nil, err
		}
		peers = append(peers, peer)
	}
	sort.Slice(peers, func(i, j int) bool { return peers[i].Name < peers[j].Name })
	return peers, nil
}
func (db *DB) peerTrusted(key ed25519.PublicKey) bool {
	peers, err := db.TrustedPeers()
	if err != nil {
		return false
	}
	for _, peer := range peers {
		if string(peer.PublicKey) == string(key) {
			return true
		}
	}
	return false
}

func certificate(identity peerIdentity) (tls.Certificate, error) {
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 120))
	if err != nil {
		return tls.Certificate{}, err
	}
	template := &x509.Certificate{SerialNumber: serial, NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(365 * 24 * time.Hour), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth}, BasicConstraintsValid: true}
	der, err := x509.CreateCertificate(rand.Reader, template, template, identity.public, identity.private)
	if err != nil {
		return tls.Certificate{}, err
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: identity.private}, nil
}
func publicFromRawCertificate(raw []byte) (ed25519.PublicKey, error) {
	certificate, err := x509.ParseCertificate(raw)
	if err != nil {
		return nil, err
	}
	public, ok := certificate.PublicKey.(ed25519.PublicKey)
	if !ok || len(public) != ed25519.PublicKeySize {
		return nil, ErrCorruption
	}
	return public, nil
}
func (db *DB) serverTLSConfig(identity peerIdentity) (*tls.Config, error) {
	cert, err := certificate(identity)
	if err != nil {
		return nil, err
	}
	return &tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{cert}, ClientAuth: tls.RequireAnyClientCert, VerifyPeerCertificate: func(raw [][]byte, _ [][]*x509.Certificate) error {
		if len(raw) != 1 {
			return fmt.Errorf("%w: certificate count", ErrCorruption)
		}
		key, err := publicFromRawCertificate(raw[0])
		if err != nil {
			return err
		}
		if !db.peerTrusted(key) {
			return fmt.Errorf("%w: peer is not trusted", ErrInvalidArgument)
		}
		return nil
	}}, nil
}
func (db *DB) clientTLSConfig(identity peerIdentity, expected ed25519.PublicKey) (*tls.Config, error) {
	if len(expected) != ed25519.PublicKeySize {
		return nil, fmt.Errorf("%w: peer public key", ErrInvalidArgument)
	}
	cert, err := certificate(identity)
	if err != nil {
		return nil, err
	}
	// Public PKI cannot authenticate a private mesh. The callback below performs
	// exact Ed25519 certificate-key pinning before any protocol frame is accepted.
	return &tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{cert}, InsecureSkipVerify: true, VerifyPeerCertificate: func(raw [][]byte, _ [][]*x509.Certificate) error {
		if len(raw) != 1 {
			return fmt.Errorf("%w: certificate count", ErrCorruption)
		}
		key, err := publicFromRawCertificate(raw[0])
		if err != nil {
			return err
		}
		if string(key) != string(expected) {
			return fmt.Errorf("%w: TLS peer key mismatch", ErrInvalidArgument)
		}
		return nil
	}}, nil
}

func writeWireFrame(writer io.Writer, kind byte, payload []byte) error {
	if len(payload)+1 > maxWireFrame {
		return ErrResourceLimit
	}
	var prefix [10]byte
	size := binary.PutUvarint(prefix[:], uint64(len(payload)+1))
	if _, err := writer.Write(prefix[:size]); err != nil {
		return err
	}
	if _, err := writer.Write([]byte{kind}); err != nil {
		return err
	}
	_, err := writer.Write(payload)
	return err
}
func readWireUvarint(reader *bufio.Reader) (uint64, error) {
	var bytes [10]byte
	for index := range bytes {
		byteValue, err := reader.ReadByte()
		if err != nil {
			return 0, err
		}
		bytes[index] = byteValue
		if byteValue < 0x80 {
			value, count := binary.Uvarint(bytes[:index+1])
			if count <= 0 {
				return 0, ErrCorruption
			}
			var canonical [10]byte
			if binary.PutUvarint(canonical[:], value) != index+1 || string(canonical[:index+1]) != string(bytes[:index+1]) {
				return 0, ErrCorruption
			}
			return value, nil
		}
	}
	return 0, ErrCorruption
}
func readWireFrame(reader *bufio.Reader) (byte, []byte, error) {
	size, err := readWireUvarint(reader)
	if err != nil {
		return 0, nil, err
	}
	if size == 0 || size > maxWireFrame {
		return 0, nil, ErrResourceLimit
	}
	data := make([]byte, size)
	if _, err := io.ReadFull(reader, data); err != nil {
		return 0, nil, err
	}
	return data[0], data[1:], nil
}

type signedBatch struct {
	database  DatabaseID
	canonical []byte
	signature []byte
}

func signedBatchMessage(database DatabaseID, canonical []byte) []byte {
	var encoded encoder
	encoded.raw([]byte("M0BS"))
	encoded.u(1)
	encoded.id(ID(database))
	encoded.bytes(canonical)
	digest := sha256.Sum256(encoded.Bytes())
	return digest[:]
}

func signBatch(identity peerIdentity, database DatabaseID, batch Batch) ([]byte, error) {
	canonical, err := batch.MarshalBinary()
	if err != nil {
		return nil, err
	}
	var encoded encoder
	encoded.raw([]byte("M0SB"))
	encoded.u(1)
	encoded.id(ID(database))
	encoded.bytes(canonical)
	encoded.bytes(ed25519.Sign(identity.private, signedBatchMessage(database, canonical)))
	return encoded.Bytes(), nil
}

func decodeSignedBatch(payload []byte, public ed25519.PublicKey, expectedDatabase DatabaseID) (Batch, error) {
	decoded := decoder{b: payload}
	magic, err := decoded.raw(4)
	if err != nil || string(magic) != "M0SB" {
		return Batch{}, ErrCorruption
	}
	generation, err := decoded.u()
	if err != nil || generation != 1 {
		return Batch{}, ErrProtocolIncompatible
	}
	database, err := decoded.id()
	if err != nil || DatabaseID(database) != expectedDatabase {
		return Batch{}, ErrAuthorizationDenied
	}
	canonical, err := decoded.bytes(maxBatchBytes)
	if err != nil {
		return Batch{}, err
	}
	signature, err := decoded.bytes(ed25519.SignatureSize)
	if err != nil || len(signature) != ed25519.SignatureSize || decoded.done() != nil {
		return Batch{}, ErrCorruption
	}
	if len(public) != ed25519.PublicKeySize || !ed25519.Verify(public, signedBatchMessage(expectedDatabase, canonical), signature) {
		return Batch{}, ErrInvalidSignature
	}
	batch, err := UnmarshalBatch(canonical)
	if err != nil {
		return Batch{}, err
	}
	reencoded, err := batch.MarshalBinary()
	if err != nil || string(reencoded) != string(canonical) {
		return Batch{}, ErrCorruption
	}
	return batch, nil
}

type syncHello struct {
	database DatabaseID
	actor    ActorID
	public   ed25519.PublicKey
}

func (h syncHello) encode() []byte {
	var encoded encoder
	encoded.raw([]byte("M0HL"))
	encoded.u(uint64(formatGeneration))
	encoded.id(ID(h.database))
	encoded.id(ID(h.actor))
	encoded.bytes(h.public)
	return encoded.Bytes()
}
func decodeHello(data []byte) (syncHello, error) {
	var hello syncHello
	decoded := decoder{b: data}
	magic, err := decoded.raw(4)
	if err != nil || string(magic) != "M0HL" {
		return hello, ErrCorruption
	}
	generation, err := decoded.u()
	if err != nil || generation != formatGeneration {
		return hello, ErrCorruption
	}
	database, err := decoded.id()
	if err != nil {
		return hello, err
	}
	actor, err := decoded.id()
	if err != nil {
		return hello, err
	}
	key, err := decoded.bytes(ed25519.PublicKeySize)
	if err != nil || len(key) != ed25519.PublicKeySize {
		return hello, ErrCorruption
	}
	if err := decoded.done(); err != nil {
		return hello, err
	}
	hello.database, hello.actor, hello.public = DatabaseID(database), ActorID(actor), ed25519.PublicKey(key)
	return hello, nil
}
func encodeClock(clock VersionVector) []byte {
	var encoded encoder
	encoded.clock(clock)
	return encoded.Bytes()
}
func decodeClock(data []byte) (VersionVector, error) {
	decoded := decoder{b: data}
	clock, err := decoded.clock()
	if err != nil {
		return nil, err
	}
	return clock, decoded.done()
}
func encodeDigest(frontier VersionVector, digest [32]byte) []byte {
	var encoded encoder
	encoded.clock(frontier)
	encoded.raw(digest[:])
	return encoded.Bytes()
}
func decodeDigest(data []byte) (VersionVector, [32]byte, error) {
	var digest [32]byte
	decoded := decoder{b: data}
	frontier, err := decoded.clock()
	if err != nil {
		return nil, digest, err
	}
	raw, err := decoded.raw(len(digest))
	if err != nil {
		return nil, digest, err
	}
	copy(digest[:], raw)
	return frontier, digest, decoded.done()
}

func sameFrontier(left, right VersionVector) bool { return left.Compare(right) == ClockEqual }

type actorRange struct {
	actor       ActorID
	first, last uint64
}

func encodeActorRanges(ranges []actorRange) []byte {
	var encoded encoder
	encoded.u(uint64(len(ranges)))
	for _, interval := range ranges {
		encoded.id(ID(interval.actor))
		encoded.u(interval.first)
		encoded.u(interval.last)
	}
	return encoded.Bytes()
}

func decodeActorRanges(data []byte) ([]actorRange, error) {
	decoded := decoder{b: data}
	count, err := decoded.u()
	if err != nil || count > maxSyncRanges {
		return nil, ErrCorruption
	}
	ranges := make([]actorRange, 0, count)
	var previous ID
	for index := uint64(0); index < count; index++ {
		id, err := decoded.id()
		if err != nil || ID(id).IsZero() || (index > 0 && idCompare(previous, id) >= 0) {
			return nil, ErrCorruption
		}
		first, err := decoded.u()
		if err != nil || first == 0 {
			return nil, ErrCorruption
		}
		last, err := decoded.u()
		if err != nil || last < first || last-first >= maxSyncRangeSpan {
			return nil, ErrCorruption
		}
		ranges = append(ranges, actorRange{actor: ActorID(id), first: first, last: last})
		previous = id
	}
	return ranges, decoded.done()
}

// missingActorRanges describes the bounded contiguous suffix local needs from
// the directly connected actor. Direct-peer batches are signed by that actor,
// so requesting any other actor's history would be an unsupported relay.
func missingActorRanges(local, remote VersionVector, actor ActorID) ([]actorRange, error) {
	last := remote[actor]
	first := local[actor] + 1
	if first == 0 || first > last {
		return nil, nil
	}
	ranges := make([]actorRange, 0, 1)
	for first <= last {
		if len(ranges) == maxSyncRanges {
			return nil, ErrResourceLimit
		}
		end := first + maxSyncRangeSpan - 1
		if end < first || end > last {
			end = last
		}
		ranges = append(ranges, actorRange{actor: actor, first: first, last: end})
		if end == last {
			break
		}
		first = end + 1
	}
	return ranges, nil
}

// batchMaxHeap retains the earliest direct-actor transaction batches without
// materializing an unbounded matching history. Its root is the latest batch.
type batchMaxHeap []Batch

func (h batchMaxHeap) Len() int           { return len(h) }
func (h batchMaxHeap) Less(i, j int) bool { return h[i].First.Compare(h[j].First) > 0 }
func (h batchMaxHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *batchMaxHeap) Push(value any)    { *h = append(*h, value.(Batch)) }
func (h *batchMaxHeap) Pop() any {
	old := *h
	last := len(old) - 1
	value := old[last]
	*h = old[:last]
	return value
}

// batchesForRanges selects the earliest bounded, whole-batch response page for
// direct range reconciliation. The caller repeats a fresh authenticated round
// when more history remains; batches are never split or silently discarded.
func (db *DB) batchesForRanges(ranges []actorRange) ([]Batch, bool, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	selected := make(batchMaxHeap, 0, maxSyncResponseBatches+1)
	heap.Init(&selected)
	more := false
	for _, batch := range db.state.Batches {
		end := batch.First.Seq + uint64(batch.Count) - 1
		matches := false
		for _, interval := range ranges {
			if batch.First.Actor == interval.actor && batch.First.Seq <= interval.last && end >= interval.first {
				matches = true
				break
			}
		}
		if !matches {
			continue
		}
		if selected.Len() < maxSyncResponseBatches+1 {
			heap.Push(&selected, batch)
			continue
		}
		more = true
		if batch.First.Compare(selected[0].First) < 0 {
			selected[0] = batch
			heap.Fix(&selected, 0)
		}
	}
	batches := append([]Batch(nil), selected...)
	BatchSort(batches)
	responseBytes := 0
	pageEnd := len(batches)
	for index, batch := range batches {
		canonical, err := batch.MarshalBinary()
		if err != nil {
			return nil, false, err
		}
		if responseBytes+len(canonical) > maxSyncResponseBytes {
			pageEnd = index
			more = true
			break
		}
		responseBytes += len(canonical)
	}
	if pageEnd == 0 && len(batches) != 0 {
		return nil, false, ErrResourceLimit
	}
	if len(batches) > maxSyncResponseBatches {
		pageEnd = maxSyncResponseBatches
		more = true
	}
	return batches[:pageEnd], more, nil
}

func (db *DB) batchesMissing(remote VersionVector) []Batch {
	db.mu.RLock()
	defer db.mu.RUnlock()
	batches := make([]Batch, 0)
	for _, batch := range db.state.Batches {
		if remote[batch.First.Actor] < batch.First.Seq+uint64(batch.Count)-1 {
			batches = append(batches, batch)
		}
	}
	BatchSort(batches)
	return batches
}

// Sync dials one explicitly pinned peer and reconciles the union of retained
// operations. It never needs a central authority: both sides send their delta.
func (db *DB) Sync(ctx context.Context, peer PeerConfig) error {
	if peer.Address == "" || len(peer.PublicKey) != ed25519.PublicKeySize {
		return fmt.Errorf("%w: peer address/public key", ErrInvalidArgument)
	}
	if !db.peerTrusted(peer.PublicKey) {
		return ErrPeerUntrusted
	}
	identity, err := db.peerIdentity()
	if err != nil {
		return err
	}
	config, err := db.clientTLSConfig(identity, peer.PublicKey)
	if err != nil {
		return err
	}
	dialer := &net.Dialer{}
	connection, err := tls.DialWithDialer(dialer, "tcp", peer.Address, config)
	if err != nil {
		return err
	}
	defer connection.Close()
	return db.syncConnection(ctx, connection, identity, &peer.PublicKey)
}
func (db *DB) Connect(ctx context.Context, peer PeerConfig) error { return db.Sync(ctx, peer) }

// Serve accepts only peers previously added through TrustPeer. It is blocking
// by design; applications can run it in their own supervised goroutine.
func (db *DB) Serve(ctx context.Context, listen string) error {
	if listen == "" {
		return fmt.Errorf("%w: listen address", ErrInvalidArgument)
	}
	identity, err := db.peerIdentity()
	if err != nil {
		return err
	}
	config, err := db.serverTLSConfig(identity)
	if err != nil {
		return err
	}
	listener, err := tls.Listen("tcp", listen, config)
	if err != nil {
		return err
	}
	defer listener.Close()
	go func() { <-ctx.Done(); _ = listener.Close() }()
	for {
		connection, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		go func(connection net.Conn) {
			defer connection.Close()
			tlsConnection, ok := connection.(*tls.Conn)
			if !ok {
				return
			}
			_ = tlsConnection.HandshakeContext(ctx)
			_ = db.syncConnection(ctx, tlsConnection, identity, nil)
		}(connection)
	}
}

func (db *DB) syncConnection(ctx context.Context, connection *tls.Conn, identity peerIdentity, expected *ed25519.PublicKey) error {
	if err := connection.HandshakeContext(ctx); err != nil {
		return err
	}
	deadline := time.Now().Add(60 * time.Second)
	_ = connection.SetDeadline(deadline)
	defer connection.SetDeadline(time.Time{})
	reader := bufio.NewReader(connection)
	status, err := db.Status()
	if err != nil {
		return err
	}
	localHello := syncHello{database: status.DatabaseID, actor: status.ActorID, public: identity.public}
	if err := writeWireFrame(connection, wireHello, localHello.encode()); err != nil {
		return err
	}
	kind, payload, err := readWireFrame(reader)
	if err != nil {
		return err
	}
	if kind != wireHello {
		return ErrCorruption
	}
	remoteHello, err := decodeHello(payload)
	if err != nil {
		return err
	}
	if remoteHello.database != status.DatabaseID {
		return fmt.Errorf("%w: database identity mismatch", ErrInvalidArgument)
	}
	if expected != nil && string(remoteHello.public) != string(*expected) {
		return fmt.Errorf("%w: peer identity mismatch", ErrInvalidArgument)
	}
	if expected == nil && !db.peerTrusted(remoteHello.public) {
		return ErrPeerUntrusted
	}
	state := connection.ConnectionState()
	if len(state.PeerCertificates) != 1 {
		return fmt.Errorf("%w: TLS certificate count", ErrCorruption)
	}
	authenticatedKey, err := publicFromRawCertificate(state.PeerCertificates[0].Raw)
	if err != nil {
		return err
	}
	if string(authenticatedKey) != string(remoteHello.public) {
		return fmt.Errorf("%w: hello/TLS key mismatch", ErrAuthorizationDenied)
	}
	if !db.actorBoundToKey(remoteHello.actor, authenticatedKey) {
		return fmt.Errorf("%w: peer actor is not explicitly bound to its key", ErrAuthorizationDenied)
	}

	sent := 0
	rounds := 0
round:
	if rounds == maxSyncRounds {
		return ErrResourceLimit
	}
	rounds++
	status, err = db.Status()
	if err != nil {
		return err
	}
	if err := writeWireFrame(connection, wireClock, encodeClock(status.Frontier)); err != nil {
		return err
	}
	kind, payload, err = readWireFrame(reader)
	if err != nil {
		return err
	}
	if kind != wireClock {
		return ErrCorruption
	}
	remoteClock, err := decodeClock(payload)
	if err != nil {
		return err
	}
	requestedRanges, err := missingActorRanges(status.Frontier, remoteClock, remoteHello.actor)
	if err != nil {
		return err
	}
	if err := writeWireFrame(connection, wireRanges, encodeActorRanges(requestedRanges)); err != nil {
		return err
	}
	kind, payload, err = readWireFrame(reader)
	if err != nil {
		return err
	}
	if kind != wireRanges {
		return ErrCorruption
	}
	remoteRanges, err := decodeActorRanges(payload)
	if err != nil {
		return err
	}
	for _, interval := range remoteRanges {
		if interval.actor != status.ActorID {
			return fmt.Errorf("%w: direct peer requested a foreign actor range", ErrAuthorizationDenied)
		}
	}
	outgoing, _, err := db.batchesForRanges(remoteRanges)
	if err != nil {
		return err
	}
	if len(remoteRanges) != 0 && len(outgoing) == 0 {
		return fmt.Errorf("%w: requested direct actor range is unavailable", ErrCausalGap)
	}
	writeErr := make(chan error, 1)
	go func() {
		for _, batch := range outgoing {
			payload, err := signBatch(identity, status.DatabaseID, batch)
			if err != nil {
				writeErr <- err
				return
			}
			if err := writeWireFrame(connection, wireBatch, payload); err != nil {
				writeErr <- err
				return
			}
		}
		writeErr <- writeWireFrame(connection, wireDone, nil)
	}()
	pending := make(map[Dot]Batch)
	pendingBytes := 0
	admit := func(batch Batch) error {
		if err := db.applyRemoteFromPeer(ctx, batch, remoteHello.actor, authenticatedKey); errors.Is(err, ErrCausalGap) {
			if _, exists := pending[batch.First]; !exists {
				raw, marshalErr := batch.MarshalBinary()
				if marshalErr != nil {
					return marshalErr
				}
				if len(pending) >= maxPendingBatches || pendingBytes+len(raw) > maxPendingBytes {
					return ErrResourceLimit
				}
				pending[batch.First] = batch
				pendingBytes += len(raw)
			}
			return nil
		} else {
			return err
		}
	}
	for {
		kind, payload, err = readWireFrame(reader)
		if err != nil {
			return err
		}
		switch kind {
		case wireBatch:
			batch, err := decodeSignedBatch(payload, remoteHello.public, status.DatabaseID)
			if err != nil {
				return err
			}
			if err := admit(batch); err != nil {
				return err
			}
			for progressed := true; progressed && len(pending) > 0; {
				progressed = false
				for first, held := range pending {
					if err := db.applyRemoteFromPeer(ctx, held, remoteHello.actor, authenticatedKey); errors.Is(err, ErrCausalGap) {
						continue
					} else if err != nil {
						return err
					}
					raw, marshalErr := held.MarshalBinary()
					if marshalErr != nil {
						return marshalErr
					}
					delete(pending, first)
					pendingBytes -= len(raw)
					progressed = true
				}
			}
		case wireDone:
			if err := <-writeErr; err != nil {
				return err
			}
			if len(pending) != 0 {
				return fmt.Errorf("%w: unresolved remote dependency", ErrCausalGap)
			}
			sent += len(outgoing)
			if len(requestedRanges) == 0 && len(remoteRanges) == 0 {
				goto exchangeDigest
			}
			goto round
		case wireError:
			return fmt.Errorf("peer error: %s", string(payload))
		default:
			return ErrCorruption
		}
	}
exchangeDigest:
	final, err := db.Status()
	if err != nil {
		return err
	}
	if err := writeWireFrame(connection, wireDigest, encodeDigest(final.Frontier, final.LogicalDigest)); err != nil {
		return err
	}
	kind, payload, err = readWireFrame(reader)
	if err != nil {
		return err
	}
	if kind != wireDigest {
		return ErrCorruption
	}
	remoteFrontier, remoteDigest, err := decodeDigest(payload)
	if err != nil {
		return err
	}
	if !sameFrontier(final.Frontier, remoteFrontier) {
		return fmt.Errorf("%w: peer frontier differs after sync", ErrCausalGap)
	}
	if final.LogicalDigest != remoteDigest {
		return fmt.Errorf("%w: peer logical digest differs", ErrCorruption)
	}
	db.logger.Info("sync.converged", "peer", peerFingerprint(remoteHello.public), "operations_sent", sent, "rounds", rounds)
	return nil
}

// SyncPair is an in-process integration helper that synchronizes two DBs over
// a loopback TLS connection without external services.
func SyncPair(ctx context.Context, left, right *DB) error {
	leftKey, err := left.PeerPublicKey()
	if err != nil {
		return err
	}
	rightKey, err := right.PeerPublicKey()
	if err != nil {
		return err
	}
	if err := left.TrustPeer("right", rightKey); err != nil {
		return err
	}
	if err := right.TrustPeer("left", leftKey); err != nil {
		return err
	}
	if err := left.BindPeerActor(right.ActorID(), rightKey); err != nil {
		return err
	}
	if err := right.BindPeerActor(left.ActorID(), leftKey); err != nil {
		return err
	}
	leftIdentity, err := left.peerIdentity()
	if err != nil {
		return err
	}
	rightIdentity, err := right.peerIdentity()
	if err != nil {
		return err
	}
	leftConfig, err := left.clientTLSConfig(leftIdentity, rightKey)
	if err != nil {
		return err
	}
	rightConfig, err := right.serverTLSConfig(rightIdentity)
	if err != nil {
		return err
	}
	listener, err := tls.Listen("tcp", "127.0.0.1:0", rightConfig)
	if err != nil {
		return err
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
		tlsConnection := connection.(*tls.Conn)
		serverResult <- right.syncConnection(ctx, tlsConnection, rightIdentity, nil)
	}()
	connection, err := tls.Dial("tcp", listener.Addr().String(), leftConfig)
	if err != nil {
		return err
	}
	clientResult := left.syncConnection(ctx, connection, leftIdentity, &rightKey)
	_ = connection.Close()
	serverErr := <-serverResult
	if clientResult != nil {
		return clientResult
	}
	return serverErr
}
