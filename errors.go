// Package mesh0 is a standard-library-only, local-first embedded CRDT database.
package mesh0

import "errors"

var (
	ErrInvalidArgument      = errors.New("mesh0: invalid argument")
	ErrNotFound             = errors.New("mesh0: not found")
	ErrConflict             = errors.New("mesh0: conflict present")
	ErrCorruption           = errors.New("mesh0: corruption")
	ErrDurability           = errors.New("mesh0: durability failure")
	ErrResourceLimit        = errors.New("mesh0: resource limit")
	ErrActorFork            = errors.New("mesh0: actor sequence fork")
	ErrClosed               = errors.New("mesh0: database closed")
	ErrLocked               = errors.New("mesh0: database is locked")
	ErrCausalGap            = errors.New("mesh0: causal dependency missing")
	ErrRecoveryRequired     = errors.New("mesh0: recovery required")
	ErrAuthorizationDenied  = errors.New("mesh0: authorization denied")
	ErrPeerUntrusted        = errors.New("mesh0: peer untrusted")
	ErrInvalidSignature     = errors.New("mesh0: invalid signature")
	ErrProtocolIncompatible = errors.New("mesh0: protocol incompatible")
	ErrFeatureUnavailable   = errors.New("mesh0: feature unavailable")
)
