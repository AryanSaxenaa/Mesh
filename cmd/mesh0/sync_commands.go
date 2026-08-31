package main

import (
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"os/signal"

	"github.com/mesh0/mesh0"
)

func parsePeerKey(encoded string) (ed25519.PublicKey, error) {
	bytes, err := hex.DecodeString(encoded)
	if err != nil || len(bytes) != ed25519.PublicKeySize {
		return nil, fmt.Errorf("%w: Ed25519 peer key must be %d hex bytes", mesh0.ErrInvalidArgument, ed25519.PublicKeySize)
	}
	return ed25519.PublicKey(bytes), nil
}

func parsePeerActor(encoded string) (mesh0.ActorID, error) {
	return mesh0.ParseActorID(encoded)
}
func peerCommand(args []string, out io.Writer) error {
	if len(args) < 2 {
		return fmt.Errorf("%w: peer identity|add|remove|grant|revoke|list", mesh0.ErrInvalidArgument)
	}
	db, err := open(args[1])
	if err != nil {
		return err
	}
	defer db.Close()
	switch args[0] {
	case "identity":
		if len(args) != 2 {
			return mesh0.ErrInvalidArgument
		}
		key, err := db.PeerPublicKey()
		if err != nil {
			return err
		}
		_, err = fmt.Fprintln(out, hex.EncodeToString(key))
		return err
	case "add":
		if len(args) != 5 {
			return fmt.Errorf("%w: peer add PATH NAME ACTOR_ID PUBLIC_KEY", mesh0.ErrInvalidArgument)
		}
		actor, err := parsePeerActor(args[3])
		if err != nil {
			return err
		}
		key, err := parsePeerKey(args[4])
		if err != nil {
			return err
		}
		if err := db.TrustAndBindPeerActor(args[2], actor, key); err != nil {
			return err
		}
		_, err = fmt.Fprintln(out, "peer trusted and actor authorized")
		return err
	case "remove":
		if len(args) != 3 {
			return fmt.Errorf("%w: peer remove PATH PUBLIC_KEY", mesh0.ErrInvalidArgument)
		}
		key, err := parsePeerKey(args[2])
		if err != nil {
			return err
		}
		if err := db.UntrustPeer(key); err != nil {
			return err
		}
		_, err = fmt.Fprintln(out, "peer trust removed")
		return err
	case "grant", "revoke":
		if len(args) != 4 {
			return fmt.Errorf("%w: peer %s PATH ACTOR_ID COLLECTION", mesh0.ErrInvalidArgument, args[0])
		}
		actor, err := parsePeerActor(args[2])
		if err != nil {
			return err
		}
		verb := "granted"
		if args[0] == "grant" {
			err = db.GrantPeerCollectionWrite(actor, args[3])
		} else {
			err = db.RevokePeerCollectionWrite(actor, args[3])
			verb = "revoked"
		}
		if err != nil {
			return err
		}
		_, err = fmt.Fprintf(out, "peer collection write %s\n", verb)
		return err
	case "list":
		if len(args) != 2 {
			return mesh0.ErrInvalidArgument
		}
		peers, err := db.TrustedPeers()
		if err != nil {
			return err
		}
		for _, peer := range peers {
			if _, err := fmt.Fprintf(out, "%s %s\n", peer.Name, peer.Fingerprint); err != nil {
				return err
			}
		}
		return nil
	default:
		return fmt.Errorf("%w: unknown peer subcommand", mesh0.ErrInvalidArgument)
	}
}
func syncCommand(ctx context.Context, args []string, out io.Writer) error {
	if len(args) < 3 || len(args) > 4 {
		return fmt.Errorf("%w: sync PATH ADDRESS PUBLIC_KEY [NAME]", mesh0.ErrInvalidArgument)
	}
	key, err := parsePeerKey(args[2])
	if err != nil {
		return err
	}
	db, err := open(args[0])
	if err != nil {
		return err
	}
	defer db.Close()
	name := "peer"
	if len(args) == 4 {
		name = args[3]
	}
	if err := db.Sync(ctx, mesh0.PeerConfig{Name: name, Address: args[1], PublicKey: key}); err != nil {
		return err
	}
	_, err = fmt.Fprintln(out, "synchronized")
	return err
}
func serveCommand(ctx context.Context, args []string, out io.Writer) error {
	if len(args) != 3 || args[1] != "--listen" {
		return fmt.Errorf("%w: serve PATH --listen ADDRESS", mesh0.ErrInvalidArgument)
	}
	db, err := open(args[0])
	if err != nil {
		return err
	}
	defer db.Close()
	serveContext, stop := signal.NotifyContext(ctx, os.Interrupt)
	defer stop()
	if _, err := fmt.Fprintf(out, "serving %s\n", args[2]); err != nil {
		return err
	}
	return db.Serve(serveContext, args[2])
}
