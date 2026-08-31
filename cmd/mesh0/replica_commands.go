package main

import (
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/mesh0/mesh0"
)

// replicaCommand prepares independent writable replicas that retain the same
// database identity and causal history. Freshly initialized databases are
// deliberately separate databases and must not be connected with sync.
func replicaCommand(ctx context.Context, args []string, out io.Writer) error {
	if len(args) == 2 && args[0] == "rotate" {
		return replicaRotate(args[1], out)
	}

	includeBlobs := false
	clean := args[:0]
	for _, arg := range args {
		if arg == "--blobs" {
			includeBlobs = true
		} else {
			clean = append(clean, arg)
		}
	}
	if len(clean) != 3 || clean[0] != "create" {
		return fmt.Errorf("%w: replica create SOURCE DESTINATION [--blobs] | replica rotate PATH", mesh0.ErrInvalidArgument)
	}
	return replicaCreate(ctx, clean[1], clean[2], includeBlobs, out)
}

func replicaCreate(ctx context.Context, source, destination string, includeBlobs bool, out io.Writer) error {
	source = filepath.Clean(source)
	destination = filepath.Clean(destination)
	if source == destination {
		return fmt.Errorf("%w: replica source and destination must be different paths", mesh0.ErrInvalidArgument)
	}
	if _, err := os.Lstat(destination); err == nil {
		return fmt.Errorf("%w: replica destination must not already exist", mesh0.ErrInvalidArgument)
	} else if !os.IsNotExist(err) {
		return err
	}

	archive, err := os.CreateTemp("", "mesh0-replica-*.zip")
	if err != nil {
		return err
	}
	archivePath := archive.Name()
	if err := archive.Close(); err != nil {
		_ = os.Remove(archivePath)
		return err
	}
	// Backup requires a new archive path, so remove the placeholder created by
	// CreateTemp before asking Mesh0 to create the checked archive.
	if err := os.Remove(archivePath); err != nil {
		return err
	}
	defer os.Remove(archivePath)

	db, err := open(source)
	if err != nil {
		return err
	}
	if err := db.Backup(ctx, archivePath, includeBlobs); err != nil {
		_ = db.Close()
		return err
	}
	if err := db.Close(); err != nil {
		return err
	}

	parent := filepath.Dir(destination)
	staging, err := os.MkdirTemp(parent, ".mesh0-replica-")
	if err != nil {
		return err
	}
	committed := false
	defer func() {
		if !committed {
			_ = os.RemoveAll(staging)
		}
	}()
	if err := mesh0.Restore(archivePath, staging); err != nil {
		return err
	}
	actor, key, err := rotateReplica(staging)
	if err != nil {
		return err
	}
	if err := os.Rename(staging, destination); err != nil {
		return err
	}
	committed = true
	_, err = fmt.Fprintf(out, "replica created %s\nactor    %s\npublic key %s\n", destination, mesh0.ID(actor).String(), hex.EncodeToString(key))
	return err
}

func replicaRotate(path string, out io.Writer) error {
	actor, key, err := rotateReplica(path)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(out, "replica identity rotated\nactor    %s\npublic key %s\n", mesh0.ID(actor).String(), hex.EncodeToString(key))
	return err
}

func rotateReplica(path string) (mesh0.ActorID, []byte, error) {
	db, err := open(path)
	if err != nil {
		return mesh0.ActorID{}, nil, err
	}
	defer db.Close()
	actor, err := db.RotateActor()
	if err != nil {
		return mesh0.ActorID{}, nil, err
	}
	key, err := db.PeerPublicKey()
	if err != nil {
		return mesh0.ActorID{}, nil, err
	}
	return actor, key, nil
}
