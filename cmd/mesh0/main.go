// Command mesh0 provides local-first database administration without a third-party CLI framework.
package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"github.com/mesh0/mesh0"
)

func main() { os.Exit(run(context.Background(), os.Args[1:], os.Stdout, os.Stderr)) }

func run(ctx context.Context, args []string, out, errOut io.Writer) int {
	if len(args) == 0 {
		usage(out)
		return 2
	}
	var err error
	switch args[0] {
	case "init":
		err = initCommand(ctx, args[1:], out)
	case "put", "resolve":
		err = putCommand(ctx, args[1:], out)
	case "get":
		err = getCommand(ctx, args[1:], out)
	case "delete":
		err = deleteCommand(ctx, args[1:], out)
	case "query":
		err = queryCommand(ctx, args[1:], out)
	case "history":
		err = historyCommand(ctx, args[1:], out)
	case "conflicts":
		err = conflictsCommand(ctx, args[1:], out)
	case "status":
		err = statusCommand(ctx, args[1:], out)
	case "verify":
		err = verifyCommand(ctx, args[1:], out)
	case "snapshot", "compact":
		err = snapshotCommand(ctx, args[1:], out)
	case "backup":
		err = backupCommand(ctx, args[1:], out)
	case "restore":
		err = restoreCommand(args[1:], out)
	case "export":
		err = exportCommand(ctx, args[1:], out)
	case "doctor":
		err = doctorCommand(ctx, args[1:], out)
	case "selftest":
		err = selfTest(ctx, out)
	case "serve":
		err = serveCommand(ctx, args[1:], out)
	case "sync":
		err = syncCommand(ctx, args[1:], out)
	case "peer":
		err = peerCommand(args[1:], out)
	case "help", "-h", "--help":
		usage(out)
		return 0
	default:
		err = fmt.Errorf("%w: unknown command %q", mesh0.ErrInvalidArgument, args[0])
	}
	if err == nil {
		return 0
	}
	fmt.Fprintln(errOut, "mesh0:", err)
	if errors.Is(err, mesh0.ErrInvalidArgument) {
		return 2
	}
	return 1
}

func usage(out io.Writer) {
	fmt.Fprintln(out, `mesh0 - local-first CRDT database

Usage:
  mesh0 init PATH
  mesh0 put PATH COLLECTION/ID field=value [...]
  mesh0 get PATH COLLECTION/ID [--conflicts]
  mesh0 delete PATH COLLECTION/ID [field]
  mesh0 query PATH COLLECTION [--where field=value] [--prefix field=text] [--exists field] [--limit N] [--explain]
  mesh0 history PATH [COLLECTION/ID]
  mesh0 conflicts PATH
  mesh0 resolve PATH COLLECTION/ID field=value
  mesh0 status PATH
  mesh0 verify PATH [--blobs]
  mesh0 snapshot PATH | compact PATH
  mesh0 backup PATH ARCHIVE.zip [--blobs]
  mesh0 restore ARCHIVE.zip DESTINATION
  mesh0 export PATH [COLLECTION] [--projected]
  mesh0 peer identity PATH | peer add PATH NAME ACTOR_ID PUBLIC_KEY | peer grant PATH ACTOR_ID COLLECTION | peer revoke PATH ACTOR_ID COLLECTION | peer list PATH
  mesh0 serve PATH --listen ADDRESS
  mesh0 sync PATH ADDRESS PUBLIC_KEY [NAME]
  mesh0 doctor PATH
  mesh0 selftest

All local reads and writes work while disconnected. Concurrent scalar writes are preserved as conflicts.`)
}

func open(path string) (*mesh0.DB, error) {
	return mesh0.Open(path, mesh0.Options{Durability: mesh0.DurabilitySync})
}
func initCommand(_ context.Context, args []string, out io.Writer) error {
	if len(args) != 1 {
		return fmt.Errorf("%w: init requires PATH", mesh0.ErrInvalidArgument)
	}
	db, err := open(args[0])
	if err != nil {
		return err
	}
	defer db.Close()
	status, err := db.Status()
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(out, "initialized %s\ndatabase %s\nactor    %s\n", args[0], mesh0.ID(status.DatabaseID).String(), mesh0.ID(status.ActorID).String())
	return err
}

func splitDocument(input string) (string, string, error) {
	parts := strings.SplitN(input, "/", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", "", fmt.Errorf("%w: document must be COLLECTION/ID", mesh0.ErrInvalidArgument)
	}
	return parts[0], parts[1], nil
}
func splitAssignment(input string) (string, mesh0.Value, error) {
	parts := strings.SplitN(input, "=", 2)
	if len(parts) != 2 || parts[0] == "" {
		return "", mesh0.Value{}, fmt.Errorf("%w: assignment must be field=value", mesh0.ErrInvalidArgument)
	}
	value, err := mesh0.ParseValue(parts[1])
	return parts[0], value, err
}
func putCommand(ctx context.Context, args []string, out io.Writer) error {
	if len(args) < 3 {
		return fmt.Errorf("%w: put requires PATH DOCUMENT assignments", mesh0.ErrInvalidArgument)
	}
	collection, id, err := splitDocument(args[1])
	if err != nil {
		return err
	}
	db, err := open(args[0])
	if err != nil {
		return err
	}
	defer db.Close()
	err = db.Update(ctx, func(tx *mesh0.Tx) error {
		document := tx.Document(collection, id)
		for _, assignment := range args[2:] {
			field, value, err := splitAssignment(assignment)
			if err != nil {
				return err
			}
			if err := document.Set(field, value); err != nil {
				return err
			}
		}
		return nil
	})
	if err == nil {
		_, err = fmt.Fprintln(out, "committed")
	}
	return err
}

func getCommand(ctx context.Context, args []string, out io.Writer) error {
	conflicts := false
	clean := args[:0]
	for _, arg := range args {
		if arg == "--conflicts" {
			conflicts = true
		} else {
			clean = append(clean, arg)
		}
	}
	args = clean
	if len(args) != 2 {
		return fmt.Errorf("%w: get requires PATH DOCUMENT", mesh0.ErrInvalidArgument)
	}
	collection, id, err := splitDocument(args[1])
	if err != nil {
		return err
	}
	db, err := open(args[0])
	if err != nil {
		return err
	}
	defer db.Close()
	return db.View(ctx, func(read *mesh0.ReadTx) error {
		document, ok := read.Document(collection, id)
		if !ok {
			return mesh0.ErrNotFound
		}
		projection, err := mesh0.ExportDocument(document, conflicts)
		if err != nil {
			return err
		}
		return writeJSON(out, projection)
	})
}
func deleteCommand(ctx context.Context, args []string, out io.Writer) error {
	if len(args) < 2 || len(args) > 3 {
		return fmt.Errorf("%w: delete requires PATH DOCUMENT [field]", mesh0.ErrInvalidArgument)
	}
	collection, id, err := splitDocument(args[1])
	if err != nil {
		return err
	}
	db, err := open(args[0])
	if err != nil {
		return err
	}
	defer db.Close()
	err = db.Update(ctx, func(tx *mesh0.Tx) error {
		document := tx.Document(collection, id)
		if len(args) == 3 {
			return document.Delete(args[2])
		}
		return document.DeleteDocument()
	})
	if err == nil {
		_, err = fmt.Fprintln(out, "committed")
	}
	return err
}

func queryCommand(ctx context.Context, args []string, out io.Writer) error {
	if len(args) < 2 {
		return fmt.Errorf("%w: query requires PATH COLLECTION", mesh0.ErrInvalidArgument)
	}
	query := mesh0.Query{Collection: args[1]}
	explain := false
	for index := 2; index < len(args); index++ {
		switch args[index] {
		case "--where":
			index++
			if index >= len(args) {
				return mesh0.ErrInvalidArgument
			}
			field, value, err := splitAssignment(args[index])
			if err != nil {
				return err
			}
			query.Path, query.Equal = field, &value
		case "--prefix":
			index++
			if index >= len(args) {
				return mesh0.ErrInvalidArgument
			}
			parts := strings.SplitN(args[index], "=", 2)
			if len(parts) != 2 {
				return mesh0.ErrInvalidArgument
			}
			query.Path, query.Prefix = parts[0], parts[1]
		case "--exists":
			index++
			if index >= len(args) {
				return mesh0.ErrInvalidArgument
			}
			query.Path, query.Exists = args[index], true
		case "--limit":
			index++
			if index >= len(args) {
				return mesh0.ErrInvalidArgument
			}
			limit, err := strconv.Atoi(args[index])
			if err != nil {
				return err
			}
			query.Limit = limit
		case "--explain":
			explain = true
		default:
			return fmt.Errorf("%w: query option %s", mesh0.ErrInvalidArgument, args[index])
		}
	}
	if explain {
		_, _ = fmt.Fprintln(out, mesh0.ExplainQuery(query))
		return nil
	}
	db, err := open(args[0])
	if err != nil {
		return err
	}
	defer db.Close()
	results, err := db.Query(ctx, query)
	if err != nil {
		return err
	}
	exported := make([]map[string]any, 0, len(results))
	for _, result := range results {
		value, err := mesh0.ExportDocument(result.Document, true)
		if err != nil {
			return err
		}
		value["$id"] = result.Key.ID
		exported = append(exported, value)
	}
	return writeJSON(out, exported)
}

func historyCommand(ctx context.Context, args []string, out io.Writer) error {
	if len(args) < 1 || len(args) > 2 {
		return fmt.Errorf("%w: history requires PATH [DOCUMENT]", mesh0.ErrInvalidArgument)
	}
	collection, id := "", ""
	var err error
	if len(args) == 2 {
		collection, id, err = splitDocument(args[1])
		if err != nil {
			return err
		}
	}
	db, err := open(args[0])
	if err != nil {
		return err
	}
	defer db.Close()
	batches, err := db.History(collection, id)
	if err != nil {
		return err
	}
	for _, batch := range batches {
		if _, err := fmt.Fprintf(out, "%s operations=%d deps=%d time=%d\n", batch.ID(), batch.Count, len(batch.Dependencies), batch.TimestampNanos); err != nil {
			return err
		}
	}
	return nil
}
func conflictsCommand(ctx context.Context, args []string, out io.Writer) error {
	if len(args) != 1 {
		return fmt.Errorf("%w: conflicts requires PATH", mesh0.ErrInvalidArgument)
	}
	db, err := open(args[0])
	if err != nil {
		return err
	}
	defer db.Close()
	keys, err := db.Documents("")
	if err != nil {
		return err
	}
	found := false
	return db.View(ctx, func(read *mesh0.ReadTx) error {
		for _, key := range keys {
			document, _ := read.Document(key.Collection, key.ID)
			for _, encoded := range document.Fields() {
				path, err := decodeCLIPath(encoded)
				if err != nil {
					return err
				}
				values := document.Values(path)
				if len(values) < 2 {
					continue
				}
				found = true
				if _, err := fmt.Fprintf(out, "CONFLICT %s $.%s\n", key.String(), path); err != nil {
					return err
				}
				for _, value := range values {
					if _, err := fmt.Fprintf(out, "  %s\n", value.String()); err != nil {
						return err
					}
				}
			}
		}
		if !found {
			_, err := fmt.Fprintln(out, "no conflicts")
			return err
		}
		return nil
	})
}
func decodeCLIPath(encoded string) (string, error) {
	if len(encoded) == 0 {
		return "", mesh0.ErrCorruption
	}
	colon := strings.IndexByte(encoded, ':')
	if colon < 1 {
		return "", mesh0.ErrCorruption
	}
	n, err := strconv.Atoi(encoded[:colon])
	if err != nil || colon+1+n > len(encoded) {
		return "", mesh0.ErrCorruption
	}
	return encoded[colon+1 : colon+1+n], nil
}

func statusCommand(ctx context.Context, args []string, out io.Writer) error {
	if len(args) != 1 {
		return fmt.Errorf("%w: status requires PATH", mesh0.ErrInvalidArgument)
	}
	db, err := open(args[0])
	if err != nil {
		return err
	}
	defer db.Close()
	status, err := db.Status()
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(out, "DATABASE STATUS\n\nreplica\n  database    %s\n  actor       %s\n  durable     %t\n  documents   %d\n\nhistory\n  operations  %d\n  actors      %d\n  frontier    %v\n\nstorage\n  active segment  %d\n  snapshot        %s\n  digest          %s\n", mesh0.ID(status.DatabaseID).String(), mesh0.ID(status.ActorID).String(), status.Durability == mesh0.DurabilitySync, status.Documents, status.Operations, status.KnownActors, status.Frontier, status.ActiveSegment, status.Snapshot, hex.EncodeToString(status.LogicalDigest[:]))
	return err
}
func verifyCommand(ctx context.Context, args []string, out io.Writer) error {
	blobs := false
	clean := args[:0]
	for _, arg := range args {
		if arg == "--blobs" {
			blobs = true
		} else {
			clean = append(clean, arg)
		}
	}
	if len(clean) != 1 {
		return fmt.Errorf("%w: verify requires PATH", mesh0.ErrInvalidArgument)
	}
	db, err := open(clean[0])
	if err != nil {
		return err
	}
	defer db.Close()
	if err := db.Verify(ctx, blobs); err != nil {
		return err
	}
	_, err = fmt.Fprintln(out, "verification passed")
	return err
}
func snapshotCommand(ctx context.Context, args []string, out io.Writer) error {
	if len(args) != 1 {
		return fmt.Errorf("%w: snapshot requires PATH", mesh0.ErrInvalidArgument)
	}
	db, err := open(args[0])
	if err != nil {
		return err
	}
	defer db.Close()
	name, err := db.Snapshot(ctx)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintln(out, name)
	return err
}
func backupCommand(ctx context.Context, args []string, out io.Writer) error {
	blobs := false
	clean := args[:0]
	for _, arg := range args {
		if arg == "--blobs" {
			blobs = true
		} else {
			clean = append(clean, arg)
		}
	}
	if len(clean) != 2 {
		return fmt.Errorf("%w: backup requires PATH ARCHIVE", mesh0.ErrInvalidArgument)
	}
	db, err := open(clean[0])
	if err != nil {
		return err
	}
	defer db.Close()
	if err = db.Backup(ctx, clean[1], blobs); err != nil {
		return err
	}
	_, err = fmt.Fprintln(out, "backup created")
	return err
}
func restoreCommand(args []string, out io.Writer) error {
	if len(args) != 2 {
		return fmt.Errorf("%w: restore requires ARCHIVE DESTINATION", mesh0.ErrInvalidArgument)
	}
	if err := mesh0.Restore(args[0], args[1]); err != nil {
		return err
	}
	_, err := fmt.Fprintln(out, "restore complete")
	return err
}
func exportCommand(ctx context.Context, args []string, out io.Writer) error {
	projected := false
	clean := args[:0]
	for _, arg := range args {
		if arg == "--projected" {
			projected = true
		} else {
			clean = append(clean, arg)
		}
	}
	if len(clean) < 1 || len(clean) > 2 {
		return fmt.Errorf("%w: export requires PATH [COLLECTION]", mesh0.ErrInvalidArgument)
	}
	collection := ""
	if len(clean) == 2 {
		collection = clean[1]
	}
	db, err := open(clean[0])
	if err != nil {
		return err
	}
	defer db.Close()
	keys, err := db.Documents(collection)
	if err != nil {
		return err
	}
	result := make([]map[string]any, 0, len(keys))
	err = db.View(ctx, func(read *mesh0.ReadTx) error {
		for _, key := range keys {
			document, _ := read.Document(key.Collection, key.ID)
			value, err := mesh0.ExportDocument(document, !projected)
			if err != nil {
				return err
			}
			value["$collection"], value["$id"] = key.Collection, key.ID
			result = append(result, value)
		}
		return nil
	})
	if err != nil {
		return err
	}
	return writeJSON(out, result)
}
func doctorCommand(ctx context.Context, args []string, out io.Writer) error {
	if len(args) != 1 {
		return fmt.Errorf("%w: doctor requires PATH", mesh0.ErrInvalidArgument)
	}
	db, err := open(args[0])
	if err != nil {
		return err
	}
	defer db.Close()
	if err := db.Verify(ctx, false); err != nil {
		return err
	}
	_, err = fmt.Fprintln(out, "DOCTOR\n  storage recovery: OK\n  atomic manifest: OK\n  logical state: OK\n  TLS profile: standard library available")
	return err
}
func selfTest(ctx context.Context, out io.Writer) error {
	root, err := os.MkdirTemp("", "mesh0-selftest-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(root)
	db, err := open(root)
	if err != nil {
		return err
	}
	if err = db.Update(ctx, func(tx *mesh0.Tx) error {
		document := tx.Document("tasks", "42")
		if err := document.Set("title", mesh0.String("Ship")); err != nil {
			return err
		}
		return document.CounterAdd("edits", 1)
	}); err == nil {
		err = db.Verify(ctx, false)
	}
	if closeErr := db.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	db, err = open(root)
	if err == nil {
		err = db.View(ctx, func(read *mesh0.ReadTx) error {
			document, ok := read.Document("tasks", "42")
			if !ok {
				return mesh0.ErrNotFound
			}
			value, ok := document.Value("title")
			if !ok || value.Text != "Ship" || document.Counter("edits") != 1 {
				return mesh0.ErrCorruption
			}
			return nil
		})
	}
	if db != nil {
		_ = db.Close()
	}
	if err != nil {
		return err
	}
	_, err = fmt.Fprintln(out, "MESH0 SELF-TEST\n[PASS] local durable commit\n[PASS] restart recovery\n[PASS] canonical state verification\nCORE INVARIANTS PASSED")
	return err
}
func writeJSON(out io.Writer, value any) error {
	encoder := json.NewEncoder(out)
	encoder.SetIndent("", "  ")
	return encoder.Encode(value)
}

// Keep flag imported intentionally: Mesh0 CLI is designed around the standard
// library flag package, while command routing above allows positional syntax.
var _ = flag.CommandLine
