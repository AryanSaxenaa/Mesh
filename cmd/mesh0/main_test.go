package main

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mesh0/mesh0"
)

// runCLI invokes the same entry point as main() against isolated buffers so
// argument parsing, exit codes, and stdout/stderr routing are verified without
// spawning a subprocess.
func runCLI(t *testing.T, args ...string) (stdout, stderr string, code int) {
	t.Helper()
	var out, errOut bytes.Buffer
	code = run(context.Background(), args, &out, &errOut)
	return out.String(), errOut.String(), code
}

func TestRunNoArgsPrintsUsageAndExitsTwo(t *testing.T) {
	stdout, stderr, code := runCLI(t)
	if code != 2 {
		t.Fatalf("exit code = %d, want 2", code)
	}
	if !strings.Contains(stdout, "mesh0 - local-first CRDT database") {
		t.Fatalf("stdout missing usage banner: %q", stdout)
	}
	if stderr != "" {
		t.Fatalf("stderr = %q, want empty", stderr)
	}
}

func TestRunHelpExitsZero(t *testing.T) {
	for _, flag := range []string{"help", "-h", "--help"} {
		stdout, stderr, code := runCLI(t, flag)
		if code != 0 {
			t.Fatalf("%s: exit code = %d, want 0", flag, code)
		}
		if !strings.Contains(stdout, "Usage:") {
			t.Fatalf("%s: stdout missing usage body: %q", flag, stdout)
		}
		if stderr != "" {
			t.Fatalf("%s: stderr = %q, want empty", flag, stderr)
		}
	}
}

func TestRunUnknownCommandExitsTwoAndWritesStderr(t *testing.T) {
	stdout, stderr, code := runCLI(t, "not-a-command")
	if code != 2 {
		t.Fatalf("exit code = %d, want 2", code)
	}
	if stdout != "" {
		t.Fatalf("stdout = %q, want empty", stdout)
	}
	if !strings.Contains(stderr, "mesh0:") || !strings.Contains(stderr, "unknown command") {
		t.Fatalf("stderr = %q, want an unknown-command message prefixed with mesh0:", stderr)
	}
}

func TestRunInvalidArgumentExitsTwo(t *testing.T) {
	// "put" requires PATH DOCUMENT and at least one assignment.
	_, stderr, code := runCLI(t, "put", "onlyonearg")
	if code != 2 {
		t.Fatalf("exit code = %d, want 2", code)
	}
	if stderr == "" {
		t.Fatal("expected an error message on stderr")
	}
}

func TestRunInitPutGetRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data")

	stdout, stderr, code := runCLI(t, "init", path)
	if code != 0 {
		t.Fatalf("init exit code = %d, stderr = %q", code, stderr)
	}
	if !strings.Contains(stdout, "initialized "+path) {
		t.Fatalf("init stdout = %q", stdout)
	}

	stdout, stderr, code = runCLI(t, "put", path, "tasks/1", `title="Ship it"`, "done=false")
	if code != 0 {
		t.Fatalf("put exit code = %d, stderr = %q", code, stderr)
	}
	if strings.TrimSpace(stdout) != "committed" {
		t.Fatalf("put stdout = %q", stdout)
	}

	stdout, stderr, code = runCLI(t, "get", path, "tasks/1")
	if code != 0 {
		t.Fatalf("get exit code = %d, stderr = %q", code, stderr)
	}
	if !strings.Contains(stdout, `"title"`) || !strings.Contains(stdout, "Ship it") {
		t.Fatalf("get stdout = %q", stdout)
	}

	stdout, stderr, code = runCLI(t, "status", path)
	if code != 0 {
		t.Fatalf("status exit code = %d, stderr = %q", code, stderr)
	}
	if !strings.Contains(stdout, "DATABASE STATUS") {
		t.Fatalf("status stdout = %q", stdout)
	}
}

func TestRunGetMissingDocumentIsNotFound(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data")
	if _, stderr, code := runCLI(t, "init", path); code != 0 {
		t.Fatalf("init failed: %s", stderr)
	}
	stdout, stderr, code := runCLI(t, "get", path, "tasks/missing")
	if code != 1 {
		t.Fatalf("exit code = %d, want 1 (not ErrInvalidArgument), stderr = %q", code, stderr)
	}
	if stdout != "" {
		t.Fatalf("stdout = %q, want empty", stdout)
	}
	if !strings.Contains(stderr, "not found") {
		t.Fatalf("stderr = %q, want a not-found message", stderr)
	}
}

func TestRunSelftest(t *testing.T) {
	stdout, stderr, code := runCLI(t, "selftest")
	if code != 0 {
		t.Fatalf("selftest exit code = %d, stderr = %q", code, stderr)
	}
	if !strings.Contains(stdout, "CORE INVARIANTS PASSED") {
		t.Fatalf("selftest stdout = %q", stdout)
	}
}

func TestPeerAddRemoveGrantRevokeAndListRoundTrip(t *testing.T) {
	localPath := filepath.Join(t.TempDir(), "local")
	if _, stderr, code := runCLI(t, "init", localPath); code != 0 {
		t.Fatalf("init failed: %s", stderr)
	}

	remoteDB, err := mesh0.Open(t.TempDir(), mesh0.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer remoteDB.Close()
	remoteKey, err := remoteDB.PeerPublicKey()
	if err != nil {
		t.Fatal(err)
	}
	remoteActor := mesh0.ID(remoteDB.ActorID()).String()
	remoteKeyHex := hex.EncodeToString(remoteKey)

	stdout, stderr, code := runCLI(t, "peer", "add", localPath, "remote", remoteActor, remoteKeyHex)
	if code != 0 {
		t.Fatalf("peer add exit code = %d, stderr = %q", code, stderr)
	}
	if !strings.Contains(stdout, "trusted and actor authorized") {
		t.Fatalf("peer add stdout = %q", stdout)
	}

	stdout, stderr, code = runCLI(t, "peer", "list", localPath)
	if code != 0 {
		t.Fatalf("peer list exit code = %d, stderr = %q", code, stderr)
	}
	if !strings.Contains(stdout, "remote") {
		t.Fatalf("peer list stdout = %q, want the newly trusted peer", stdout)
	}

	stdout, stderr, code = runCLI(t, "peer", "grant", localPath, remoteActor, "tasks")
	if code != 0 {
		t.Fatalf("peer grant exit code = %d, stderr = %q", code, stderr)
	}
	if !strings.Contains(stdout, "write granted") {
		t.Fatalf("peer grant stdout = %q", stdout)
	}

	stdout, stderr, code = runCLI(t, "peer", "revoke", localPath, remoteActor, "tasks")
	if code != 0 {
		t.Fatalf("peer revoke exit code = %d, stderr = %q", code, stderr)
	}
	if !strings.Contains(stdout, "write revoked") {
		t.Fatalf("peer revoke stdout = %q", stdout)
	}

	stdout, stderr, code = runCLI(t, "peer", "remove", localPath, remoteKeyHex)
	if code != 0 {
		t.Fatalf("peer remove exit code = %d, stderr = %q", code, stderr)
	}
	if !strings.Contains(stdout, "trust removed") {
		t.Fatalf("peer remove stdout = %q", stdout)
	}

	stdout, stderr, code = runCLI(t, "peer", "list", localPath)
	if code != 0 {
		t.Fatalf("peer list exit code = %d, stderr = %q", code, stderr)
	}
	if strings.Contains(stdout, "remote") {
		t.Fatalf("peer list stdout = %q, want the removed peer gone", stdout)
	}
}

func TestPeerAddPartialFailureLeavesNoTrust(t *testing.T) {
	localPath := filepath.Join(t.TempDir(), "local")
	if _, stderr, code := runCLI(t, "init", localPath); code != 0 {
		t.Fatalf("init failed: %s", stderr)
	}
	db, err := mesh0.Open(localPath, mesh0.Options{Durability: mesh0.DurabilitySync})
	if err != nil {
		t.Fatal(err)
	}
	localActor := mesh0.ID(db.ActorID()).String()
	localKey, err := db.PeerPublicKey()
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	// Attempting to pair a peer using the database's own actor/key must be
	// rejected before any trust pin is left behind.
	_, stderr, code := runCLI(t, "peer", "add", localPath, "self", localActor, hex.EncodeToString(localKey))
	if code == 0 {
		t.Fatal("expected peer add with the local actor/key to fail")
	}
	if !strings.Contains(stderr, "mesh0:") {
		t.Fatalf("stderr = %q, want an mesh0-prefixed error", stderr)
	}
	stdout, stderr, code := runCLI(t, "peer", "list", localPath)
	if code != 0 {
		t.Fatalf("peer list exit code = %d, stderr = %q", code, stderr)
	}
	if strings.TrimSpace(stdout) != "" {
		t.Fatalf("peer list stdout = %q, want no trusted peers after a failed pairing", stdout)
	}
}

func TestParsePeerKeyRejectsMalformedInput(t *testing.T) {
	if _, err := parsePeerKey("not-hex"); !errors.Is(err, mesh0.ErrInvalidArgument) {
		t.Fatalf("error = %v, want ErrInvalidArgument", err)
	}
	if _, err := parsePeerKey("00"); !errors.Is(err, mesh0.ErrInvalidArgument) {
		t.Fatalf("error = %v, want ErrInvalidArgument for wrong-length key", err)
	}
}
