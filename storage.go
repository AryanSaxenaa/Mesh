package mesh0

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

const (
	identityName = "IDENTITY"
	manifestName = "MANIFEST"
	lockName     = "LOCK"
	segmentsDir  = "segments"
	snapshotsDir = "snapshots"
	blobsDir     = "blobs"
	tmpDir       = "tmp"
)

type manifest struct {
	Database DatabaseID
	Actor    ActorID
	NextSeq  uint64
	Active   uint64
	Snapshot string
}

func syncDir(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	if err := f.Sync(); err != nil {
		// Windows does not expose POSIX-style fsync for directory handles. File
		// contents are still synced before the atomic rename; rename durability
		// relies on the documented NTFS/Windows storage semantics.
		if runtime.GOOS == "windows" {
			return nil
		}
		return err
	}
	return nil
}
func atomicWrite(path string, data []byte, perm os.FileMode) error {
	dir := filepath.Dir(path)
	f, err := os.CreateTemp(dir, "."+filepath.Base(path)+".new-")
	if err != nil {
		return err
	}
	tmp := f.Name()
	defer os.Remove(tmp)
	if err = f.Chmod(perm); err == nil {
		_, err = f.Write(data)
	}
	if err == nil {
		err = f.Sync()
	}
	if closeErr := f.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	if err = atomicReplace(tmp, path); err != nil {
		return err
	}
	return syncDir(dir)
}
func identityBytes(m manifest) []byte {
	var e encoder
	e.raw([]byte("M0ID"))
	e.u(uint64(formatGeneration))
	e.id(ID(m.Database))
	e.id(ID(m.Actor))
	e.u(m.NextSeq)
	p := e.Bytes()
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], crc32.Checksum(p, castagnoli))
	return append(p, b[:]...)
}
func parseIdentity(b []byte) (DatabaseID, ActorID, uint64, error) {
	if len(b) < 4+1+64+1+4 {
		return DatabaseID{}, ActorID{}, 0, ErrCorruption
	}
	want := binary.BigEndian.Uint32(b[len(b)-4:])
	if crc32.Checksum(b[:len(b)-4], castagnoli) != want {
		return DatabaseID{}, ActorID{}, 0, ErrCorruption
	}
	d := decoder{b: b[:len(b)-4]}
	m, e := d.raw(4)
	if e != nil || string(m) != "M0ID" {
		return DatabaseID{}, ActorID{}, 0, ErrCorruption
	}
	g, e := d.u()
	if e != nil || g != formatGeneration {
		return DatabaseID{}, ActorID{}, 0, ErrCorruption
	}
	db, e := d.id()
	if e != nil {
		return DatabaseID{}, ActorID{}, 0, e
	}
	a, e := d.id()
	if e != nil {
		return DatabaseID{}, ActorID{}, 0, e
	}
	seq, e := d.u()
	if e != nil {
		return DatabaseID{}, ActorID{}, 0, e
	}
	return DatabaseID(db), ActorID(a), seq, d.done()
}
func manifestBytes(m manifest) []byte {
	var e encoder
	e.raw([]byte("M0MF"))
	e.u(uint64(formatGeneration))
	e.id(ID(m.Database))
	e.id(ID(m.Actor))
	e.u(m.NextSeq)
	e.u(m.Active)
	e.str(m.Snapshot)
	p := e.Bytes()
	h := sha256.Sum256(p)
	return append(p, h[:]...)
}
func parseManifest(b []byte) (manifest, error) {
	var m manifest
	if len(b) < 32 {
		return m, ErrCorruption
	}
	h := sha256.Sum256(b[:len(b)-32])
	if !bytes.Equal(h[:], b[len(b)-32:]) {
		return m, ErrCorruption
	}
	d := decoder{b: b[:len(b)-32]}
	mark, e := d.raw(4)
	if e != nil || string(mark) != "M0MF" {
		return m, ErrCorruption
	}
	g, e := d.u()
	if e != nil || g != formatGeneration {
		return m, ErrCorruption
	}
	id, e := d.id()
	if e != nil {
		return m, e
	}
	actor, e := d.id()
	if e != nil {
		return m, e
	}
	m.Database = DatabaseID(id)
	m.Actor = ActorID(actor)
	if m.NextSeq, e = d.u(); e != nil {
		return m, e
	}
	if m.Active, e = d.u(); e != nil {
		return m, e
	}
	if m.Snapshot, e = d.str(4096); e != nil {
		return m, e
	}
	return m, d.done()
}
func writeManifest(root string, m manifest) error {
	return atomicWrite(filepath.Join(root, manifestName), manifestBytes(m), 0600)
}
func readManifest(root string) (manifest, error) {
	b, e := os.ReadFile(filepath.Join(root, manifestName))
	if e != nil {
		return manifest{}, e
	}
	return parseManifest(b)
}
func initStorage(root string) (manifest, error) {
	if err := os.MkdirAll(root, 0700); err != nil {
		return manifest{}, err
	}
	for _, directory := range []string{segmentsDir, snapshotsDir, blobsDir, tmpDir} {
		if err := os.MkdirAll(filepath.Join(root, directory), 0700); err != nil {
			return manifest{}, err
		}
	}

	// MANIFEST is the commit record for database creation. Everything it names
	// must be durable before it is published, so an uncommitted initialization
	// never appears as a valid database after a crash.
	if _, err := os.Stat(filepath.Join(root, identityName)); err == nil {
		database, actor, nextSequence, err := parseIdentityFile(root)
		if err != nil {
			return manifest{}, fmt.Errorf("%w: incomplete identity: %v", ErrCorruption, err)
		}
		// A missing manifest is safely recoverable only when no checkpoint may
		// select history outside the WAL. Reconstruct the active segment from
		// the validated contiguous log rather than silently discarding rotated
		// segments.
		snapshots, err := os.ReadDir(filepath.Join(root, snapshotsDir))
		if err != nil {
			return manifest{}, err
		}
		if len(snapshots) != 0 {
			return manifest{}, fmt.Errorf("%w: manifest missing with snapshots present", ErrCorruption)
		}
		files, err := segmentFiles(root)
		if err != nil {
			return manifest{}, err
		}
		active := uint64(1)
		if len(files) == 0 {
			if err := ensureSegment(root, manifest{Database: database, Active: active}); err != nil {
				return manifest{}, err
			}
		} else {
			for index, file := range files {
				number, parseErr := strconv.ParseUint(strings.TrimSuffix(filepath.Base(file), ".seg"), 10, 64)
				if parseErr != nil || number == 0 || number != uint64(index+1) {
					return manifest{}, fmt.Errorf("%w: incomplete segment topology", ErrCorruption)
				}
				if _, scanErr := scanSegment(file, database, number, index == len(files)-1, func(Batch) error { return nil }); scanErr != nil {
					return manifest{}, fmt.Errorf("%w: incomplete segment: %v", ErrCorruption, scanErr)
				}
				active = number
			}
		}
		m := manifest{Database: database, Actor: actor, NextSeq: nextSequence, Active: active}
		if err := writeManifest(root, m); err != nil {
			return manifest{}, err
		}
		return m, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return manifest{}, err
	}

	// With neither MANIFEST nor IDENTITY there is no committed database
	// identity. Any segment files are uncommitted creation debris and can be
	// removed before creating a fresh identity.
	files, err := segmentFiles(root)
	if err != nil {
		return manifest{}, err
	}
	for _, file := range files {
		if err := os.Remove(file); err != nil {
			return manifest{}, err
		}
	}
	if len(files) != 0 {
		if err := syncDir(filepath.Join(root, segmentsDir)); err != nil {
			return manifest{}, err
		}
	}

	database, err := newID()
	if err != nil {
		return manifest{}, err
	}
	actor, err := newID()
	if err != nil {
		return manifest{}, err
	}
	m := manifest{Database: DatabaseID(database), Actor: ActorID(actor), NextSeq: 1, Active: 1}
	if err := ensureSegment(root, m); err != nil {
		return manifest{}, err
	}
	if err := persistIdentity(root, m); err != nil {
		return manifest{}, err
	}
	if err := writeManifest(root, m); err != nil {
		return manifest{}, err
	}
	return m, nil
}
func segmentPath(root string, n uint64) string {
	return filepath.Join(root, segmentsDir, fmt.Sprintf("%08d.seg", n))
}
func ensureSegment(root string, m manifest) error {
	path := segmentPath(root, m.Active)
	if _, err := os.Stat(path); err == nil {
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	var header encoder
	header.raw([]byte("M0SG"))
	header.u(uint64(formatGeneration))
	header.id(ID(m.Database))
	header.u(m.Active)
	if _, err = file.Write(header.Bytes()); err == nil {
		err = file.Sync()
	}
	if closeErr := file.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	// A manifest can only safely name this segment after the containing
	// directory entry is durable; otherwise checkpoint cleanup could discard
	// the previous authoritative logs before a crash loses this new entry.
	return syncDir(filepath.Join(root, segmentsDir))
}
func appendFrame(path string, payload []byte, syncIt bool) error {
	if len(payload) > maxBatchBytes {
		return ErrResourceLimit
	}
	f, e := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0600)
	if e != nil {
		return e
	}
	defer f.Close()
	var h encoder
	h.raw([]byte("M0FR"))
	h.u(uint64(len(payload)))
	head := h.Bytes()
	var c [4]byte
	sum := crc32.Update(0, castagnoli, head)
	sum = crc32.Update(sum, castagnoli, payload)
	binary.BigEndian.PutUint32(c[:], sum)
	if _, e = f.Write(head); e == nil {
		_, e = f.Write(payload)
	}
	if e == nil {
		_, e = f.Write(c[:])
	}
	if e == nil && syncIt {
		e = f.Sync()
	}
	if e != nil {
		return fmt.Errorf("%w: %v", ErrDurability, e)
	}
	return nil
}
func segmentFiles(root string) ([]string, error) {
	p, e := filepath.Glob(filepath.Join(root, segmentsDir, "*.seg"))
	if e != nil {
		return nil, e
	}
	sort.Strings(p)
	return p, nil
}
func scanSegment(path string, database DatabaseID, number uint64, active bool, fn func(Batch) error) (int64, error) {
	f, e := os.Open(path)
	if e != nil {
		return 0, e
	}
	defer f.Close()
	all, e := io.ReadAll(f)
	if e != nil {
		return 0, e
	}
	d := decoder{b: all}
	m, e := d.raw(4)
	if e != nil || string(m) != "M0SG" {
		return 0, ErrCorruption
	}
	g, e := d.u()
	if e != nil || g != formatGeneration {
		return 0, ErrCorruption
	}
	segmentDatabase, e := d.id()
	if e != nil || DatabaseID(segmentDatabase) != database {
		return 0, ErrCorruption
	}
	segmentNumber, e := d.u()
	if e != nil || segmentNumber != number {
		return 0, ErrCorruption
	}
	last := int64(d.at)
	for d.left() > 0 {
		start := d.at
		mark, e := d.raw(4)
		if e != nil || string(mark) != "M0FR" {
			if active {
				return last, nil
			}
			return last, ErrCorruption
		}
		n, e := d.u()
		if e != nil || n > maxBatchBytes {
			if active {
				return last, nil
			}
			return last, ErrCorruption
		}
		raw, e := d.raw(int(n))
		if e != nil {
			if active {
				return last, nil
			}
			return last, ErrCorruption
		}
		crc, e := d.raw(4)
		if e != nil {
			if active {
				return last, nil
			}
			return last, ErrCorruption
		}
		sum := crc32.Update(0, castagnoli, all[start:d.at-int(n)-4])
		sum = crc32.Update(sum, castagnoli, raw)
		if binary.BigEndian.Uint32(crc) != sum {
			if active {
				return last, nil
			}
			return last, ErrCorruption
		}
		b, e := UnmarshalBatch(raw)
		if e != nil {
			return last, e
		}
		if e = fn(b); e != nil {
			return last, e
		}
		last = int64(d.at)
	}
	return last, nil
}
func readAllBatches(root string, database DatabaseID, active uint64) ([]Batch, error) {
	files, e := segmentFiles(root)
	if e != nil {
		return nil, e
	}
	var out []Batch
	for _, p := range files {
		n, parseErr := strconv.ParseUint(strings.TrimSuffix(filepath.Base(p), ".seg"), 10, 64)
		if parseErr != nil {
			return nil, ErrCorruption
		}
		_, e = scanSegment(p, database, n, n == active, func(b Batch) error { out = append(out, b); return nil })
		if e != nil {
			return nil, e
		}
	}
	BatchSort(out)
	return out, nil
}

// removeObsoleteSegments is called only after a manifest points to a durable
// snapshot containing all retained history and a new active segment exists.
func removeObsoleteSegments(root string, active uint64) error {
	files, err := segmentFiles(root)
	if err != nil {
		return err
	}
	for _, file := range files {
		number, err := strconv.ParseUint(strings.TrimSuffix(filepath.Base(file), ".seg"), 10, 64)
		if err != nil {
			return ErrCorruption
		}
		if number < active {
			if err := os.Remove(file); err != nil {
				return err
			}
		}
	}
	return syncDir(filepath.Join(root, segmentsDir))
}
func writeSnapshot(root string, m manifest, s *state) (string, error) {
	batches := make([]Batch, 0, len(s.Batches))
	for _, b := range s.Batches {
		batches = append(batches, b)
	}
	BatchSort(batches)
	var e encoder
	e.raw([]byte("M0SN"))
	e.u(uint64(formatGeneration))
	e.clock(s.Frontier)
	e.u(uint64(len(batches)))
	for _, b := range batches {
		raw, x := b.MarshalBinary()
		if x != nil {
			return "", x
		}
		e.bytes(raw)
	}
	payload := e.Bytes()
	h := sha256.Sum256(payload)
	e.raw(h[:])
	e.raw([]byte("M0END"))
	name := fmt.Sprintf("snapshot-%020d.m0s", m.Active)
	if err := atomicWrite(filepath.Join(root, snapshotsDir, name), e.Bytes(), 0600); err != nil {
		return "", err
	}
	return name, nil
}
func readSnapshot(root, name string) ([]Batch, error) {
	if name == "" {
		return nil, nil
	}
	b, e := os.ReadFile(filepath.Join(root, snapshotsDir, name))
	if e != nil {
		return nil, e
	}
	footer := []byte("M0END")
	if len(b) < len(footer)+sha256.Size {
		return nil, ErrCorruption
	}
	if !bytes.Equal(b[len(b)-len(footer):], footer) {
		return nil, ErrCorruption
	}
	payloadEnd := len(b) - len(footer) - sha256.Size
	p := b[:payloadEnd]
	h := sha256.Sum256(p)
	if !bytes.Equal(h[:], b[payloadEnd:payloadEnd+sha256.Size]) {
		return nil, ErrCorruption
	}
	d := decoder{b: p}
	magic, e := d.raw(4)
	if e != nil || string(magic) != "M0SN" {
		return nil, ErrCorruption
	}
	g, e := d.u()
	if e != nil || g != formatGeneration {
		return nil, ErrCorruption
	}
	if _, e = d.clock(); e != nil {
		return nil, e
	}
	n, e := d.u()
	if e != nil || n > 1<<20 {
		return nil, ErrCorruption
	}
	out := make([]Batch, 0, n)
	for i := uint64(0); i < n; i++ {
		raw, e := d.bytes(maxBatchBytes)
		if e != nil {
			return nil, e
		}
		x, e := UnmarshalBatch(raw)
		if e != nil {
			return nil, e
		}
		out = append(out, x)
	}
	return out, d.done()
}
