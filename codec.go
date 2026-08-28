package mesh0

import (
	"bytes"
	"crypto/rand"
	"encoding/base32"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"sort"
)

const (
	formatGeneration   = 1
	maxStringBytes     = 1 << 20
	maxBatchBytes      = 16 << 20
	maxBatchOperations = 65535
	maxPathParts       = 64
)

// ID is a cryptographically random, fixed-width logical identity.
type ID [32]byte

type DatabaseID ID
type ActorID ID

func newID() (ID, error) {
	var id ID
	_, err := io.ReadFull(rand.Reader, id[:])
	return id, err
}

func (id ID) String() string {
	// The checksum-free encoding is canonical, fixed width, and solely display metadata.
	return base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(id[:])
}

func (id ID) IsZero() bool { return id == ID{} }

// ParseActorID parses the canonical, fixed-width display form emitted by
// ActorID.String. It rejects alternate encodings so CLI pairing metadata is
// unambiguous when confirmed out of band.
func ParseActorID(encoded string) (ActorID, error) {
	decoded, err := base32.StdEncoding.WithPadding(base32.NoPadding).DecodeString(encoded)
	if err != nil || len(decoded) != len(ActorID{}) {
		return ActorID{}, fmt.Errorf("%w: actor ID", ErrInvalidArgument)
	}
	var actor ActorID
	copy(actor[:], decoded)
	if ID(actor).String() != encoded || ID(actor).IsZero() {
		return ActorID{}, fmt.Errorf("%w: actor ID", ErrInvalidArgument)
	}
	return actor, nil
}

func idCompare(a, b ID) int { return bytes.Compare(a[:], b[:]) }

// Dot uniquely identifies one immutable operation.
type Dot struct {
	Actor ActorID
	Seq   uint64
}

func (d Dot) String() string { return fmt.Sprintf("%s:%d", ID(d.Actor).String(), d.Seq) }
func (d Dot) Compare(other Dot) int {
	if c := idCompare(ID(d.Actor), ID(other.Actor)); c != 0 {
		return c
	}
	if d.Seq < other.Seq {
		return -1
	}
	if d.Seq > other.Seq {
		return 1
	}
	return 0
}

// VersionVector records the largest contiguous accepted sequence for each actor.
type VersionVector map[ActorID]uint64

func (v VersionVector) Clone() VersionVector {
	o := make(VersionVector, len(v))
	for a, s := range v {
		o[a] = s
	}
	return o
}
func (v VersionVector) Get(a ActorID) uint64 { return v[a] }
func (v VersionVector) Contains(d Dot) bool  { return v[d.Actor] >= d.Seq }
func (v VersionVector) Covers(other VersionVector) bool {
	for a, s := range other {
		if v[a] < s {
			return false
		}
	}
	return true
}
func (v VersionVector) Join(other VersionVector) {
	for a, s := range other {
		if s > v[a] {
			v[a] = s
		}
	}
}

type ClockRelation uint8

const (
	ClockEqual ClockRelation = iota
	ClockBefore
	ClockAfter
	ClockConcurrent
)

func (v VersionVector) Compare(other VersionVector) ClockRelation {
	vCovers, oCovers := v.Covers(other), other.Covers(v)
	switch {
	case vCovers && oCovers:
		return ClockEqual
	case vCovers:
		return ClockAfter
	case oCovers:
		return ClockBefore
	default:
		return ClockConcurrent
	}
}

func sortedActors(v VersionVector) []ActorID {
	a := make([]ActorID, 0, len(v))
	for x := range v {
		a = append(a, x)
	}
	sort.Slice(a, func(i, j int) bool { return idCompare(ID(a[i]), ID(a[j])) < 0 })
	return a
}

type encoder struct{ bytes.Buffer }

func (e *encoder) u(v uint64)     { var b [10]byte; n := binary.PutUvarint(b[:], v); e.Write(b[:n]) }
func (e *encoder) i(v int64)      { e.u(uint64(v<<1) ^ uint64(v>>63)) }
func (e *encoder) raw(v []byte)   { e.Write(v) }
func (e *encoder) bytes(v []byte) { e.u(uint64(len(v))); e.raw(v) }
func (e *encoder) str(v string)   { e.bytes([]byte(v)) }
func (e *encoder) id(v ID)        { e.raw(v[:]) }
func (e *encoder) dot(v Dot)      { e.id(ID(v.Actor)); e.u(v.Seq) }
func (e *encoder) element(v ElementID) {
	e.dot(v.Dot)
	e.u(uint64(v.Offset))
}
func (e *encoder) clock(v VersionVector) {
	a := sortedActors(v)
	e.u(uint64(len(a)))
	for _, actor := range a {
		e.id(ID(actor))
		e.u(v[actor])
	}
}

type decoder struct {
	b  []byte
	at int
}

func (d *decoder) left() int { return len(d.b) - d.at }
func (d *decoder) raw(n int) ([]byte, error) {
	if n < 0 || n > d.left() {
		return nil, io.ErrUnexpectedEOF
	}
	v := d.b[d.at : d.at+n]
	d.at += n
	return v, nil
}
func (d *decoder) u() (uint64, error) {
	v, n := binary.Uvarint(d.b[d.at:])
	if n <= 0 {
		if n == 0 {
			return 0, io.ErrUnexpectedEOF
		}
		return 0, fmt.Errorf("%w: non-canonical varint", ErrCorruption)
	}
	if n > 1 && d.b[d.at+n-1] == 0 {
		return 0, fmt.Errorf("%w: non-canonical varint", ErrCorruption)
	}
	d.at += n
	return v, nil
}
func (d *decoder) i() (int64, error) { u, e := d.u(); return int64(u>>1) ^ -int64(u&1), e }
func (d *decoder) bytes(max int) ([]byte, error) {
	n, e := d.u()
	if e != nil {
		return nil, e
	}
	if n > uint64(max) || n > uint64(d.left()) {
		return nil, fmt.Errorf("%w: length", ErrResourceLimit)
	}
	v, e := d.raw(int(n))
	if e != nil {
		return nil, e
	}
	return append([]byte(nil), v...), nil
}
func (d *decoder) str(max int) (string, error) { b, e := d.bytes(max); return string(b), e }
func (d *decoder) id() (ID, error) {
	var id ID
	b, e := d.raw(len(id))
	if e == nil {
		copy(id[:], b)
	}
	return id, e
}
func (d *decoder) dot() (Dot, error) {
	id, err := d.id()
	if err != nil {
		return Dot{}, err
	}
	sequence, err := d.u()
	return Dot{Actor: ActorID(id), Seq: sequence}, err
}
func (d *decoder) element() (ElementID, error) {
	dot, err := d.dot()
	if err != nil {
		return ElementID{}, err
	}
	offset, err := d.u()
	if err != nil || offset > math.MaxUint32 {
		return ElementID{}, ErrCorruption
	}
	return ElementID{Dot: dot, Offset: uint32(offset)}, nil
}
func (d *decoder) clock() (VersionVector, error) {
	n, e := d.u()
	if e != nil {
		return nil, e
	}
	if n > 65536 {
		return nil, ErrResourceLimit
	}
	v := make(VersionVector, n)
	var last ID
	for i := uint64(0); i < n; i++ {
		id, e := d.id()
		if e != nil {
			return nil, e
		}
		if i > 0 && idCompare(last, id) >= 0 {
			return nil, fmt.Errorf("%w: clock actor ordering", ErrCorruption)
		}
		s, e := d.u()
		if e != nil {
			return nil, e
		}
		v[ActorID(id)] = s
		last = id
	}
	return v, nil
}
func (d *decoder) done() error {
	if d.at != len(d.b) {
		return fmt.Errorf("%w: trailing bytes", ErrCorruption)
	}
	return nil
}

func appendFloat(e *encoder, f float64) error {
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return fmt.Errorf("%w: non-finite float", ErrInvalidArgument)
	}
	if f == 0 {
		f = 0
	}
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], math.Float64bits(f))
	e.raw(b[:])
	return nil
}
func readFloat(d *decoder) (float64, error) {
	b, e := d.raw(8)
	if e != nil {
		return 0, e
	}
	f := math.Float64frombits(binary.BigEndian.Uint64(b))
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return 0, fmt.Errorf("%w: non-finite float", ErrCorruption)
	}
	if f == 0 {
		f = 0
	}
	return f, nil
}
