package mesh0

import (
	"context"
	"errors"
	"math/rand"
	"path/filepath"
	"reflect"
	"testing"
)

func createTestList(t *testing.T, db *DB, collection, id, path string) (ObjectID, ElementID) {
	t.Helper()
	var object ObjectID
	var first ElementID
	if err := db.Update(context.Background(), func(tx *Tx) error {
		list, err := tx.Document(collection, id).CreateList(path)
		if err != nil {
			return err
		}
		object = list.ObjectID()
		ids, err := list.InsertAfter(nil, String("base"))
		if err == nil {
			first = ids[0]
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	return object, first
}

func readList(t *testing.T, db *DB, collection, id, path string) []ListElement {
	t.Helper()
	var elements []ListElement
	if err := db.View(context.Background(), func(read *ReadTx) error {
		document, ok := read.Document(collection, id)
		if !ok {
			t.Fatal("document absent")
		}
		list, ok := document.List(path)
		if !ok {
			t.Fatal("list absent")
		}
		elements = list.Elements()
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return elements
}

func TestAnchoredConcurrentListInsertConverges(t *testing.T) {
	ctx := context.Background()
	left := newTestDB(t)
	object, anchor := createTestList(t, left, "notes", "one", "items")
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
	if err := left.Update(ctx, func(tx *Tx) error {
		_, err := tx.Document("notes", "one").List(object).InsertAfter(&anchor, String("left"))
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := right.Update(ctx, func(tx *Tx) error {
		_, err := tx.Document("notes", "one").List(object).InsertAfter(&anchor, String("right"))
		return err
	}); err != nil {
		t.Fatal(err)
	}
	syncAuthorizedPair(t, right, left, "notes")
	leftElements := readList(t, left, "notes", "one", "items")
	rightElements := readList(t, right, "notes", "one", "items")
	if !reflect.DeepEqual(leftElements, rightElements) {
		t.Fatalf("concurrent insertion diverged:\nleft=%#v\nright=%#v", leftElements, rightElements)
	}
	if len(leftElements) != 3 || !leftElements[0].Value.Equal(String("base")) {
		t.Fatalf("unexpected visible sequence: %#v", leftElements)
	}
	seen := map[string]bool{}
	for _, element := range leftElements[1:] {
		seen[element.Value.Text] = true
	}
	if !seen["left"] || !seen["right"] {
		t.Fatalf("concurrent values missing: %#v", leftElements)
	}
	leftDigest, err := left.LogicalDigest()
	if err != nil {
		t.Fatal(err)
	}
	rightDigest, err := right.LogicalDigest()
	if err != nil {
		t.Fatal(err)
	}
	if leftDigest != rightDigest {
		t.Fatal("same list operation set produced distinct canonical digests")
	}
}

func TestSequenceDeletePreservesAnchorForConcurrentChild(t *testing.T) {
	ctx := context.Background()
	left := newTestDB(t)
	object, anchor := createTestList(t, left, "notes", "one", "items")
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
	if err := left.Update(ctx, func(tx *Tx) error { return tx.Document("notes", "one").List(object).Delete(anchor) }); err != nil {
		t.Fatal(err)
	}
	if err := right.Update(ctx, func(tx *Tx) error {
		_, err := tx.Document("notes", "one").List(object).InsertAfter(&anchor, String("survives"))
		return err
	}); err != nil {
		t.Fatal(err)
	}
	syncAuthorizedPair(t, left, right, "notes")
	elements := readList(t, left, "notes", "one", "items")
	if len(elements) != 1 || !elements[0].Value.Equal(String("survives")) {
		t.Fatalf("deleted anchor did not retain concurrent child: %#v", elements)
	}
}

func TestListRunTextAndSnapshotRecovery(t *testing.T) {
	ctx := context.Background()
	path := t.TempDir()
	db, err := Open(path, Options{})
	if err != nil {
		t.Fatal(err)
	}
	var object ObjectID
	var textIDs []ElementID
	if err := db.Update(ctx, func(tx *Tx) error {
		text, err := tx.Document("notes", "one").CreateText("body")
		if err != nil {
			return err
		}
		object = text.ObjectID()
		textIDs, err = text.InsertTextAfter(nil, "A界🙂")
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.Update(ctx, func(tx *Tx) error {
		_, err := tx.Document("notes", "one").Text(object).InsertTextAfter(&textIDs[1], "!")
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Snapshot(ctx); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(path, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.View(ctx, func(read *ReadTx) error {
		document, _ := read.Document("notes", "one")
		text, ok := document.List("body")
		if !ok {
			t.Fatal("text absent")
		}
		actual, ok := text.Text()
		if !ok || actual != "A界🙂!" {
			t.Fatalf("text projection = %q, %t", actual, ok)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.Verify(ctx, false); err != nil {
		t.Fatal(err)
	}
}

func TestListBatchCodecRejectsUnorderedDeleteTargets(t *testing.T) {
	id, err := newID()
	if err != nil {
		t.Fatal(err)
	}
	actor := ActorID(id)
	objectDot := Dot{Actor: actor, Seq: 1}
	deleteDot := Dot{Actor: actor, Seq: 2}
	first := ElementID{Dot: objectDot}
	second := ElementID{Dot: deleteDot}
	batch := Batch{First: deleteDot, Count: 1, Dependencies: VersionVector{actor: 1}, Operations: []Operation{{Dot: deleteDot, Document: DocumentKey{"notes", "one"}, Action: ListDelete, Object: ObjectID{Dot: objectDot}, Targets: []ElementID{second, first}}}}
	if err := batch.Validate(); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("unordered delete targets error = %v, want invalid argument", err)
	}
}

func TestAnchoredListPermutationProducesIdenticalState(t *testing.T) {
	firstID, err := newID()
	if err != nil {
		t.Fatal(err)
	}
	secondID, err := newID()
	if err != nil {
		t.Fatal(err)
	}
	thirdID, err := newID()
	if err != nil {
		t.Fatal(err)
	}
	firstActor, secondActor, thirdActor := ActorID(firstID), ActorID(secondID), ActorID(thirdID)
	creation := Dot{Actor: firstActor, Seq: 1}
	object := ObjectID{Dot: creation}
	anchor := ElementID{Dot: Dot{Actor: firstActor, Seq: 3}}
	base := Batch{First: creation, Count: 3, Dependencies: VersionVector{}, Operations: []Operation{
		{Dot: creation, Document: DocumentKey{"notes", "one"}, Action: MakeList, Object: object, ObjectKind: ListObject},
		{Dot: Dot{Actor: firstActor, Seq: 2}, Document: DocumentKey{"notes", "one"}, Path: []string{"items"}, Action: MapAssign, Value: ListRef(object)},
		{Dot: anchor.Dot, Document: DocumentKey{"notes", "one"}, Action: ListInsert, Object: object, AnchorHead: true, Values: []Value{String("base")}},
	}}
	left := Batch{First: Dot{Actor: secondActor, Seq: 1}, Count: 1, Dependencies: VersionVector{firstActor: 3}, Operations: []Operation{{Dot: Dot{Actor: secondActor, Seq: 1}, Document: DocumentKey{"notes", "one"}, Action: ListInsert, Object: object, Anchor: anchor, Values: []Value{String("left")}}}}
	right := Batch{First: Dot{Actor: thirdActor, Seq: 1}, Count: 1, Dependencies: VersionVector{firstActor: 3}, Operations: []Operation{{Dot: Dot{Actor: thirdActor, Seq: 1}, Document: DocumentKey{"notes", "one"}, Action: ListInsert, Object: object, Anchor: anchor, Values: []Value{String("right")}}}}
	one := newState()
	for _, batch := range []Batch{base, left, right} {
		if one, err = one.apply(batch); err != nil {
			t.Fatal(err)
		}
	}
	two := newState()
	for _, batch := range []Batch{base, right, left} {
		if two, err = two.apply(batch); err != nil {
			t.Fatal(err)
		}
	}
	firstDocument := DocumentView{key: DocumentKey{"notes", "one"}, doc: one.Documents[DocumentKey{"notes", "one"}]}
	secondDocument := DocumentView{key: DocumentKey{"notes", "one"}, doc: two.Documents[DocumentKey{"notes", "one"}]}
	firstList, firstOK := firstDocument.List("items")
	secondList, secondOK := secondDocument.List("items")
	if !firstOK || !secondOK || !reflect.DeepEqual(firstList.Elements(), secondList.Elements()) {
		t.Fatalf("permuted valid operations diverged: %#v vs %#v", firstList.Elements(), secondList.Elements())
	}
	if one.digest(DatabaseID{}) != two.digest(DatabaseID{}) {
		t.Fatal("permuted valid list history produced distinct logical digests")
	}
}

func TestSequenceOperationsRoundTripThroughCanonicalBatchCodec(t *testing.T) {
	db := newTestDB(t)
	var object ObjectID
	if err := db.Update(context.Background(), func(tx *Tx) error {
		list, err := tx.Document("notes", "one").CreateList("items")
		if err != nil {
			return err
		}
		object = list.ObjectID()
		ids, err := list.InsertAfter(nil, String("one"), String("two"))
		if err != nil {
			return err
		}
		return list.Delete(ids[0])
	}); err != nil {
		t.Fatal(err)
	}
	history, err := db.History("notes", "one")
	if err != nil {
		t.Fatal(err)
	}
	if len(history) != 1 || history[0].Operations[0].Object != object {
		t.Fatalf("unexpected history: %#v", history)
	}
	raw, err := history[0].MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := UnmarshalBatch(raw)
	if err != nil {
		t.Fatal(err)
	}
	originalHash, err := history[0].Hash()
	if err != nil {
		t.Fatal(err)
	}
	decodedHash, err := decoded.Hash()
	if err != nil {
		t.Fatal(err)
	}
	if originalHash != decodedHash {
		t.Fatal("sequence batch codec changed the canonical batch hash")
	}
	if len(decoded.Operations) != 4 || decoded.Operations[0].Action != MakeList || decoded.Operations[2].Action != ListInsert || decoded.Operations[3].Action != ListDelete {
		t.Fatalf("sequence batch codec lost operation payloads: %#v", decoded.Operations)
	}
}

func TestExportProjectsTopLevelListAndTextReferences(t *testing.T) {
	db := newTestDB(t)
	if err := db.Update(context.Background(), func(tx *Tx) error {
		document := tx.Document("notes", "one")
		list, err := document.CreateList("items")
		if err != nil {
			return err
		}
		if _, err := list.InsertAfter(nil, String("one"), String("two")); err != nil {
			return err
		}
		text, err := document.CreateText("body")
		if err != nil {
			return err
		}
		_, err = text.InsertTextAfter(nil, "hi")
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.View(context.Background(), func(read *ReadTx) error {
		document, _ := read.Document("notes", "one")
		exported, err := ExportDocument(document, false)
		if err != nil {
			return err
		}
		items, ok := exported["items"].([]Value)
		if !ok || len(items) != 2 || !items[0].Equal(String("one")) || !items[1].Equal(String("two")) {
			t.Fatalf("list export = %#v", exported["items"])
		}
		if body, ok := exported["body"].(string); !ok || body != "hi" {
			t.Fatalf("text export = %#v", exported["body"])
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestQueryRejectsContainerReferenceEquality(t *testing.T) {
	db := newTestDB(t)
	var object ObjectID
	if err := db.Update(context.Background(), func(tx *Tx) error {
		list, err := tx.Document("notes", "one").CreateList("items")
		object = list.ObjectID()
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Query(context.Background(), Query{Collection: "notes", Path: "items", Equal: ptr(ListRef(object))}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("container equality query error = %v, want invalid argument", err)
	}
}

func ptr(value Value) *Value { return &value }

func TestNestedFieldPathsAndContainerReferencesSurviveRestart(t *testing.T) {
	path := t.TempDir()
	db, err := Open(path, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Update(context.Background(), func(tx *Tx) error {
		document := tx.Document("projects", "launch")
		if err := document.SetPath([]string{"metadata", "owner", "name"}, String("Ada")); err != nil {
			return err
		}
		if err := document.CounterAddPath([]string{"metadata", "stats", "edits"}, 3); err != nil {
			return err
		}
		list, err := document.CreateListPath([]string{"metadata", "tags"})
		if err != nil {
			return err
		}
		_, err = list.InsertAfter(nil, String("offline"))
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Snapshot(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if db, err = Open(path, Options{}); err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.View(context.Background(), func(read *ReadTx) error {
		document, ok := read.Document("projects", "launch")
		if !ok {
			t.Fatal("nested document absent")
		}
		if value, ok := document.Value("metadata", "owner", "name"); !ok || !value.Equal(String("Ada")) {
			t.Fatalf("nested value = %#v, %t", value, ok)
		}
		if edits := document.Counter("metadata", "stats", "edits"); edits != 3 {
			t.Fatalf("nested counter = %d", edits)
		}
		list, ok := document.List("metadata", "tags")
		if !ok || len(list.Elements()) != 1 || !list.Elements()[0].Value.Equal(String("offline")) {
			t.Fatalf("nested list = %#v, %t", list.Elements(), ok)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestDocumentDeleteHidesSequenceButPreservesItForCausalRevival(t *testing.T) {
	db := newTestDB(t)
	object, anchor := createTestList(t, db, "notes", "one", "items")
	if err := db.Update(context.Background(), func(tx *Tx) error { return tx.Document("notes", "one").DeleteDocument() }); err != nil {
		t.Fatal(err)
	}
	if err := db.View(context.Background(), func(read *ReadTx) error {
		document, _ := read.Document("notes", "one")
		if _, ok := document.List("items"); ok {
			t.Fatal("document delete left a visible list reference")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.Update(context.Background(), func(tx *Tx) error {
		document := tx.Document("notes", "one")
		if err := document.Set("items", ListRef(object)); err != nil {
			return err
		}
		_, err := document.List(object).InsertAfter(&anchor, String("revived"))
		return err
	}); err != nil {
		t.Fatal(err)
	}
	elements := readList(t, db, "notes", "one", "items")
	if len(elements) != 2 || !elements[0].Value.Equal(String("base")) || !elements[1].Value.Equal(String("revived")) {
		t.Fatalf("causal revival lost retained sequence state: %#v", elements)
	}
}

func TestAnchoredListDeterministicPermutationProperty(t *testing.T) {
	for seed := int64(1); seed <= 32; seed++ {
		random := rand.New(rand.NewSource(seed))
		var ids [7]ActorID
		for index := range ids {
			var id ID
			id[len(id)-1] = byte(index + 1)
			ids[index] = ActorID(id)
		}
		creator := ids[0]
		creation := Dot{Actor: creator, Seq: 1}
		object := ObjectID{Dot: creation}
		anchor := ElementID{Dot: Dot{Actor: creator, Seq: 3}}
		base := Batch{First: creation, Count: 3, Dependencies: VersionVector{}, Operations: []Operation{
			{Dot: creation, Document: DocumentKey{"notes", "property"}, Action: MakeList, Object: object, ObjectKind: ListObject},
			{Dot: Dot{Actor: creator, Seq: 2}, Document: DocumentKey{"notes", "property"}, Path: []string{"items"}, Action: MapAssign, Value: ListRef(object)},
			{Dot: anchor.Dot, Document: DocumentKey{"notes", "property"}, Action: ListInsert, Object: object, AnchorHead: true, Values: []Value{String("base")}},
		}}
		writers := 2 + random.Intn(5)
		updates := make([]Batch, writers)
		for index := range updates {
			actor := ids[index+1]
			updates[index] = Batch{First: Dot{Actor: actor, Seq: 1}, Count: 1, Dependencies: VersionVector{creator: 3}, Operations: []Operation{{Dot: Dot{Actor: actor, Seq: 1}, Document: DocumentKey{"notes", "property"}, Action: ListInsert, Object: object, Anchor: anchor, Values: []Value{String(string(rune('a' + index)))}}}}
		}
		baseline := newState()
		var err error
		if baseline, err = baseline.apply(base); err != nil {
			t.Fatal(err)
		}
		for _, batch := range updates {
			if baseline, err = baseline.apply(batch); err != nil {
				t.Fatal(err)
			}
		}
		baselineDocument := DocumentView{key: DocumentKey{"notes", "property"}, doc: baseline.Documents[DocumentKey{"notes", "property"}]}
		baselineList, ok := baselineDocument.List("items")
		if !ok {
			t.Fatal("property list absent")
		}
		for permutation := 0; permutation < 10; permutation++ {
			order := random.Perm(len(updates))
			candidate := newState()
			if candidate, err = candidate.apply(base); err != nil {
				t.Fatal(err)
			}
			for _, index := range order {
				if candidate, err = candidate.apply(updates[index]); err != nil {
					t.Fatal(err)
				}
			}
			candidateDocument := DocumentView{key: DocumentKey{"notes", "property"}, doc: candidate.Documents[DocumentKey{"notes", "property"}]}
			candidateList, ok := candidateDocument.List("items")
			if !ok || !reflect.DeepEqual(baselineList.Elements(), candidateList.Elements()) || baseline.digest(DatabaseID{}) != candidate.digest(DatabaseID{}) {
				t.Fatalf("seed %d permutation %v changed anchored-list state", seed, order)
			}
		}
	}
}
