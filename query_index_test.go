package mesh0

import (
	"context"
	"path/filepath"
	"reflect"
	"testing"
)

func queryResultIDs(results []QueryResult) []string {
	ids := make([]string, len(results))
	for index, result := range results {
		ids[index] = result.Key.ID
	}
	return ids
}

func TestEqualityIndexMatchesCanonicalQueryAndUpdatesAtomically(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	mustSet(t, db, "tasks", "b", "status", String("open"))
	mustSet(t, db, "tasks", "a", "status", String("open"))
	mustSet(t, db, "tasks", "c", "status", String("closed"))
	query := Query{Collection: "tasks", Path: "status", Equal: ptr(String("open")), Prefix: "o"}
	baseline, err := db.Query(ctx, query)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.EnsureEqualityIndex(ctx, EqualityIndex{Collection: "tasks", Path: "status"}); err != nil {
		t.Fatal(err)
	}
	plan, err := db.ExplainQuery(query)
	if err != nil {
		t.Fatal(err)
	}
	if plan.Strategy != "equality index" || plan.Index == nil || *plan.Index != (EqualityIndex{Collection: "tasks", Path: "status"}) || plan.Candidates != 2 {
		t.Fatalf("indexed plan = %#v", plan)
	}
	indexed, err := db.Query(ctx, query)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(queryResultIDs(indexed), queryResultIDs(baseline)) || !reflect.DeepEqual(queryResultIDs(indexed), []string{"a", "b"}) {
		t.Fatalf("indexed results = %#v; baseline = %#v", queryResultIDs(indexed), queryResultIDs(baseline))
	}
	mustSet(t, db, "tasks", "a", "status", String("closed"))
	indexed, err = db.Query(ctx, query)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(queryResultIDs(indexed), []string{"b"}) {
		t.Fatalf("updated indexed results = %#v", queryResultIDs(indexed))
	}
	if err := db.Update(ctx, func(tx *Tx) error { return tx.Document("tasks", "b").Delete("status") }); err != nil {
		t.Fatal(err)
	}
	indexed, err = db.Query(ctx, query)
	if err != nil {
		t.Fatal(err)
	}
	if len(indexed) != 0 {
		t.Fatalf("deleted indexed results = %#v", queryResultIDs(indexed))
	}
}

func TestEqualityIndexPreservesValueKindsAndRebuildsAfterReopen(t *testing.T) {
	ctx := context.Background()
	path := t.TempDir()
	db, err := Open(path, Options{})
	if err != nil {
		t.Fatal(err)
	}
	mustSet(t, db, "metrics", "integer", "value", Int(1))
	mustSet(t, db, "metrics", "float", "value", Float(1))
	spec := EqualityIndex{Collection: "metrics", Path: "value"}
	if err := db.EnsureEqualityIndex(ctx, spec); err != nil {
		t.Fatal(err)
	}
	integerResults, err := db.Query(ctx, Query{Collection: "metrics", Path: "value", Equal: ptr(Int(1))})
	if err != nil || !reflect.DeepEqual(queryResultIDs(integerResults), []string{"integer"}) {
		t.Fatalf("integer index results = %#v, %v", queryResultIDs(integerResults), err)
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
	plan, err := db.ExplainQuery(Query{Collection: "metrics", Path: "value", Equal: ptr(Int(1))})
	if err != nil || plan.Strategy != "canonical full scan" {
		t.Fatalf("reopened plan = %#v, %v", plan, err)
	}
	if err := db.EnsureEqualityIndex(ctx, spec); err != nil {
		t.Fatal(err)
	}
	integerResults, err = db.Query(ctx, Query{Collection: "metrics", Path: "value", Equal: ptr(Int(1))})
	if err != nil || !reflect.DeepEqual(queryResultIDs(integerResults), []string{"integer"}) {
		t.Fatalf("rebuilt integer index results = %#v, %v", queryResultIDs(integerResults), err)
	}
}

func TestEqualityIndexPreservesConcurrentRegisterValues(t *testing.T) {
	ctx := context.Background()
	left := newTestDB(t)
	mustSet(t, left, "tasks", "42", "status", String("draft"))
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
	spec := EqualityIndex{Collection: "tasks", Path: "status"}
	if err := left.EnsureEqualityIndex(ctx, spec); err != nil {
		t.Fatal(err)
	}
	if err := right.EnsureEqualityIndex(ctx, spec); err != nil {
		t.Fatal(err)
	}
	mustSet(t, left, "tasks", "42", "status", String("ready"))
	mustSet(t, right, "tasks", "42", "status", String("blocked"))
	syncAuthorizedPair(t, left, right, "tasks")
	for _, db := range []*DB{left, right} {
		for _, status := range []string{"ready", "blocked"} {
			results, err := db.Query(ctx, Query{Collection: "tasks", Path: "status", Equal: ptr(String(status))})
			if err != nil || !reflect.DeepEqual(queryResultIDs(results), []string{"42"}) {
				t.Fatalf("%s conflict index = %#v, %v", status, queryResultIDs(results), err)
			}
		}
	}
	mustSet(t, left, "tasks", "42", "status", String("ready"))
	syncAuthorizedPair(t, left, right, "tasks")
	for _, db := range []*DB{left, right} {
		results, err := db.Query(ctx, Query{Collection: "tasks", Path: "status", Equal: ptr(String("blocked"))})
		if err != nil || len(results) != 0 {
			t.Fatalf("resolved blocked index = %#v, %v", queryResultIDs(results), err)
		}
	}
}
