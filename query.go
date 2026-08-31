package mesh0

import (
	"context"
	"fmt"
	"sort"
	"strings"
)

// Query is a deliberately bounded local query profile. Derived indexes never
// change canonical state or conflict semantics.
type Query struct {
	Collection string
	Path       string
	Equal      *Value
	Exists     bool
	Prefix     string
	Limit      int
}

type QueryResult struct {
	Key      DocumentKey
	Document DocumentView
}

// QueryPlan describes the local execution strategy selected for one query.
type QueryPlan struct {
	Strategy   string
	Index      *EqualityIndex
	Candidates int
}

func validateQuery(query Query) error {
	if query.Collection == "" {
		return fmt.Errorf("%w: query collection", ErrInvalidArgument)
	}
	if query.Limit < 0 || query.Limit > 100000 {
		return ErrResourceLimit
	}
	if query.Equal != nil {
		if err := query.Equal.Validate(); err != nil {
			return err
		}
		if query.Equal.Kind == ListRefValue || query.Equal.Kind == TextRefValue {
			return fmt.Errorf("%w: container equality query", ErrInvalidArgument)
		}
	}
	return nil
}

func (db *DB) querySnapshot() (*state, *equalityIndexSnapshot, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return nil, nil, ErrClosed
	}
	if db.failed != nil {
		return nil, nil, db.failed
	}
	return db.state, db.indexes, nil
}

func planQuery(root *state, indexes *equalityIndexSnapshot, query Query) ([]DocumentKey, QueryPlan, error) {
	plan := QueryPlan{Strategy: "canonical full scan"}
	if query.Equal != nil && indexes != nil {
		spec := EqualityIndex{Collection: query.Collection, Path: query.Path}
		if _, declared := indexes.declared[spec]; declared {
			valueKey, err := equalityValueKey(*query.Equal)
			if err != nil {
				return nil, plan, err
			}
			bucket := indexes.buckets[spec][valueKey]
			keys := make([]DocumentKey, 0, len(bucket))
			for key := range bucket {
				keys = append(keys, key)
			}
			indexed := spec
			plan = QueryPlan{Strategy: "equality index", Index: &indexed, Candidates: len(keys)}
			sort.Slice(keys, func(i, j int) bool { return keys[i].ID < keys[j].ID })
			return keys, plan, nil
		}
	}
	keys := make([]DocumentKey, 0)
	for key := range root.Documents {
		if key.Collection == query.Collection {
			keys = append(keys, key)
		}
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i].ID < keys[j].ID })
	plan.Candidates = len(keys)
	return keys, plan, nil
}

func queryMatches(document DocumentView, query Query) bool {
	values := document.Values(query.Path)
	if query.Exists && len(values) == 0 {
		return false
	}
	if query.Equal != nil {
		found := false
		for _, value := range values {
			if value.Equal(*query.Equal) {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	if query.Prefix != "" {
		found := false
		for _, value := range values {
			if value.Kind == StringValue && strings.HasPrefix(value.Text, query.Prefix) {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

func (db *DB) Query(ctx context.Context, query Query) ([]QueryResult, error) {
	if err := validateQuery(query); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	root, indexes, err := db.querySnapshot()
	if err != nil {
		return nil, err
	}
	keys, _, err := planQuery(root, indexes, query)
	if err != nil {
		return nil, err
	}
	results := make([]QueryResult, 0)
	for _, key := range keys {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		document, ok := root.Documents[key]
		if !ok {
			continue
		}
		view := DocumentView{key: key, doc: document}
		if !queryMatches(view, query) {
			continue
		}
		results = append(results, QueryResult{Key: key, Document: view})
		if query.Limit > 0 && len(results) >= query.Limit {
			break
		}
	}
	return results, nil
}

// ExplainQuery reports the local plan for this DB's currently declared indexes.
func (db *DB) ExplainQuery(query Query) (QueryPlan, error) {
	if err := validateQuery(query); err != nil {
		return QueryPlan{}, err
	}
	root, indexes, err := db.querySnapshot()
	if err != nil {
		return QueryPlan{}, err
	}
	_, plan, err := planQuery(root, indexes, query)
	return plan, err
}

// ExplainQuery documents the baseline planner when no DB-local index catalog is
// available (for example, existing callers of the package-level helper).
func ExplainQuery(query Query) string {
	filters := make([]string, 0, 3)
	if query.Equal != nil {
		filters = append(filters, query.Path+" == "+query.Equal.String())
	}
	if query.Exists {
		filters = append(filters, query.Path+" exists")
	}
	if query.Prefix != "" {
		filters = append(filters, query.Path+" prefix "+fmt.Sprintf("%q", query.Prefix))
	}
	if len(filters) == 0 {
		filters = append(filters, "none")
	}
	return "PLAN\n\ncollection: " + query.Collection + "\nstrategy: canonical full scan\npost-filter: " + strings.Join(filters, " and ") + "\nindexes: none declared"
}
