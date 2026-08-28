package mesh0

import (
	"context"
	"fmt"
	"sort"
	"strings"
)

// Query is a deliberately bounded local query profile. Derived indexes can be
// added without changing the canonical state or query semantics.
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

func (db *DB) Query(ctx context.Context, query Query) ([]QueryResult, error) {
	if query.Collection == "" {
		return nil, fmt.Errorf("%w: query collection", ErrInvalidArgument)
	}
	if query.Limit < 0 || query.Limit > 100000 {
		return nil, ErrResourceLimit
	}
	if query.Equal != nil {
		if err := query.Equal.Validate(); err != nil {
			return nil, err
		}
		if query.Equal.Kind == ListRefValue || query.Equal.Kind == TextRefValue {
			return nil, fmt.Errorf("%w: container equality query", ErrInvalidArgument)
		}
	}
	var results []QueryResult
	err := db.View(ctx, func(read *ReadTx) error {
		keys := make([]DocumentKey, 0)
		for key := range read.state.Documents {
			if key.Collection == query.Collection {
				keys = append(keys, key)
			}
		}
		sort.Slice(keys, func(i, j int) bool { return keys[i].ID < keys[j].ID })
		for _, key := range keys {
			if err := ctx.Err(); err != nil {
				return err
			}
			document, _ := read.Document(key.Collection, key.ID)
			values := document.Values(query.Path)
			if query.Exists && len(values) == 0 {
				continue
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
					continue
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
					continue
				}
			}
			results = append(results, QueryResult{Key: key, Document: document})
			if query.Limit > 0 && len(results) >= query.Limit {
				break
			}
		}
		return nil
	})
	return results, err
}

// ExplainQuery documents the safe baseline planner while persistent derived
// indexes are intentionally kept rebuildable and out of canonical storage.
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
