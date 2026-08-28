package mesh0

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

// ExportDocument produces a projection that retains concurrent scalar values
// explicitly instead of silently applying the deterministic read projection.
func ExportDocument(document DocumentView, includeConflicts bool) (map[string]any, error) {
	out := map[string]any{}
	for _, encodedPath := range document.Fields() {
		path, err := decodePathKey(encodedPath)
		if err != nil {
			return nil, err
		}
		values := document.Values(path...)
		if len(values) == 0 {
			continue
		}
		key := strings.Join(path, ".")
		if includeConflicts && len(values) > 1 {
			conflicts := make([]any, len(values))
			for index, value := range values {
				conflicts[index], err = document.exportValue(value)
				if err != nil {
					return nil, err
				}
			}
			out[key] = map[string]any{"$conflict": conflicts}
			continue
		}
		out[key], err = document.exportValue(values[len(values)-1])
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}

// exportValue resolves a top-level visible sequence reference to its immutable
// projection. Nested container references remain stable references, avoiding
// accidental recursive expansion of arbitrary object graphs.
func (document DocumentView) exportValue(value Value) (any, error) {
	if value.Kind != ListRefValue && value.Kind != TextRefValue {
		return value, nil
	}
	sequence := document.doc.Lists[value.Object]
	if sequence == nil {
		return nil, fmt.Errorf("%w: missing sequence object", ErrCorruption)
	}
	view := ListView{sequence: sequence}
	if sequence.Kind == TextObject {
		text, ok := view.Text()
		if !ok {
			return nil, fmt.Errorf("%w: invalid text sequence", ErrCorruption)
		}
		return text, nil
	}
	elements := view.Elements()
	projection := make([]Value, len(elements))
	for index, element := range elements {
		projection[index] = element.Value
	}
	return projection, nil
}

func (d DocumentView) MarshalJSON() ([]byte, error) {
	value, err := ExportDocument(d, true)
	if err != nil {
		return nil, err
	}
	return json.Marshal(value)
}

func decodePathKey(key string) ([]string, error) {
	parts := make([]string, 0, 1)
	for key != "" {
		colon := strings.IndexByte(key, ':')
		if colon < 1 {
			return nil, fmt.Errorf("%w: path key", ErrCorruption)
		}
		length, err := strconv.Atoi(key[:colon])
		if err != nil || length < 0 || colon+1+length > len(key) {
			return nil, fmt.Errorf("%w: path key", ErrCorruption)
		}
		parts = append(parts, key[colon+1:colon+1+length])
		key = key[colon+1+length:]
	}
	return parts, nil
}
