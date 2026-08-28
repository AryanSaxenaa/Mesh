package mesh0

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math"
)

type ValueKind uint8

const (
	NullValue ValueKind = iota
	BoolValue
	IntValue
	FloatValue
	StringValue
	BytesValue
	ListRefValue
	TextRefValue
)

// Value is a bounded scalar or a stable reference to an anchored list/text object.
type Value struct {
	Kind   ValueKind
	Bool   bool
	Int    int64
	Float  float64
	Text   string
	Bytes  []byte
	Object ObjectID
}

func Null() Value               { return Value{Kind: NullValue} }
func Bool(v bool) Value         { return Value{Kind: BoolValue, Bool: v} }
func Int(v int64) Value         { return Value{Kind: IntValue, Int: v} }
func Float(v float64) Value     { return Value{Kind: FloatValue, Float: v} }
func String(v string) Value     { return Value{Kind: StringValue, Text: v} }
func Bytes(v []byte) Value      { return Value{Kind: BytesValue, Bytes: append([]byte(nil), v...)} }
func ListRef(id ObjectID) Value { return Value{Kind: ListRefValue, Object: id} }
func TextRef(id ObjectID) Value { return Value{Kind: TextRefValue, Object: id} }

func (v Value) Validate() error {
	switch v.Kind {
	case NullValue, BoolValue, IntValue:
		return nil
	case FloatValue:
		if math.IsNaN(v.Float) || math.IsInf(v.Float, 0) {
			return fmt.Errorf("%w: non-finite float", ErrInvalidArgument)
		}
		return nil
	case StringValue:
		if len(v.Text) > maxStringBytes {
			return ErrResourceLimit
		}
		return nil
	case BytesValue:
		if len(v.Bytes) > maxStringBytes {
			return ErrResourceLimit
		}
		return nil
	case ListRefValue, TextRefValue:
		if !v.Object.valid() {
			return fmt.Errorf("%w: object reference", ErrInvalidArgument)
		}
		return nil
	default:
		return fmt.Errorf("%w: unknown value kind", ErrInvalidArgument)
	}
}

func (v Value) Equal(other Value) bool {
	if v.Kind != other.Kind {
		return false
	}
	switch v.Kind {
	case NullValue:
		return true
	case BoolValue:
		return v.Bool == other.Bool
	case IntValue:
		return v.Int == other.Int
	case FloatValue:
		return v.Float == other.Float
	case StringValue:
		return v.Text == other.Text
	case BytesValue:
		return string(v.Bytes) == string(other.Bytes)
	case ListRefValue, TextRefValue:
		return v.Object == other.Object
	}
	return false
}
func (v Value) Key() string { encoded, _ := v.MarshalJSON(); return string(encoded) }
func (v Value) MarshalJSON() ([]byte, error) {
	switch v.Kind {
	case NullValue:
		return []byte("null"), nil
	case BoolValue:
		return json.Marshal(v.Bool)
	case IntValue:
		return json.Marshal(v.Int)
	case FloatValue:
		return json.Marshal(v.Float)
	case StringValue:
		return json.Marshal(v.Text)
	case BytesValue:
		return json.Marshal(map[string]string{"$bytes": base64.StdEncoding.EncodeToString(v.Bytes)})
	case ListRefValue:
		return json.Marshal(map[string]string{"$list": v.Object.String()})
	case TextRefValue:
		return json.Marshal(map[string]string{"$text": v.Object.String()})
	}
	return nil, fmt.Errorf("unknown value kind")
}
func (v Value) String() string {
	encoded, err := v.MarshalJSON()
	if err != nil {
		return "<invalid>"
	}
	return string(encoded)
}
func (v Value) encode(encoded *encoder) error {
	if err := v.Validate(); err != nil {
		return err
	}
	encoded.u(uint64(v.Kind))
	switch v.Kind {
	case BoolValue:
		if v.Bool {
			encoded.u(1)
		} else {
			encoded.u(0)
		}
	case IntValue:
		encoded.i(v.Int)
	case FloatValue:
		return appendFloat(encoded, v.Float)
	case StringValue:
		encoded.str(v.Text)
	case BytesValue:
		encoded.bytes(v.Bytes)
	case ListRefValue, TextRefValue:
		encoded.dot(v.Object.Dot)
	}
	return nil
}
func decodeValue(decoded *decoder) (Value, error) {
	kind, err := decoded.u()
	if err != nil {
		return Value{}, err
	}
	value := Value{Kind: ValueKind(kind)}
	switch value.Kind {
	case NullValue:
	case BoolValue:
		boolean, err := decoded.u()
		if err != nil {
			return value, err
		}
		if boolean > 1 {
			return value, ErrCorruption
		}
		value.Bool = boolean == 1
	case IntValue:
		value.Int, err = decoded.i()
	case FloatValue:
		value.Float, err = readFloat(decoded)
	case StringValue:
		value.Text, err = decoded.str(maxStringBytes)
	case BytesValue:
		value.Bytes, err = decoded.bytes(maxStringBytes)
	case ListRefValue, TextRefValue:
		value.Object.Dot, err = decoded.dot()
	default:
		return value, ErrCorruption
	}
	if err != nil {
		return value, err
	}
	return value, value.Validate()
}

func ParseValue(input string) (Value, error) {
	var parsed any
	if err := json.Unmarshal([]byte(input), &parsed); err != nil {
		return String(input), nil
	}
	switch value := parsed.(type) {
	case nil:
		return Null(), nil
	case bool:
		return Bool(value), nil
	case float64:
		if math.Trunc(value) == value && value >= math.MinInt64 && value <= math.MaxInt64 {
			return Int(int64(value)), nil
		}
		return Float(value), nil
	case string:
		return String(value), nil
	case map[string]any:
		if raw, ok := value["$bytes"].(string); ok && len(value) == 1 {
			bytes, err := base64.StdEncoding.DecodeString(raw)
			return Bytes(bytes), err
		}
	}
	return Value{}, fmt.Errorf("%w: scalar JSON value required", ErrInvalidArgument)
}
