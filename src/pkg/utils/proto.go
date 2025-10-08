package utils

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/go-faster/jx"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/structpb"
	"strconv"
)

// JXMapToAnyMap converts map[string]jx.Raw to map[string]*anypb.Any
func JXMapToAnyMap(m map[string]jx.Raw) (map[string]*anypb.Any, error) {
	out := make(map[string]*anypb.Any, len(m))

	for k, raw := range m {
		var v any
		if err := json.Unmarshal(raw, &v); err != nil {
			return nil, fmt.Errorf("key %q: %w", k, err)
		}

		pbVal, err := structpb.NewValue(v)
		if err != nil {
			return nil, fmt.Errorf("key %q: %w", k, err)
		}

		anyVal, err := anypb.New(pbVal)
		if err != nil {
			return nil, fmt.Errorf("key %q: %w", k, err)
		}

		out[k] = anyVal
	}

	return out, nil
}

func AnyMapToStringMap(m map[string]*anypb.Any) (map[string]string, error) {
	out := make(map[string]string, len(m))
	for k, anyVal := range m {
		var val float64
		buf := bytes.NewReader(anyVal.Value)
		err := binary.Read(buf, binary.BigEndian, &val)
		if err != nil {
			return nil, fmt.Errorf("key %q: %w", k, err)
		}
		out[k] = strconv.FormatFloat(val, 'E', -1, 64)
	}
	return out, nil
}

func GoAnyMapToStringMap(m map[string]any) (map[string]string, error) {
	out := make(map[string]string, len(m))
	for k, anyVal := range m {
		v, ok := anyVal.(string)
		if ok {
			out[k] = v
			continue
		}

		v2, ok := anyVal.(float64)
		if ok {
			out[k] = strconv.FormatFloat(v2, 'E', -1, 64)
			continue
		}

		v3, ok := anyVal.(int)
		if ok {
			out[k] = strconv.Itoa(v3)
		}
	}
	return out, nil
}

// GoMapToAnyMap converts map[string]any to map[string]*anypb.Any
func GoMapToAnyMap(m map[string]any) (map[string]*anypb.Any, error) {
	out := make(map[string]*anypb.Any, len(m))

	for k, v := range m {
		pbVal, err := structpb.NewValue(v)
		if err != nil {
			return nil, fmt.Errorf("key %q: %w", k, err)
		}

		anyVal, err := anypb.New(pbVal)
		if err != nil {
			return nil, fmt.Errorf("key %q: %w", k, err)
		}

		out[k] = anyVal
	}

	return out, nil
}
