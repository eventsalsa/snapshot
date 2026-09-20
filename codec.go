package snapshot

import (
	"encoding/json"
	"fmt"
)

// Codec converts a state value to and from its payload bytes.
type Codec[T any] interface {
	// Encode returns the serialized payload for state.
	Encode(state T) ([]byte, error)

	// Decode returns the state deserialized from payload. streamID is the target stream ID,
	// allowing implementations to validate that the decoded state belongs to the requested stream.
	Decode(streamID string, payload []byte) (T, error)
}

type jsonCodec[T any] struct{}

func (j jsonCodec[T]) Encode(state T) ([]byte, error) {
	return json.Marshal(state)
}

func (j jsonCodec[T]) Decode(_ string, payload []byte) (T, error) {
	var state T
	if err := json.Unmarshal(payload, &state); err != nil {
		return state, err
	}
	return state, nil
}

// JSON returns a default Codec backed by standard library encoding/json.
// It handles both value and pointer state types and automatically honors
// json.Marshaler and json.Unmarshaler implementations.
func JSON[T any]() Codec[T] {
	return jsonCodec[T]{}
}

type funcCodec[T any] struct {
	encode func(state T) ([]byte, error)
	decode func(streamID string, payload []byte) (T, error)
}

func (f funcCodec[T]) Encode(state T) ([]byte, error) {
	if f.encode == nil {
		return nil, fmt.Errorf("encode func cannot be nil")
	}
	return f.encode(state)
}

func (f funcCodec[T]) Decode(streamID string, payload []byte) (T, error) {
	if f.decode == nil {
		var zero T
		return zero, fmt.Errorf("decode func cannot be nil")
	}
	return f.decode(streamID, payload)
}

// FuncCodec creates a Codec from explicit encode and decode functions.
func FuncCodec[T any](
	encode func(state T) ([]byte, error),
	decode func(streamID string, payload []byte) (T, error),
) Codec[T] {
	return funcCodec[T]{
		encode: encode,
		decode: decode,
	}
}
