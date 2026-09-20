package snapshot

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
)

// PayloadTransformer transforms raw snapshot payload bytes on write and read paths.
// Examples include wire compression (e.g. Gzip), envelope encryption, or framing.
// Transformers operate strictly outside the schema versioning boundary; upcasters always
// operate on restored plaintext bytes.
type PayloadTransformer interface {
	// Transform applies the transformation to plaintext payload bytes before storage (e.g., compress).
	Transform(ctx context.Context, payload []byte) ([]byte, error)

	// Restore reverses the transformation on stored bytes to produce plaintext bytes (e.g., decompress).
	Restore(ctx context.Context, payload []byte) ([]byte, error)
}

type gzipTransformer struct {
	level int
}

// Gzip returns a PayloadTransformer that compresses payloads using gzip.
// An optional compression level can be provided (defaulting to gzip.DefaultCompression).
func Gzip(level ...int) PayloadTransformer {
	lvl := gzip.DefaultCompression
	if len(level) > 0 {
		lvl = level[0]
	}
	return &gzipTransformer{level: lvl}
}

func (g *gzipTransformer) Transform(_ context.Context, payload []byte) ([]byte, error) {
	var buf bytes.Buffer
	w, err := gzip.NewWriterLevel(&buf, g.level)
	if err != nil {
		return nil, fmt.Errorf("gzip new writer failed: %w", err)
	}
	if _, err := w.Write(payload); err != nil {
		_ = w.Close()
		return nil, fmt.Errorf("gzip write failed: %w", err)
	}
	if err := w.Close(); err != nil {
		return nil, fmt.Errorf("gzip close failed: %w", err)
	}
	return buf.Bytes(), nil
}

func (g *gzipTransformer) Restore(_ context.Context, payload []byte) ([]byte, error) {
	r, err := gzip.NewReader(bytes.NewReader(payload))
	if err != nil {
		return nil, fmt.Errorf("gzip new reader failed: %w", err)
	}
	defer r.Close()

	decompressed, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("gzip read failed: %w", err)
	}
	return decompressed, nil
}

type funcTransformer struct {
	transform func(ctx context.Context, payload []byte) ([]byte, error)
	restore   func(ctx context.Context, payload []byte) ([]byte, error)
}

func (f *funcTransformer) Transform(ctx context.Context, payload []byte) ([]byte, error) {
	if f.transform == nil {
		return nil, fmt.Errorf("transform func cannot be nil")
	}
	return f.transform(ctx, payload)
}

func (f *funcTransformer) Restore(ctx context.Context, payload []byte) ([]byte, error) {
	if f.restore == nil {
		return nil, fmt.Errorf("restore func cannot be nil")
	}
	return f.restore(ctx, payload)
}

// FuncTransformer creates a PayloadTransformer from explicit transform and restore functions.
func FuncTransformer(
	transform func(ctx context.Context, payload []byte) ([]byte, error),
	restore func(ctx context.Context, payload []byte) ([]byte, error),
) PayloadTransformer {
	return &funcTransformer{
		transform: transform,
		restore:   restore,
	}
}
