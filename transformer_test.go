package snapshot_test

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/eventsalsa/store"
	"github.com/jackc/pgx/v5"

	"github.com/eventsalsa/snapshot"
)

func TestGzipTransformer(t *testing.T) {
	ctx := context.Background()

	t.Run("default compression roundtrip", func(t *testing.T) {
		gt := snapshot.Gzip()
		original := []byte(strings.Repeat("payload data to be compressed with gzip ", 50))

		compressed, err := gt.Transform(ctx, original)
		if err != nil {
			t.Fatalf("Transform failed: %v", err)
		}

		if len(compressed) < 2 || compressed[0] != 0x1f || compressed[1] != 0x8b {
			t.Errorf("expected gzip magic bytes, got %x %x", compressed[0], compressed[1])
		}

		if len(compressed) >= len(original) {
			t.Errorf("expected compression to reduce size: original %d, compressed %d", len(original), len(compressed))
		}

		restored, err := gt.Restore(ctx, compressed)
		if err != nil {
			t.Fatalf("Restore failed: %v", err)
		}

		if !bytes.Equal(restored, original) {
			t.Errorf("restored payload does not match original")
		}
	})

	t.Run("custom compression level", func(t *testing.T) {
		gtBestSpeed := snapshot.Gzip(gzip.BestSpeed)
		gtBestCompression := snapshot.Gzip(gzip.BestCompression)
		original := []byte(strings.Repeat("test repetitively compressible text data ", 100))

		speedCompressed, err := gtBestSpeed.Transform(ctx, original)
		if err != nil {
			t.Fatalf("Transform (BestSpeed) failed: %v", err)
		}

		bestCompressed, err := gtBestCompression.Transform(ctx, original)
		if err != nil {
			t.Fatalf("Transform (BestCompression) failed: %v", err)
		}

		restoredSpeed, err := gtBestSpeed.Restore(ctx, speedCompressed)
		if err != nil {
			t.Fatalf("Restore (BestSpeed) failed: %v", err)
		}
		if !bytes.Equal(restoredSpeed, original) {
			t.Errorf("restored payload mismatch for BestSpeed")
		}

		restoredBest, err := gtBestCompression.Restore(ctx, bestCompressed)
		if err != nil {
			t.Fatalf("Restore (BestCompression) failed: %v", err)
		}
		if !bytes.Equal(restoredBest, original) {
			t.Errorf("restored payload mismatch for BestCompression")
		}
	})

	t.Run("invalid compression level returns error on transform", func(t *testing.T) {
		gt := snapshot.Gzip(9999)
		_, err := gt.Transform(ctx, []byte("data"))
		if err == nil || !strings.Contains(err.Error(), "gzip new writer failed") {
			t.Errorf("expected gzip new writer error, got: %v", err)
		}
	})

	t.Run("restore non-gzip payload returns error", func(t *testing.T) {
		gt := snapshot.Gzip()
		_, err := gt.Restore(ctx, []byte("this is not gzipped"))
		if err == nil || !strings.Contains(err.Error(), "gzip new reader failed") {
			t.Errorf("expected gzip new reader error, got: %v", err)
		}
	})

	t.Run("restore truncated gzip payload returns error", func(t *testing.T) {
		gt := snapshot.Gzip()
		compressed, err := gt.Transform(ctx, []byte(strings.Repeat("hello world", 20)))
		if err != nil {
			t.Fatalf("Transform failed: %v", err)
		}
		truncated := compressed[:len(compressed)/2]
		_, err = gt.Restore(ctx, truncated)
		if err == nil || !strings.Contains(err.Error(), "gzip read failed") {
			t.Errorf("expected gzip read failed error, got: %v", err)
		}
	})
}

func TestFuncTransformer(t *testing.T) {
	ctx := context.Background()

	t.Run("successful transform and restore", func(t *testing.T) {
		ft := snapshot.FuncTransformer(
			func(_ context.Context, payload []byte) ([]byte, error) {
				return append([]byte("prefix:"), payload...), nil
			},
			func(_ context.Context, payload []byte) ([]byte, error) {
				if !bytes.HasPrefix(payload, []byte("prefix:")) {
					return nil, errors.New("missing prefix")
				}
				return bytes.TrimPrefix(payload, []byte("prefix:")), nil
			},
		)

		transformed, err := ft.Transform(ctx, []byte("hello"))
		if err != nil {
			t.Fatalf("Transform failed: %v", err)
		}
		if string(transformed) != "prefix:hello" {
			t.Errorf("expected 'prefix:hello', got %q", string(transformed))
		}

		restored, err := ft.Restore(ctx, transformed)
		if err != nil {
			t.Fatalf("Restore failed: %v", err)
		}
		if string(restored) != "hello" {
			t.Errorf("expected 'hello', got %q", string(restored))
		}
	})

	t.Run("nil transform function returns error", func(t *testing.T) {
		ft := snapshot.FuncTransformer(nil, func(_ context.Context, payload []byte) ([]byte, error) {
			return payload, nil
		})
		_, err := ft.Transform(ctx, []byte("hello"))
		if err == nil || !strings.Contains(err.Error(), "transform func cannot be nil") {
			t.Errorf("expected nil transform error, got: %v", err)
		}
	})

	t.Run("nil restore function returns error", func(t *testing.T) {
		ft := snapshot.FuncTransformer(func(_ context.Context, payload []byte) ([]byte, error) {
			return payload, nil
		}, nil)
		_, err := ft.Restore(ctx, []byte("hello"))
		if err == nil || !strings.Contains(err.Error(), "restore func cannot be nil") {
			t.Errorf("expected nil restore error, got: %v", err)
		}
	})
}

func TestRepository_Transformers_RoundTrip(t *testing.T) {
	ctx := context.Background()

	cfg := defaultTestConfig()
	cfg.Transformers = []snapshot.PayloadTransformer{snapshot.Gzip()}

	var storedSnapshot snapshot.Snapshot
	snapStore := &mockSnapshotStore{
		putFunc: func(_ context.Context, _ pgx.Tx, snap *snapshot.Snapshot) error {
			storedSnapshot = *snap
			return nil
		},
		getFunc: func(_ context.Context, _ pgx.Tx, _, _ string, _ int) (snapshot.Snapshot, error) {
			return storedSnapshot, nil
		},
	}

	reader := &mockStreamReader{
		readStreamFunc: func(_ context.Context, _ pgx.Tx, streamType, streamID string, fromVersion, toVersion *int64) (store.Stream, error) {
			if fromVersion != nil && toVersion != nil && *fromVersion == 5 && *toVersion == 5 {
				return store.Stream{
					StreamType: streamType,
					StreamID:   streamID,
					Events: []store.PersistedEvent{
						{StreamType: streamType, StreamID: streamID, StreamVersion: 5},
					},
				}, nil
			}
			return store.Stream{StreamType: streamType, StreamID: streamID}, nil
		},
	}

	repo, err := snapshot.NewRepository(reader, snapStore, cfg)
	if err != nil {
		t.Fatalf("NewRepository: %v", err)
	}

	origState := &userState{ID: "user-1", Name: "Alice", Email: "alice@example.com"}
	if err := repo.Save(ctx, nil, "user-1", 5, origState); err != nil {
		t.Fatalf("Save: %v", err)
	}

	if len(storedSnapshot.Payload) < 2 || storedSnapshot.Payload[0] != 0x1f || storedSnapshot.Payload[1] != 0x8b {
		t.Fatalf("expected stored snapshot payload to be gzipped, got %x %x", storedSnapshot.Payload[0], storedSnapshot.Payload[1])
	}

	res, err := repo.Load(ctx, nil, "user-1")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	if !res.SnapshotHit {
		t.Errorf("expected SnapshotHit = true")
	}
	if res.SnapshotVersion != 5 {
		t.Errorf("expected SnapshotVersion = 5, got %d", res.SnapshotVersion)
	}
	if res.StreamVersion != 5 {
		t.Errorf("expected StreamVersion = 5, got %d", res.StreamVersion)
	}
	if res.State.Name != "Alice" || res.State.Email != "alice@example.com" {
		t.Errorf("unexpected loaded state: %+v", res.State)
	}
}

func TestRepository_Transformers_Ordering(t *testing.T) {
	ctx := context.Background()

	t1 := snapshot.FuncTransformer(
		func(_ context.Context, payload []byte) ([]byte, error) {
			return append([]byte("[T1]"), append(payload, []byte("[/T1]")...)...), nil
		},
		func(_ context.Context, payload []byte) ([]byte, error) {
			if !bytes.HasPrefix(payload, []byte("[T1]")) || !bytes.HasSuffix(payload, []byte("[/T1]")) {
				return nil, fmt.Errorf("T1 restore: missing [T1] wrapper, got %q", string(payload))
			}
			trimmed := bytes.TrimPrefix(payload, []byte("[T1]"))
			trimmed = bytes.TrimSuffix(trimmed, []byte("[/T1]"))
			return trimmed, nil
		},
	)

	t2 := snapshot.FuncTransformer(
		func(_ context.Context, payload []byte) ([]byte, error) {
			return append([]byte("[T2]"), append(payload, []byte("[/T2]")...)...), nil
		},
		func(_ context.Context, payload []byte) ([]byte, error) {
			if !bytes.HasPrefix(payload, []byte("[T2]")) || !bytes.HasSuffix(payload, []byte("[/T2]")) {
				return nil, fmt.Errorf("T2 restore: missing [T2] wrapper, got %q", string(payload))
			}
			trimmed := bytes.TrimPrefix(payload, []byte("[T2]"))
			trimmed = bytes.TrimSuffix(trimmed, []byte("[/T2]"))
			return trimmed, nil
		},
	)

	cfg := defaultTestConfig()
	cfg.Transformers = []snapshot.PayloadTransformer{t1, t2}

	var storedSnapshot snapshot.Snapshot
	snapStore := &mockSnapshotStore{
		putFunc: func(_ context.Context, _ pgx.Tx, snap *snapshot.Snapshot) error {
			storedSnapshot = *snap
			return nil
		},
		getFunc: func(_ context.Context, _ pgx.Tx, _, _ string, _ int) (snapshot.Snapshot, error) {
			return storedSnapshot, nil
		},
	}

	reader := &mockStreamReader{
		readStreamFunc: func(_ context.Context, _ pgx.Tx, streamType, streamID string, fromVersion, toVersion *int64) (store.Stream, error) {
			if fromVersion != nil && toVersion != nil && *fromVersion == 3 && *toVersion == 3 {
				return store.Stream{
					StreamType: streamType,
					StreamID:   streamID,
					Events:     []store.PersistedEvent{{StreamType: streamType, StreamID: streamID, StreamVersion: 3}},
				}, nil
			}
			return store.Stream{StreamType: streamType, StreamID: streamID}, nil
		},
	}

	repo, err := snapshot.NewRepository(reader, snapStore, cfg)
	if err != nil {
		t.Fatalf("NewRepository: %v", err)
	}

	if err := repo.Save(ctx, nil, "user-order", 3, &userState{ID: "user-order", Name: "OrderTest"}); err != nil {
		t.Fatalf("Save: %v", err)
	}

	storedStr := string(storedSnapshot.Payload)
	if !strings.HasPrefix(storedStr, "[T2][T1]") || !strings.HasSuffix(storedStr, "[/T1][/T2]") {
		t.Fatalf("expected stored payload wrapped as [T2][T1]...[/T1][/T2], got %q", storedStr)
	}

	res, err := repo.Load(ctx, nil, "user-order")
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	if res.State.Name != "OrderTest" {
		t.Errorf("expected state name 'OrderTest', got %q", res.State.Name)
	}
}

func TestRepository_Transformers_WithUpcasters(t *testing.T) {
	ctx := context.Background()

	gt := snapshot.Gzip()

	s1Plaintext := []byte(`{"id":"u-upcast","name":"OldName"}`)
	s1Compressed, err := gt.Transform(ctx, s1Plaintext)
	if err != nil {
		t.Fatalf("compress s1: %v", err)
	}

	snapStore := &mockSnapshotStore{
		getFunc: func(_ context.Context, _ pgx.Tx, streamType, streamID string, _ int) (snapshot.Snapshot, error) {
			return snapshot.Snapshot{
				StreamType:    streamType,
				StreamID:      streamID,
				StreamVersion: 5,
				SchemaVersion: 1,
				Payload:       s1Compressed,
			}, nil
		},
	}

	reader := &mockStreamReader{
		readStreamFunc: func(_ context.Context, _ pgx.Tx, streamType, streamID string, fromVersion, toVersion *int64) (store.Stream, error) {
			if fromVersion != nil && toVersion != nil && *fromVersion == 5 && *toVersion == 5 {
				return store.Stream{
					StreamType: streamType,
					StreamID:   streamID,
					Events:     []store.PersistedEvent{{StreamType: streamType, StreamID: streamID, StreamVersion: 5}},
				}, nil
			}
			return store.Stream{StreamType: streamType, StreamID: streamID}, nil
		},
	}

	var upcasterReceivedPayload []byte
	cfg := defaultTestConfig()
	cfg.SchemaVersion = 2
	cfg.Transformers = []snapshot.PayloadTransformer{snapshot.Gzip()}
	cfg.Upcasters = map[int]snapshot.Upcaster{
		1: func(_ int, payload []byte) ([]byte, error) {
			upcasterReceivedPayload = payload
			var data map[string]any
			if err := json.Unmarshal(payload, &data); err != nil {
				return nil, fmt.Errorf("upcaster received non-JSON/compressed payload: %w", err)
			}
			name, ok := data["name"].(string)
			if !ok {
				return nil, errors.New("name field is not a string")
			}
			data["name"] = name + "Upcasted"
			return json.Marshal(data)
		},
	}

	repo, err := snapshot.NewRepository(reader, snapStore, cfg)
	if err != nil {
		t.Fatalf("NewRepository: %v", err)
	}

	res, err := repo.Load(ctx, nil, "u-upcast")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	if !res.SnapshotHit {
		t.Errorf("expected SnapshotHit = true")
	}
	if !res.Upcasted {
		t.Errorf("expected Upcasted = true")
	}
	if res.SnapshotSchemaVersion != 1 {
		t.Errorf("expected SnapshotSchemaVersion = 1, got %d", res.SnapshotSchemaVersion)
	}
	if res.State.Name != "OldNameUpcasted" {
		t.Errorf("expected 'OldNameUpcasted', got %q", res.State.Name)
	}
	if !bytes.Equal(upcasterReceivedPayload, s1Plaintext) {
		t.Errorf("expected upcaster to receive plaintext %q, got %q", string(s1Plaintext), string(upcasterReceivedPayload))
	}
}

func TestRepository_Transformers_ErrorHandling(t *testing.T) {
	ctx := context.Background()

	t.Run("Save returns error when transformer fails", func(t *testing.T) {
		putCalled := false
		snapStore := &mockSnapshotStore{
			putFunc: func(_ context.Context, _ pgx.Tx, _ *snapshot.Snapshot) error {
				putCalled = true
				return nil
			},
		}

		reader := &mockStreamReader{
			readStreamFunc: func(_ context.Context, _ pgx.Tx, streamType, streamID string, _, _ *int64) (store.Stream, error) {
				return store.Stream{
					StreamType: streamType,
					StreamID:   streamID,
					Events:     []store.PersistedEvent{{StreamType: streamType, StreamID: streamID, StreamVersion: 1}},
				}, nil
			},
		}

		cfg := defaultTestConfig()
		cfg.Transformers = []snapshot.PayloadTransformer{
			snapshot.FuncTransformer(
				func(_ context.Context, _ []byte) ([]byte, error) {
					return nil, errors.New("simulated transform failure")
				},
				func(_ context.Context, payload []byte) ([]byte, error) {
					return payload, nil
				},
			),
		}

		repo, err := snapshot.NewRepository(reader, snapStore, cfg)
		if err != nil {
			t.Fatalf("NewRepository: %v", err)
		}

		err = repo.Save(ctx, nil, "u-1", 1, &userState{ID: "u-1", Name: "Test"})
		if err == nil || !strings.Contains(err.Error(), "transformer 0 failed to transform snapshot payload") {
			t.Errorf("expected transformer error on save, got: %v", err)
		}
		if putCalled {
			t.Errorf("expected snapshot store Put not to be called on transform failure")
		}
	})

	t.Run("Load falls back to stream replay on restore error by default", func(t *testing.T) {
		var rejectedReason string
		var rejectedErr error

		snapStore := &mockSnapshotStore{
			getFunc: func(_ context.Context, _ pgx.Tx, streamType, streamID string, _ int) (snapshot.Snapshot, error) {
				return snapshot.Snapshot{
					StreamType:    streamType,
					StreamID:      streamID,
					StreamVersion: 10,
					SchemaVersion: 1,
					Payload:       []byte("corrupt-data"),
				}, nil
			},
		}

		reader := &mockStreamReader{
			readStreamFunc: func(_ context.Context, _ pgx.Tx, streamType, streamID string, fromVersion, _ *int64) (store.Stream, error) {
				if fromVersion == nil {
					evPayload, err := json.Marshal(userEvent{Name: "ReplayedFromEvents"})
					if err != nil {
						return store.Stream{}, err
					}
					return store.Stream{
						StreamType: streamType,
						StreamID:   streamID,
						Events: []store.PersistedEvent{
							{StreamType: streamType, StreamID: streamID, StreamVersion: 1, Payload: evPayload},
						},
					}, nil
				}
				return store.Stream{StreamType: streamType, StreamID: streamID}, nil
			},
		}

		cfg := defaultTestConfig()
		cfg.Transformers = []snapshot.PayloadTransformer{snapshot.Gzip()}
		cfg.OnSnapshotRejected = func(_ context.Context, _, reason string, err error) {
			rejectedReason = reason
			rejectedErr = err
		}

		repo, err := snapshot.NewRepository(reader, snapStore, cfg)
		if err != nil {
			t.Fatalf("NewRepository: %v", err)
		}

		res, err := repo.Load(ctx, nil, "u-corrupt")
		if err != nil {
			t.Fatalf("Load fallback failed: %v", err)
		}
		if res.SnapshotHit {
			t.Errorf("expected SnapshotHit = false")
		}
		if res.State.Name != "ReplayedFromEvents" {
			t.Errorf("expected state from replay 'ReplayedFromEvents', got %q", res.State.Name)
		}
		if !strings.Contains(rejectedReason, "transformer 0 failed to restore snapshot payload") {
			t.Errorf("expected rejected reason for transformer restore, got %q", rejectedReason)
		}
		if rejectedErr == nil {
			t.Errorf("expected non-nil rejectedErr")
		}
	})

	t.Run("Load fails fast on restore error when FailOnCorruptSnapshot=true", func(t *testing.T) {
		snapStore := &mockSnapshotStore{
			getFunc: func(_ context.Context, _ pgx.Tx, streamType, streamID string, _ int) (snapshot.Snapshot, error) {
				return snapshot.Snapshot{
					StreamType:    streamType,
					StreamID:      streamID,
					StreamVersion: 10,
					SchemaVersion: 1,
					Payload:       []byte("corrupt-data"),
				}, nil
			},
		}

		reader := &mockStreamReader{}
		cfg := defaultTestConfig()
		cfg.FailOnCorruptSnapshot = true
		cfg.Transformers = []snapshot.PayloadTransformer{snapshot.Gzip()}

		repo, err := snapshot.NewRepository(reader, snapStore, cfg)
		if err != nil {
			t.Fatalf("NewRepository: %v", err)
		}

		_, err = repo.Load(ctx, nil, "u-corrupt")
		if err == nil || !strings.Contains(err.Error(), "transformer 0 failed to restore snapshot payload") {
			t.Errorf("expected error containing 'transformer 0 failed to restore snapshot payload', got: %v", err)
		}
	})
}
