package snapshot_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/eventsalsa/store"
	"github.com/jackc/pgx/v5"

	"github.com/eventsalsa/snapshot"
)

func TestRepository_Upcasters(t *testing.T) {
	ctx := context.Background()

	t.Run("single-step upcast 1 -> 2 succeeds", func(t *testing.T) {
		cfg := defaultTestConfig()
		cfg.SchemaVersion = 2
		cfg.Upcasters = map[int]snapshot.Upcaster{
			1: func(fromVer int, payload []byte) ([]byte, error) {
				if fromVer != 1 {
					t.Fatalf("expected fromVer 1, got %d", fromVer)
				}
				var raw map[string]any
				if err := json.Unmarshal(payload, &raw); err != nil {
					return nil, err
				}
				name, ok := raw["name"].(string)
				if !ok {
					return nil, errors.New("missing or invalid name")
				}
				raw["name"] = name + " (v2)"
				return json.Marshal(raw)
			},
		}

		v1Payload, err := json.Marshal(&userState{ID: "user-1", Name: "Alice", Email: "alice@example.com"})
		if err != nil {
			t.Fatalf("marshal v1 payload: %v", err)
		}
		snapStore := &mockSnapshotStore{
			getFunc: func(_ context.Context, _ pgx.Tx, streamType, streamID string, maxSchemaVersion int) (snapshot.Snapshot, error) {
				if maxSchemaVersion != 2 {
					t.Fatalf("expected maxSchemaVersion 2, got %d", maxSchemaVersion)
				}
				return snapshot.Snapshot{
					StreamType:    streamType,
					StreamID:      streamID,
					StreamVersion: 5,
					SchemaVersion: 1,
					Payload:       v1Payload,
				}, nil
			},
		}

		deltaEventPayload, err := json.Marshal(&userEvent{Email: "alice.v2@example.com"})
		if err != nil {
			t.Fatalf("marshal delta event: %v", err)
		}
		reader := &mockStreamReader{
			readStreamFunc: func(_ context.Context, _ pgx.Tx, streamType, streamID string, fromVersion, _ *int64) (store.Stream, error) {
				if fromVersion == nil || *fromVersion != 6 {
					t.Fatalf("expected fromVersion 6, got %v", fromVersion)
				}
				return store.Stream{
					StreamType: streamType,
					StreamID:   streamID,
					Events: []store.PersistedEvent{
						{StreamVersion: 6, Payload: deltaEventPayload},
					},
				}, nil
			},
		}

		repo, err := snapshot.NewRepository(reader, snapStore, cfg)
		if err != nil {
			t.Fatalf("NewRepository: %v", err)
		}

		res, err := repo.Load(ctx, nil, "user-1")
		if err != nil {
			t.Fatalf("Load: %v", err)
		}

		if !res.SnapshotHit {
			t.Error("expected SnapshotHit = true")
		}
		if !res.Upcasted {
			t.Error("expected Upcasted = true")
		}
		if res.SnapshotSchemaVersion != 1 {
			t.Errorf("expected SnapshotSchemaVersion = 1, got %d", res.SnapshotSchemaVersion)
		}
		if res.SnapshotVersion != 5 {
			t.Errorf("expected SnapshotVersion = 5, got %d", res.SnapshotVersion)
		}
		if res.StreamVersion != 6 {
			t.Errorf("expected StreamVersion = 6, got %d", res.StreamVersion)
		}
		if res.EventsReplayed != 1 {
			t.Errorf("expected EventsReplayed = 1, got %d", res.EventsReplayed)
		}
		if res.State.Name != "Alice (v2)" {
			t.Errorf("expected Name = 'Alice (v2)', got %q", res.State.Name)
		}
		if res.State.Email != "alice.v2@example.com" {
			t.Errorf("expected Email = 'alice.v2@example.com', got %q", res.State.Email)
		}
	})

	t.Run("multi-step upcast chain 1 -> 2 -> 3 succeeds", func(t *testing.T) {
		cfg := defaultTestConfig()
		cfg.SchemaVersion = 3
		cfg.Upcasters = map[int]snapshot.Upcaster{
			1: func(_ int, payload []byte) ([]byte, error) {
				var raw map[string]any
				if err := json.Unmarshal(payload, &raw); err != nil {
					return nil, err
				}
				name, ok := raw["name"].(string)
				if !ok {
					return nil, errors.New("missing or invalid name")
				}
				raw["name"] = name + " (v2)"
				return json.Marshal(raw)
			},
			2: func(_ int, payload []byte) ([]byte, error) {
				var raw map[string]any
				if err := json.Unmarshal(payload, &raw); err != nil {
					return nil, err
				}
				name, ok := raw["name"].(string)
				if !ok {
					return nil, errors.New("missing or invalid name")
				}
				raw["name"] = name + " (v3)"
				return json.Marshal(raw)
			},
		}

		v1Payload, err := json.Marshal(&userState{ID: "user-1", Name: "Bob"})
		if err != nil {
			t.Fatalf("marshal v1 payload: %v", err)
		}
		snapStore := &mockSnapshotStore{
			getFunc: func(_ context.Context, _ pgx.Tx, streamType, streamID string, _ int) (snapshot.Snapshot, error) {
				return snapshot.Snapshot{
					StreamType:    streamType,
					StreamID:      streamID,
					StreamVersion: 10,
					SchemaVersion: 1,
					Payload:       v1Payload,
				}, nil
			},
		}
		reader := &mockStreamReader{
			readStreamFunc: func(_ context.Context, _ pgx.Tx, streamType, streamID string, fromVersion, toVersion *int64) (store.Stream, error) {
				if fromVersion != nil && toVersion != nil && *fromVersion == 10 && *toVersion == 10 {
					return store.Stream{
						StreamType: streamType,
						StreamID:   streamID,
						Events:     []store.PersistedEvent{{StreamVersion: 10}},
					}, nil
				}
				if fromVersion != nil && *fromVersion == 11 {
					return store.Stream{StreamType: streamType, StreamID: streamID}, nil
				}
				t.Fatalf("unexpected ReadStream call: from=%v, to=%v", fromVersion, toVersion)
				return store.Stream{}, nil
			},
		}

		repo, err := snapshot.NewRepository(reader, snapStore, cfg)
		if err != nil {
			t.Fatalf("NewRepository: %v", err)
		}

		res, err := repo.Load(ctx, nil, "user-1")
		if err != nil {
			t.Fatalf("Load: %v", err)
		}

		if !res.SnapshotHit || !res.Upcasted {
			t.Errorf("expected SnapshotHit=true and Upcasted=true, got hit=%v, upcasted=%v", res.SnapshotHit, res.Upcasted)
		}
		if res.SnapshotSchemaVersion != 1 {
			t.Errorf("expected SnapshotSchemaVersion = 1, got %d", res.SnapshotSchemaVersion)
		}
		if res.State.Name != "Bob (v2) (v3)" {
			t.Errorf("expected Name = 'Bob (v2) (v3)', got %q", res.State.Name)
		}
	})

	t.Run("missing intermediate upcaster in chain falls back to full stream replay", func(t *testing.T) {
		logger := &mockLogger{}
		cfg := defaultTestConfig()
		cfg.SchemaVersion = 3
		cfg.Logger = logger
		cfg.Upcasters = map[int]snapshot.Upcaster{
			2: func(_ int, payload []byte) ([]byte, error) {
				return payload, nil
			},
		}

		v1Payload, err := json.Marshal(&userState{ID: "user-1", Name: "Charlie"})
		if err != nil {
			t.Fatalf("marshal v1 payload: %v", err)
		}
		snapStore := &mockSnapshotStore{
			getFunc: func(_ context.Context, _ pgx.Tx, streamType, streamID string, _ int) (snapshot.Snapshot, error) {
				return snapshot.Snapshot{
					StreamType:    streamType,
					StreamID:      streamID,
					StreamVersion: 5,
					SchemaVersion: 1,
					Payload:       v1Payload,
				}, nil
			},
		}

		eventPayload, err := json.Marshal(&userEvent{Name: "Charlie From Scratch"})
		if err != nil {
			t.Fatalf("marshal event: %v", err)
		}
		reader := &mockStreamReader{
			readStreamFunc: func(_ context.Context, _ pgx.Tx, streamType, streamID string, fromVersion, _ *int64) (store.Stream, error) {
				if fromVersion != nil {
					t.Fatalf("expected full replay fromVersion nil, got %v", fromVersion)
				}
				return store.Stream{
					StreamType: streamType,
					StreamID:   streamID,
					Events: []store.PersistedEvent{
						{StreamVersion: 1, Payload: eventPayload},
					},
				}, nil
			},
		}

		repo, err := snapshot.NewRepository(reader, snapStore, cfg)
		if err != nil {
			t.Fatalf("NewRepository: %v", err)
		}

		res, err := repo.Load(ctx, nil, "user-1")
		if err != nil {
			t.Fatalf("Load: %v", err)
		}

		if res.SnapshotHit {
			t.Error("expected SnapshotHit = false due to missing upcaster")
		}
		if res.Upcasted {
			t.Error("expected Upcasted = false")
		}
		if res.SnapshotSchemaVersion != 0 {
			t.Errorf("expected SnapshotSchemaVersion = 0, got %d", res.SnapshotSchemaVersion)
		}
		if res.State.Name != "Charlie From Scratch" {
			t.Errorf("expected state from full replay, got %q", res.State.Name)
		}
		if logger.debugCalls == 0 {
			t.Error("expected debug log for missing upcaster in chain")
		}
	})

	t.Run("upcaster error falls back to full replay in resilient mode", func(t *testing.T) {
		logger := &mockLogger{}
		cfg := defaultTestConfig()
		cfg.SchemaVersion = 2
		cfg.Logger = logger
		cfg.FailOnCorruptSnapshot = false
		cfg.Upcasters = map[int]snapshot.Upcaster{
			1: func(_ int, _ []byte) ([]byte, error) {
				return nil, errors.New("upcast computation error")
			},
		}

		v1Payload, err := json.Marshal(&userState{ID: "user-1", Name: "Dave"})
		if err != nil {
			t.Fatalf("marshal v1 payload: %v", err)
		}
		snapStore := &mockSnapshotStore{
			getFunc: func(_ context.Context, _ pgx.Tx, streamType, streamID string, _ int) (snapshot.Snapshot, error) {
				return snapshot.Snapshot{
					StreamType:    streamType,
					StreamID:      streamID,
					StreamVersion: 4,
					SchemaVersion: 1,
					Payload:       v1Payload,
				}, nil
			},
		}
		eventPayload, err := json.Marshal(&userEvent{Name: "Dave Replayed"})
		if err != nil {
			t.Fatalf("marshal event: %v", err)
		}
		reader := &mockStreamReader{
			readStreamFunc: func(_ context.Context, _ pgx.Tx, streamType, streamID string, fromVersion, _ *int64) (store.Stream, error) {
				if fromVersion != nil {
					t.Fatalf("expected full replay fromVersion nil, got %v", fromVersion)
				}
				return store.Stream{
					StreamType: streamType,
					StreamID:   streamID,
					Events: []store.PersistedEvent{
						{StreamVersion: 1, Payload: eventPayload},
					},
				}, nil
			},
		}

		repo, err := snapshot.NewRepository(reader, snapStore, cfg)
		if err != nil {
			t.Fatalf("NewRepository: %v", err)
		}

		res, err := repo.Load(ctx, nil, "user-1")
		if err != nil {
			t.Fatalf("Load: %v", err)
		}

		if res.SnapshotHit {
			t.Error("expected SnapshotHit = false")
		}
		if res.State.Name != "Dave Replayed" {
			t.Errorf("expected full replay state, got %q", res.State.Name)
		}
		if logger.errorCalls == 0 {
			t.Error("expected error log for upcaster failure")
		}
	})

	t.Run("upcaster error propagates in strict mode", func(t *testing.T) {
		cfg := defaultTestConfig()
		cfg.SchemaVersion = 2
		cfg.FailOnCorruptSnapshot = true
		cfg.Upcasters = map[int]snapshot.Upcaster{
			1: func(_ int, _ []byte) ([]byte, error) {
				return nil, errors.New("boom upcast")
			},
		}

		v1Payload, err := json.Marshal(&userState{ID: "user-1", Name: "Eve"})
		if err != nil {
			t.Fatalf("marshal v1 payload: %v", err)
		}
		snapStore := &mockSnapshotStore{
			getFunc: func(_ context.Context, _ pgx.Tx, streamType, streamID string, _ int) (snapshot.Snapshot, error) {
				return snapshot.Snapshot{
					StreamType:    streamType,
					StreamID:      streamID,
					StreamVersion: 4,
					SchemaVersion: 1,
					Payload:       v1Payload,
				}, nil
			},
		}
		reader := &mockStreamReader{}
		repo, err := snapshot.NewRepository(reader, snapStore, cfg)
		if err != nil {
			t.Fatalf("NewRepository: %v", err)
		}

		_, err = repo.Load(ctx, nil, "user-1")
		if err == nil || !strings.Contains(err.Error(), "failed to upcast snapshot from schema version 1") {
			t.Fatalf("expected upcast error propagation, got: %v", err)
		}
	})

	t.Run("upcasted payload unmarshal error falls back to full replay in resilient mode", func(t *testing.T) {
		logger := &mockLogger{}
		cfg := defaultTestConfig()
		cfg.SchemaVersion = 2
		cfg.Logger = logger
		cfg.FailOnCorruptSnapshot = false
		cfg.Upcasters = map[int]snapshot.Upcaster{
			1: func(_ int, _ []byte) ([]byte, error) {
				return []byte("{invalid-json"), nil
			},
		}

		v1Payload, err := json.Marshal(&userState{ID: "user-1", Name: "Frank"})
		if err != nil {
			t.Fatalf("marshal v1 payload: %v", err)
		}
		snapStore := &mockSnapshotStore{
			getFunc: func(_ context.Context, _ pgx.Tx, streamType, streamID string, _ int) (snapshot.Snapshot, error) {
				return snapshot.Snapshot{
					StreamType:    streamType,
					StreamID:      streamID,
					StreamVersion: 4,
					SchemaVersion: 1,
					Payload:       v1Payload,
				}, nil
			},
		}
		eventPayload, err := json.Marshal(&userEvent{Name: "Frank Replayed"})
		if err != nil {
			t.Fatalf("marshal event: %v", err)
		}
		reader := &mockStreamReader{
			readStreamFunc: func(_ context.Context, _ pgx.Tx, streamType, streamID string, fromVersion, _ *int64) (store.Stream, error) {
				if fromVersion != nil {
					t.Fatalf("expected full replay fromVersion nil, got %v", fromVersion)
				}
				return store.Stream{
					StreamType: streamType,
					StreamID:   streamID,
					Events: []store.PersistedEvent{
						{StreamVersion: 1, Payload: eventPayload},
					},
				}, nil
			},
		}

		repo, err := snapshot.NewRepository(reader, snapStore, cfg)
		if err != nil {
			t.Fatalf("NewRepository: %v", err)
		}

		res, err := repo.Load(ctx, nil, "user-1")
		if err != nil {
			t.Fatalf("Load: %v", err)
		}

		if res.SnapshotHit {
			t.Error("expected SnapshotHit = false")
		}
		if res.State.Name != "Frank Replayed" {
			t.Errorf("expected full replay state, got %q", res.State.Name)
		}
		if logger.errorCalls == 0 {
			t.Error("expected error log for unmarshal failure")
		}
	})

	t.Run("upcasted payload unmarshal error propagates in strict mode", func(t *testing.T) {
		cfg := defaultTestConfig()
		cfg.SchemaVersion = 2
		cfg.FailOnCorruptSnapshot = true
		cfg.Upcasters = map[int]snapshot.Upcaster{
			1: func(_ int, _ []byte) ([]byte, error) {
				return []byte("{invalid-json"), nil
			},
		}

		v1Payload, err := json.Marshal(&userState{ID: "user-1", Name: "Grace"})
		if err != nil {
			t.Fatalf("marshal v1 payload: %v", err)
		}
		snapStore := &mockSnapshotStore{
			getFunc: func(_ context.Context, _ pgx.Tx, streamType, streamID string, _ int) (snapshot.Snapshot, error) {
				return snapshot.Snapshot{
					StreamType:    streamType,
					StreamID:      streamID,
					StreamVersion: 4,
					SchemaVersion: 1,
					Payload:       v1Payload,
				}, nil
			},
		}
		reader := &mockStreamReader{}
		repo, err := snapshot.NewRepository(reader, snapStore, cfg)
		if err != nil {
			t.Fatalf("NewRepository: %v", err)
		}

		_, err = repo.Load(ctx, nil, "user-1")
		if err == nil || !strings.Contains(err.Error(), "failed to decode snapshot") {
			t.Fatalf("expected decode error propagation, got: %v", err)
		}
	})
}
