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

type userState struct {
	ID    string `json:"id"`
	Name  string `json:"name"`
	Email string `json:"email"`
}

type userEvent struct {
	Name  string `json:"name"`
	Email string `json:"email"`
}

type mockStreamReader struct {
	readStreamFunc func(ctx context.Context, tx pgx.Tx, streamType, streamID string, fromVersion, toVersion *int64) (store.Stream, error)
}

func (m *mockStreamReader) ReadStream(ctx context.Context, tx pgx.Tx, streamType, streamID string, fromVersion, toVersion *int64) (store.Stream, error) {
	if m.readStreamFunc != nil {
		return m.readStreamFunc(ctx, tx, streamType, streamID, fromVersion, toVersion)
	}
	return store.Stream{StreamType: streamType, StreamID: streamID}, nil
}

type mockSnapshotStore struct {
	getFunc func(ctx context.Context, tx pgx.Tx, streamType, streamID string, maxSchemaVersion int) (snapshot.Snapshot, error)
	putFunc func(ctx context.Context, tx pgx.Tx, snap *snapshot.Snapshot) error
}

func (m *mockSnapshotStore) Get(ctx context.Context, tx pgx.Tx, streamType, streamID string, maxSchemaVersion int) (snapshot.Snapshot, error) {
	if m.getFunc != nil {
		return m.getFunc(ctx, tx, streamType, streamID, maxSchemaVersion)
	}
	return snapshot.Snapshot{}, nil
}

func (m *mockSnapshotStore) Put(ctx context.Context, tx pgx.Tx, snap *snapshot.Snapshot) error {
	if m.putFunc != nil {
		return m.putFunc(ctx, tx, snap)
	}
	return nil
}

type mockLogger struct {
	debugCalls int
	infoCalls  int
	errorCalls int
	lastErrMsg string
}

func (m *mockLogger) Debug(_ context.Context, _ string, _ ...any) { m.debugCalls++ }
func (m *mockLogger) Info(_ context.Context, _ string, _ ...any)  { m.infoCalls++ }
func (m *mockLogger) Error(_ context.Context, msg string, _ ...any) {
	m.errorCalls++
	m.lastErrMsg = msg
}

func defaultTestConfig() snapshot.RepositoryConfig[*userState] {
	return snapshot.RepositoryConfig[*userState]{
		StreamType:    "User",
		SchemaVersion: 1,
		Initializer: func(id string) *userState {
			return &userState{ID: id}
		},
		Apply: func(s *userState, e store.PersistedEvent) (*userState, error) {
			var ev userEvent
			if err := json.Unmarshal(e.Payload, &ev); err != nil {
				return nil, err
			}
			if ev.Name != "" {
				s.Name = ev.Name
			}
			if ev.Email != "" {
				s.Email = ev.Email
			}
			return s, nil
		},
		Codec: snapshot.JSON[*userState](),
	}
}

func TestNewRepository_Validation(t *testing.T) {
	validCfg := defaultTestConfig()
	reader := &mockStreamReader{}
	snapStore := &mockSnapshotStore{}

	tests := []struct {
		name      string
		reader    store.StreamReader
		store     snapshot.Store
		cfgMod    func(c *snapshot.RepositoryConfig[*userState])
		wantErr   bool
		errSubstr string
	}{
		{
			name:      "nil reader",
			reader:    nil,
			store:     snapStore,
			wantErr:   true,
			errSubstr: "event store reader cannot be nil",
		},
		{
			name:      "nil snapshot store",
			reader:    reader,
			store:     nil,
			wantErr:   true,
			errSubstr: "snapshot store cannot be nil",
		},
		{
			name:   "empty stream type",
			reader: reader,
			store:  snapStore,
			cfgMod: func(c *snapshot.RepositoryConfig[*userState]) {
				c.StreamType = ""
			},
			wantErr:   true,
			errSubstr: "stream type cannot be empty",
		},
		{
			name:   "nil initializer",
			reader: reader,
			store:  snapStore,
			cfgMod: func(c *snapshot.RepositoryConfig[*userState]) {
				c.Initializer = nil
			},
			wantErr:   true,
			errSubstr: "initializer cannot be nil",
		},
		{
			name:   "nil apply",
			reader: reader,
			store:  snapStore,
			cfgMod: func(c *snapshot.RepositoryConfig[*userState]) {
				c.Apply = nil
			},
			wantErr:   true,
			errSubstr: "apply cannot be nil",
		},
		{
			name:   "nil codec",
			reader: reader,
			store:  snapStore,
			cfgMod: func(c *snapshot.RepositoryConfig[*userState]) {
				c.Codec = nil
			},
			wantErr:   true,
			errSubstr: "codec cannot be nil",
		},
		{
			name:   "zero schema version",
			reader: reader,
			store:  snapStore,
			cfgMod: func(c *snapshot.RepositoryConfig[*userState]) {
				c.SchemaVersion = 0
			},
			wantErr:   true,
			errSubstr: "schema version must be >= 1",
		},
		{
			name:   "negative schema version",
			reader: reader,
			store:  snapStore,
			cfgMod: func(c *snapshot.RepositoryConfig[*userState]) {
				c.SchemaVersion = -1
			},
			wantErr:   true,
			errSubstr: "schema version must be >= 1",
		},
		{
			name:   "upcaster source schema version zero",
			reader: reader,
			store:  snapStore,
			cfgMod: func(c *snapshot.RepositoryConfig[*userState]) {
				c.SchemaVersion = 2
				c.Upcasters = map[int]snapshot.Upcaster{
					0: func(_ int, payload []byte) ([]byte, error) { return payload, nil },
				}
			},
			wantErr:   true,
			errSubstr: "upcaster source schema version must be >= 1",
		},
		{
			name:   "upcaster source schema version negative",
			reader: reader,
			store:  snapStore,
			cfgMod: func(c *snapshot.RepositoryConfig[*userState]) {
				c.SchemaVersion = 2
				c.Upcasters = map[int]snapshot.Upcaster{
					-1: func(_ int, payload []byte) ([]byte, error) { return payload, nil },
				}
			},
			wantErr:   true,
			errSubstr: "upcaster source schema version must be >= 1",
		},
		{
			name:   "upcaster source schema version equals repository version",
			reader: reader,
			store:  snapStore,
			cfgMod: func(c *snapshot.RepositoryConfig[*userState]) {
				c.SchemaVersion = 2
				c.Upcasters = map[int]snapshot.Upcaster{
					2: func(_ int, payload []byte) ([]byte, error) { return payload, nil },
				}
			},
			wantErr:   true,
			errSubstr: "cannot be >= repository schema version",
		},
		{
			name:   "upcaster source schema version greater than repository version",
			reader: reader,
			store:  snapStore,
			cfgMod: func(c *snapshot.RepositoryConfig[*userState]) {
				c.SchemaVersion = 2
				c.Upcasters = map[int]snapshot.Upcaster{
					3: func(_ int, payload []byte) ([]byte, error) { return payload, nil },
				}
			},
			wantErr:   true,
			errSubstr: "cannot be >= repository schema version",
		},
		{
			name:   "nil upcaster function",
			reader: reader,
			store:  snapStore,
			cfgMod: func(c *snapshot.RepositoryConfig[*userState]) {
				c.SchemaVersion = 2
				c.Upcasters = map[int]snapshot.Upcaster{
					1: nil,
				}
			},
			wantErr:   true,
			errSubstr: "nil upcaster configured for schema version 1",
		},
		{
			name:   "valid upcaster configuration",
			reader: reader,
			store:  snapStore,
			cfgMod: func(c *snapshot.RepositoryConfig[*userState]) {
				c.SchemaVersion = 2
				c.Upcasters = map[int]snapshot.Upcaster{
					1: func(_ int, payload []byte) ([]byte, error) { return payload, nil },
				}
			},
			wantErr: false,
		},
		{
			name:   "nil transformer in configuration",
			reader: reader,
			store:  snapStore,
			cfgMod: func(c *snapshot.RepositoryConfig[*userState]) {
				c.Transformers = []snapshot.PayloadTransformer{nil}
			},
			wantErr:   true,
			errSubstr: "nil transformer configured at index 0",
		},
		{
			name:   "valid transformer configuration",
			reader: reader,
			store:  snapStore,
			cfgMod: func(c *snapshot.RepositoryConfig[*userState]) {
				c.Transformers = []snapshot.PayloadTransformer{snapshot.Gzip()}
			},
			wantErr: false,
		},
		{
			name:    "valid configuration",
			reader:  reader,
			store:   snapStore,
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := validCfg
			if tt.cfgMod != nil {
				tt.cfgMod(&cfg)
			}
			repo, err := snapshot.NewRepository(tt.reader, tt.store, cfg)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error containing %q, got nil", tt.errSubstr)
				}
				if tt.errSubstr != "" && err.Error() != tt.errSubstr {
					t.Logf("got error: %v", err)
				}
			} else {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if repo == nil {
					t.Fatal("expected non-nil repository")
				}
			}
		})
	}
}

func TestRepository_Load(t *testing.T) {
	ctx := context.Background()

	t.Run("empty stream and no snapshot", func(t *testing.T) {
		cfg := defaultTestConfig()
		reader := &mockStreamReader{}
		snapStore := &mockSnapshotStore{}

		repo, err := snapshot.NewRepository(reader, snapStore, cfg)
		if err != nil {
			t.Fatalf("new repository: %v", err)
		}

		res, err := repo.Load(ctx, nil, "user-1")
		if err != nil {
			t.Fatalf("Load failed: %v", err)
		}
		if res.StreamVersion != 0 {
			t.Errorf("expected version 0, got %d", res.StreamVersion)
		}
		if res.State.ID != "user-1" {
			t.Errorf("expected ID 'user-1', got %s", res.State.ID)
		}
	})

	t.Run("snapshot get error", func(t *testing.T) {
		cfg := defaultTestConfig()
		snapStore := &mockSnapshotStore{
			getFunc: func(_ context.Context, _ pgx.Tx, _, _ string, _ int) (snapshot.Snapshot, error) {
				return snapshot.Snapshot{}, errors.New("db query error")
			},
		}

		repo, err := snapshot.NewRepository(&mockStreamReader{}, snapStore, cfg)
		if err != nil {
			t.Fatalf("NewRepository failed: %v", err)
		}
		_, err = repo.Load(ctx, nil, "user-1")
		if err == nil {
			t.Fatal("expected error, got nil")
		}
	})

	t.Run("snapshot unmarshal error fallback by default", func(t *testing.T) {
		logger := &mockLogger{}
		cfg := defaultTestConfig()
		cfg.Logger = logger

		snapStore := &mockSnapshotStore{
			getFunc: func(_ context.Context, _ pgx.Tx, _, _ string, _ int) (snapshot.Snapshot, error) {
				return snapshot.Snapshot{
					StreamType:    "User",
					StreamID:      "user-1",
					StreamVersion: 5,
					SchemaVersion: 1,
					Payload:       []byte("invalid-json"),
				}, nil
			},
		}

		var requestedFromVersion *int64
		reader := &mockStreamReader{
			readStreamFunc: func(_ context.Context, _ pgx.Tx, streamType, streamID string, fromVersion, _ *int64) (store.Stream, error) {
				requestedFromVersion = fromVersion
				evPayload, err := json.Marshal(userEvent{Name: "Bob"})
				if err != nil {
					return store.Stream{}, err
				}
				return store.Stream{
					StreamType: streamType,
					StreamID:   streamID,
					Events: []store.PersistedEvent{
						{
							StreamType:    streamType,
							StreamID:      streamID,
							StreamVersion: 1,
							Payload:       evPayload,
						},
					},
				}, nil
			},
		}

		repo, err := snapshot.NewRepository(reader, snapStore, cfg)
		if err != nil {
			t.Fatalf("NewRepository failed: %v", err)
		}
		res, err := repo.Load(ctx, nil, "user-1")
		if err != nil {
			t.Fatalf("expected fallback to succeed, got error: %v", err)
		}
		if requestedFromVersion != nil {
			t.Errorf("expected fromVersion to be nil for full replay, got %v", *requestedFromVersion)
		}
		if res.StreamVersion != 1 {
			t.Errorf("expected version 1, got %d", res.StreamVersion)
		}
		if res.State.Name != "Bob" {
			t.Errorf("expected state name 'Bob', got %q", res.State.Name)
		}
		if logger.errorCalls == 0 {
			t.Errorf("expected logger.Error to be called on corrupt snapshot payload")
		}
	})

	t.Run("snapshot unmarshal error with FailOnCorruptSnapshot", func(t *testing.T) {
		cfg := defaultTestConfig()
		cfg.FailOnCorruptSnapshot = true

		snapStore := &mockSnapshotStore{
			getFunc: func(_ context.Context, _ pgx.Tx, _, _ string, _ int) (snapshot.Snapshot, error) {
				return snapshot.Snapshot{
					StreamType:    "User",
					StreamID:      "user-1",
					StreamVersion: 5,
					SchemaVersion: 1,
					Payload:       []byte("invalid-json"),
				}, nil
			},
		}

		repo, err := snapshot.NewRepository(&mockStreamReader{}, snapStore, cfg)
		if err != nil {
			t.Fatalf("NewRepository failed: %v", err)
		}
		_, err = repo.Load(ctx, nil, "user-1")
		if err == nil {
			t.Fatal("expected error on invalid json payload when FailOnCorruptSnapshot=true, got nil")
		}
	})

	t.Run("valid snapshot with delta events", func(t *testing.T) {
		cfg := defaultTestConfig()
		snapPayload, err := json.Marshal(&userState{ID: "user-1", Name: "Alice", Email: "alice@example.com"})
		if err != nil {
			t.Fatalf("marshal snapshot payload: %v", err)
		}
		snapStore := &mockSnapshotStore{
			getFunc: func(_ context.Context, _ pgx.Tx, _, _ string, _ int) (snapshot.Snapshot, error) {
				return snapshot.Snapshot{
					StreamType:    "User",
					StreamID:      "user-1",
					StreamVersion: 3,
					SchemaVersion: 1,
					Payload:       snapPayload,
				}, nil
			},
		}

		var requestedFromVersion *int64
		reader := &mockStreamReader{
			readStreamFunc: func(_ context.Context, _ pgx.Tx, streamType, streamID string, fromVersion, _ *int64) (store.Stream, error) {
				requestedFromVersion = fromVersion
				evPayload, err := json.Marshal(userEvent{Name: "Alice Smith"})
				if err != nil {
					return store.Stream{}, err
				}
				return store.Stream{
					StreamType: streamType,
					StreamID:   streamID,
					Events: []store.PersistedEvent{
						{
							StreamType:    streamType,
							StreamID:      streamID,
							StreamVersion: 4,
							Payload:       evPayload,
						},
					},
				}, nil
			},
		}

		repo, err := snapshot.NewRepository(reader, snapStore, cfg)
		if err != nil {
			t.Fatalf("NewRepository failed: %v", err)
		}
		res, err := repo.Load(ctx, nil, "user-1")
		if err != nil {
			t.Fatalf("Load failed: %v", err)
		}
		if requestedFromVersion == nil || *requestedFromVersion != 4 {
			t.Fatalf("expected fromVersion 4, got %v", requestedFromVersion)
		}
		if res.StreamVersion != 4 {
			t.Errorf("expected final version 4, got %d", res.StreamVersion)
		}
		if res.State.Name != "Alice Smith" || res.State.Email != "alice@example.com" {
			t.Errorf("unexpected state: %+v", res.State)
		}
	})

	t.Run("schema version mismatch triggers full replay", func(t *testing.T) {
		cfg := defaultTestConfig()
		cfg.SchemaVersion = 2 // Current schema is v2

		snapPayload, err := json.Marshal(&userState{ID: "user-1", Name: "OldAlice"})
		if err != nil {
			t.Fatalf("marshal snapshot payload: %v", err)
		}
		snapStore := &mockSnapshotStore{
			getFunc: func(_ context.Context, _ pgx.Tx, _, _ string, _ int) (snapshot.Snapshot, error) {
				return snapshot.Snapshot{
					StreamType:    "User",
					StreamID:      "user-1",
					StreamVersion: 5,
					SchemaVersion: 1, // Stored snapshot is v1
					Payload:       snapPayload,
				}, nil
			},
		}

		var requestedFromVersion *int64
		reader := &mockStreamReader{
			readStreamFunc: func(_ context.Context, _ pgx.Tx, streamType, streamID string, fromVersion, _ *int64) (store.Stream, error) {
				requestedFromVersion = fromVersion
				ev1, err := json.Marshal(userEvent{Name: "Alice", Email: "alice@example.com"})
				if err != nil {
					return store.Stream{}, err
				}
				ev2, err := json.Marshal(userEvent{Name: "Alice S."})
				if err != nil {
					return store.Stream{}, err
				}
				return store.Stream{
					StreamType: streamType,
					StreamID:   streamID,
					Events: []store.PersistedEvent{
						{StreamVersion: 1, Payload: ev1},
						{StreamVersion: 2, Payload: ev2},
					},
				}, nil
			},
		}

		repo, err := snapshot.NewRepository(reader, snapStore, cfg)
		if err != nil {
			t.Fatalf("NewRepository failed: %v", err)
		}
		res, err := repo.Load(ctx, nil, "user-1")
		if err != nil {
			t.Fatalf("Load failed: %v", err)
		}
		if requestedFromVersion != nil {
			t.Errorf("expected fromVersion nil (replay from beginning), got %v", requestedFromVersion)
		}
		if res.StreamVersion != 2 {
			t.Errorf("expected version 2, got %d", res.StreamVersion)
		}
		if res.State.Name != "Alice S." {
			t.Errorf("expected Name 'Alice S.', got %s", res.State.Name)
		}
	})

	t.Run("stored snapshot schema version greater than repository schema version is ignored", func(t *testing.T) {
		cfg := defaultTestConfig()
		cfg.SchemaVersion = 2
		snapStore := &mockSnapshotStore{
			getFunc: func(_ context.Context, _ pgx.Tx, streamType, streamID string, _ int) (snapshot.Snapshot, error) {
				return snapshot.Snapshot{
					StreamType:    streamType,
					StreamID:      streamID,
					StreamVersion: 5,
					SchemaVersion: 3,
					Payload:       []byte(`{}`),
				}, nil
			},
		}
		reader := &mockStreamReader{
			readStreamFunc: func(_ context.Context, _ pgx.Tx, streamType, streamID string, fromVersion, _ *int64) (store.Stream, error) {
				if fromVersion != nil {
					t.Fatalf("expected full replay fromVersion nil, got %v", fromVersion)
				}
				return store.Stream{StreamType: streamType, StreamID: streamID}, nil
			},
		}
		repo, err := snapshot.NewRepository(reader, snapStore, cfg)
		if err != nil {
			t.Fatalf("NewRepository failed: %v", err)
		}
		res, err := repo.Load(ctx, nil, "user-1")
		if err != nil {
			t.Fatalf("Load failed: %v", err)
		}
		if res.SnapshotHit {
			t.Error("expected SnapshotHit = false")
		}
	})

	t.Run("read stream error", func(t *testing.T) {
		cfg := defaultTestConfig()
		reader := &mockStreamReader{
			readStreamFunc: func(_ context.Context, _ pgx.Tx, _, _ string, _, _ *int64) (store.Stream, error) {
				return store.Stream{}, errors.New("read stream failed")
			},
		}

		repo, err := snapshot.NewRepository(reader, &mockSnapshotStore{}, cfg)
		if err != nil {
			t.Fatalf("NewRepository failed: %v", err)
		}
		_, err = repo.Load(ctx, nil, "user-1")
		if err == nil {
			t.Fatal("expected error, got nil")
		}
	})

	t.Run("apply error", func(t *testing.T) {
		cfg := defaultTestConfig()
		cfg.Apply = func(_ *userState, _ store.PersistedEvent) (*userState, error) {
			return nil, errors.New("apply event failed")
		}
		reader := &mockStreamReader{
			readStreamFunc: func(_ context.Context, _ pgx.Tx, _, _ string, _, _ *int64) (store.Stream, error) {
				return store.Stream{
					Events: []store.PersistedEvent{{StreamVersion: 1, Payload: []byte("{}")}},
				}, nil
			},
		}

		repo, err := snapshot.NewRepository(reader, &mockSnapshotStore{}, cfg)
		if err != nil {
			t.Fatalf("NewRepository failed: %v", err)
		}
		_, err = repo.Load(ctx, nil, "user-1")
		if err == nil {
			t.Fatal("expected error from Apply, got nil")
		}
	})
}

func TestRepository_Save(t *testing.T) {
	ctx := context.Background()
	cfg := defaultTestConfig()

	t.Run("invalid version", func(t *testing.T) {
		repo, err := snapshot.NewRepository(&mockStreamReader{}, &mockSnapshotStore{}, cfg)
		if err != nil {
			t.Fatalf("NewRepository failed: %v", err)
		}

		if err := repo.Save(ctx, nil, "user-1", 0, &userState{ID: "user-1"}); err == nil {
			t.Error("expected error for version 0")
		}
		if err := repo.Save(ctx, nil, "user-1", -1, &userState{ID: "user-1"}); err == nil {
			t.Error("expected error for negative version")
		}
	})

	t.Run("codec encode error", func(t *testing.T) {
		cfgErr := cfg
		cfgErr.Codec = snapshot.FuncCodec(
			func(_ *userState) ([]byte, error) {
				return nil, errors.New("encode error")
			},
			nil,
		)
		reader := &mockStreamReader{
			readStreamFunc: func(_ context.Context, _ pgx.Tx, streamType, streamID string, _, _ *int64) (store.Stream, error) {
				return store.Stream{
					StreamType: streamType,
					StreamID:   streamID,
					Events:     []store.PersistedEvent{{StreamVersion: 5}},
				}, nil
			},
		}

		repo, err := snapshot.NewRepository(reader, &mockSnapshotStore{}, cfgErr)
		if err != nil {
			t.Fatalf("NewRepository failed: %v", err)
		}
		err = repo.Save(ctx, nil, "user-1", 5, &userState{ID: "user-1"})
		if err == nil || !strings.Contains(err.Error(), "failed to encode snapshot") {
			t.Fatalf("expected encode error, got: %v", err)
		}
	})

	t.Run("put store error", func(t *testing.T) {
		snapStore := &mockSnapshotStore{
			putFunc: func(_ context.Context, _ pgx.Tx, _ *snapshot.Snapshot) error {
				return errors.New("put failed")
			},
		}
		reader := &mockStreamReader{
			readStreamFunc: func(_ context.Context, _ pgx.Tx, streamType, streamID string, _, _ *int64) (store.Stream, error) {
				return store.Stream{
					StreamType: streamType,
					StreamID:   streamID,
					Events:     []store.PersistedEvent{{StreamVersion: 5}},
				}, nil
			},
		}

		repo, err := snapshot.NewRepository(reader, snapStore, cfg)
		if err != nil {
			t.Fatalf("NewRepository failed: %v", err)
		}
		err = repo.Save(ctx, nil, "user-1", 5, &userState{ID: "user-1"})
		if err == nil || !strings.Contains(err.Error(), "put failed") {
			t.Fatalf("expected store put error, got: %v", err)
		}
	})

	t.Run("successful save", func(t *testing.T) {
		var savedSnap *snapshot.Snapshot
		snapStore := &mockSnapshotStore{
			putFunc: func(_ context.Context, _ pgx.Tx, snap *snapshot.Snapshot) error {
				savedSnap = snap
				return nil
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
				return store.Stream{StreamType: streamType, StreamID: streamID}, nil
			},
		}

		repo, err := snapshot.NewRepository(reader, snapStore, cfg)
		if err != nil {
			t.Fatalf("NewRepository failed: %v", err)
		}
		u := &userState{ID: "user-1", Name: "Alice", Email: "alice@example.com"}
		err = repo.Save(ctx, nil, "user-1", 10, u)
		if err != nil {
			t.Fatalf("Save failed: %v", err)
		}

		if savedSnap == nil {
			t.Fatal("expected saved snapshot, got nil")
		}
		if savedSnap.StreamType != "User" {
			t.Errorf("expected StreamType 'User', got %s", savedSnap.StreamType)
		}
		if savedSnap.StreamID != "user-1" {
			t.Errorf("expected StreamID 'user-1', got %s", savedSnap.StreamID)
		}
		if savedSnap.StreamVersion != 10 {
			t.Errorf("expected StreamVersion 10, got %d", savedSnap.StreamVersion)
		}
		if savedSnap.SchemaVersion != 1 {
			t.Errorf("expected SchemaVersion 1, got %d", savedSnap.SchemaVersion)
		}
		var decoded userState
		if err := json.Unmarshal(savedSnap.Payload, &decoded); err != nil {
			t.Fatalf("unmarshal payload failed: %v", err)
		}
		if decoded.Name != "Alice" {
			t.Errorf("expected payload name 'Alice', got %s", decoded.Name)
		}
	})

	t.Run("version not found in event log rejects save", func(t *testing.T) {
		reader := &mockStreamReader{
			readStreamFunc: func(_ context.Context, _ pgx.Tx, streamType, streamID string, _, _ *int64) (store.Stream, error) {
				return store.Stream{StreamType: streamType, StreamID: streamID}, nil
			},
		}
		repo, err := snapshot.NewRepository(reader, &mockSnapshotStore{}, cfg)
		if err != nil {
			t.Fatalf("NewRepository failed: %v", err)
		}
		u := &userState{ID: "user-1", Name: "Alice"}
		err = repo.Save(ctx, nil, "user-1", 10, u)
		if err == nil {
			t.Fatal("expected error saving at non-existent version, got nil")
		}
		if !strings.Contains(err.Error(), "stream version does not exist in event log") {
			t.Errorf("unexpected error: %v", err)
		}
	})

	t.Run("verify stream error during save propagates", func(t *testing.T) {
		reader := &mockStreamReader{
			readStreamFunc: func(_ context.Context, _ pgx.Tx, _, _ string, _, _ *int64) (store.Stream, error) {
				return store.Stream{}, errors.New("read error")
			},
		}
		repo, err := snapshot.NewRepository(reader, &mockSnapshotStore{}, cfg)
		if err != nil {
			t.Fatalf("NewRepository failed: %v", err)
		}
		u := &userState{ID: "user-1", Name: "Alice"}
		err = repo.Save(ctx, nil, "user-1", 10, u)
		if err == nil {
			t.Fatal("expected error propagating reader error, got nil")
		}
		if !strings.Contains(err.Error(), "failed to verify stream version") {
			t.Errorf("unexpected error: %v", err)
		}
	})
}

func TestRepository_Load_Result(t *testing.T) {
	ctx := context.Background()

	t.Run("snapshot hit with delta events", func(t *testing.T) {
		cfg := defaultTestConfig()
		snapPayload, err := json.Marshal(&userState{ID: "user-1", Name: "Alice"})
		if err != nil {
			t.Fatalf("marshal failed: %v", err)
		}
		snapStore := &mockSnapshotStore{
			getFunc: func(_ context.Context, _ pgx.Tx, _, _ string, _ int) (snapshot.Snapshot, error) {
				return snapshot.Snapshot{
					StreamType:    "User",
					StreamID:      "user-1",
					StreamVersion: 3,
					SchemaVersion: 1,
					Payload:       snapPayload,
				}, nil
			},
		}
		reader := &mockStreamReader{
			readStreamFunc: func(_ context.Context, _ pgx.Tx, streamType, streamID string, _, _ *int64) (store.Stream, error) {
				evPayload, err := json.Marshal(userEvent{Email: "alice@example.com"})
				if err != nil {
					return store.Stream{}, err
				}
				return store.Stream{
					StreamType: streamType,
					StreamID:   streamID,
					Events: []store.PersistedEvent{
						{StreamType: streamType, StreamID: streamID, StreamVersion: 4, Payload: evPayload},
						{StreamType: streamType, StreamID: streamID, StreamVersion: 5, Payload: evPayload},
					},
				}, nil
			},
		}

		repo, err := snapshot.NewRepository(reader, snapStore, cfg)
		if err != nil {
			t.Fatalf("NewRepository failed: %v", err)
		}
		res, err := repo.Load(ctx, nil, "user-1")
		if err != nil {
			t.Fatalf("Load failed: %v", err)
		}
		if !res.SnapshotHit {
			t.Errorf("expected SnapshotHit to be true")
		}
		if res.SnapshotVersion != 3 {
			t.Errorf("expected SnapshotVersion 3, got %d", res.SnapshotVersion)
		}
		if res.StreamVersion != 5 {
			t.Errorf("expected StreamVersion 5, got %d", res.StreamVersion)
		}
		if res.EventsReplayed != 2 {
			t.Errorf("expected EventsReplayed 2, got %d", res.EventsReplayed)
		}
		if res.State.Name != "Alice" || res.State.Email != "alice@example.com" {
			t.Errorf("unexpected state: %+v", res.State)
		}
		if res.Data().Name != "Alice" {
			t.Errorf("expected res.Data() to equal res.State")
		}
	})

	t.Run("snapshot hit at head with zero events replayed", func(t *testing.T) {
		cfg := defaultTestConfig()
		snapPayload, err := json.Marshal(&userState{ID: "user-1", Name: "Alice"})
		if err != nil {
			t.Fatalf("marshal failed: %v", err)
		}
		snapStore := &mockSnapshotStore{
			getFunc: func(_ context.Context, _ pgx.Tx, _, _ string, _ int) (snapshot.Snapshot, error) {
				return snapshot.Snapshot{
					StreamType:    "User",
					StreamID:      "user-1",
					StreamVersion: 5,
					SchemaVersion: 1,
					Payload:       snapPayload,
				}, nil
			},
		}
		reader := &mockStreamReader{
			readStreamFunc: func(_ context.Context, _ pgx.Tx, streamType, streamID string, fromVersion, toVersion *int64) (store.Stream, error) {
				if fromVersion != nil && toVersion != nil && *fromVersion == 5 && *toVersion == 5 {
					return store.Stream{
						StreamType: streamType,
						StreamID:   streamID,
						Events:     []store.PersistedEvent{{StreamVersion: 5}},
					}, nil
				}
				return store.Stream{StreamType: streamType, StreamID: streamID}, nil
			},
		}

		repo, err := snapshot.NewRepository(reader, snapStore, cfg)
		if err != nil {
			t.Fatalf("NewRepository failed: %v", err)
		}
		res, err := repo.Load(ctx, nil, "user-1")
		if err != nil {
			t.Fatalf("Load failed: %v", err)
		}
		if !res.SnapshotHit {
			t.Errorf("expected SnapshotHit to be true")
		}
		if res.SnapshotVersion != 5 {
			t.Errorf("expected SnapshotVersion 5, got %d", res.SnapshotVersion)
		}
		if res.StreamVersion != 5 {
			t.Errorf("expected StreamVersion 5, got %d", res.StreamVersion)
		}
		if res.EventsReplayed != 0 {
			t.Errorf("expected EventsReplayed 0, got %d", res.EventsReplayed)
		}
	})

	t.Run("snapshot ahead of stream log falls back to full replay", func(t *testing.T) {
		logger := &mockLogger{}
		cfg := defaultTestConfig()
		cfg.Logger = logger

		snapPayload, err := json.Marshal(&userState{ID: "user-1", Name: "Phantom"})
		if err != nil {
			t.Fatalf("marshal failed: %v", err)
		}
		snapStore := &mockSnapshotStore{
			getFunc: func(_ context.Context, _ pgx.Tx, _, _ string, _ int) (snapshot.Snapshot, error) {
				return snapshot.Snapshot{
					StreamType:    "User",
					StreamID:      "user-1",
					StreamVersion: 5000,
					SchemaVersion: 1,
					Payload:       snapPayload,
				}, nil
			},
		}

		ev1, err := json.Marshal(userEvent{Name: "Alice"})
		if err != nil {
			t.Fatalf("marshal failed: %v", err)
		}
		ev2, err := json.Marshal(userEvent{Email: "alice@example.com"})
		if err != nil {
			t.Fatalf("marshal failed: %v", err)
		}

		reader := &mockStreamReader{
			readStreamFunc: func(_ context.Context, _ pgx.Tx, streamType, streamID string, fromVersion, toVersion *int64) (store.Stream, error) {
				// Snapshot head verification (fromVersion=5000, toVersion=5000) -> not found (empty)
				if fromVersion != nil && toVersion != nil && *fromVersion == 5000 && *toVersion == 5000 {
					return store.Stream{StreamType: streamType, StreamID: streamID}, nil
				}
				// Delta read (fromVersion=5001, toVersion=nil) -> empty
				if fromVersion != nil && *fromVersion == 5001 {
					return store.Stream{StreamType: streamType, StreamID: streamID}, nil
				}
				// Full stream replay (fromVersion=nil, toVersion=nil) -> events 1 and 2
				if fromVersion == nil && toVersion == nil {
					return store.Stream{
						StreamType: streamType,
						StreamID:   streamID,
						Events: []store.PersistedEvent{
							{StreamType: streamType, StreamID: streamID, StreamVersion: 1, Payload: ev1},
							{StreamType: streamType, StreamID: streamID, StreamVersion: 2, Payload: ev2},
						},
					}, nil
				}
				return store.Stream{StreamType: streamType, StreamID: streamID}, nil
			},
		}

		repo, err := snapshot.NewRepository(reader, snapStore, cfg)
		if err != nil {
			t.Fatalf("NewRepository failed: %v", err)
		}
		res, err := repo.Load(ctx, nil, "user-1")
		if err != nil {
			t.Fatalf("Load failed: %v", err)
		}
		if res.SnapshotHit {
			t.Errorf("expected SnapshotHit to be false")
		}
		if res.SnapshotVersion != 0 {
			t.Errorf("expected SnapshotVersion 0, got %d", res.SnapshotVersion)
		}
		if res.StreamVersion != 2 {
			t.Errorf("expected StreamVersion 2, got %d", res.StreamVersion)
		}
		if res.EventsReplayed != 2 {
			t.Errorf("expected EventsReplayed 2, got %d", res.EventsReplayed)
		}
		if res.State.Name != "Alice" || res.State.Email != "alice@example.com" {
			t.Errorf("unexpected state: %+v", res.State)
		}
		if logger.errorCalls == 0 {
			t.Errorf("expected logger.Error to be called when snapshot is ahead of stream")
		}
	})

	t.Run("orphan snapshot on empty stream falls back to initial state", func(t *testing.T) {
		cfg := defaultTestConfig()
		snapPayload, err := json.Marshal(&userState{ID: "user-1", Name: "Ghost"})
		if err != nil {
			t.Fatalf("marshal failed: %v", err)
		}
		snapStore := &mockSnapshotStore{
			getFunc: func(_ context.Context, _ pgx.Tx, _, _ string, _ int) (snapshot.Snapshot, error) {
				return snapshot.Snapshot{
					StreamType:    "User",
					StreamID:      "user-1",
					StreamVersion: 42,
					SchemaVersion: 1,
					Payload:       snapPayload,
				}, nil
			},
		}
		reader := &mockStreamReader{
			readStreamFunc: func(_ context.Context, _ pgx.Tx, streamType, streamID string, _, _ *int64) (store.Stream, error) {
				return store.Stream{StreamType: streamType, StreamID: streamID}, nil
			},
		}

		repo, err := snapshot.NewRepository(reader, snapStore, cfg)
		if err != nil {
			t.Fatalf("NewRepository failed: %v", err)
		}
		res, err := repo.Load(ctx, nil, "user-1")
		if err != nil {
			t.Fatalf("Load failed: %v", err)
		}
		if res.SnapshotHit {
			t.Errorf("expected SnapshotHit to be false")
		}
		if res.SnapshotVersion != 0 {
			t.Errorf("expected SnapshotVersion 0, got %d", res.SnapshotVersion)
		}
		if res.StreamVersion != 0 {
			t.Errorf("expected StreamVersion 0, got %d", res.StreamVersion)
		}
		if res.EventsReplayed != 0 {
			t.Errorf("expected EventsReplayed 0, got %d", res.EventsReplayed)
		}
		if res.State.Name != "" || res.State.ID != "user-1" {
			t.Errorf("expected initial state, got %+v", res.State)
		}
	})

	t.Run("verify snapshot in stream error propagates", func(t *testing.T) {
		cfg := defaultTestConfig()
		snapPayload, err := json.Marshal(&userState{ID: "user-1", Name: "Alice"})
		if err != nil {
			t.Fatalf("marshal failed: %v", err)
		}
		snapStore := &mockSnapshotStore{
			getFunc: func(_ context.Context, _ pgx.Tx, _, _ string, _ int) (snapshot.Snapshot, error) {
				return snapshot.Snapshot{
					StreamType:    "User",
					StreamID:      "user-1",
					StreamVersion: 5,
					SchemaVersion: 1,
					Payload:       snapPayload,
				}, nil
			},
		}
		reader := &mockStreamReader{
			readStreamFunc: func(_ context.Context, _ pgx.Tx, streamType, streamID string, fromVersion, toVersion *int64) (store.Stream, error) {
				if fromVersion != nil && toVersion != nil && *fromVersion == 5 && *toVersion == 5 {
					return store.Stream{}, errors.New("read verify failed")
				}
				return store.Stream{StreamType: streamType, StreamID: streamID}, nil
			},
		}

		repo, err := snapshot.NewRepository(reader, snapStore, cfg)
		if err != nil {
			t.Fatalf("NewRepository failed: %v", err)
		}
		_, err = repo.Load(ctx, nil, "user-1")
		if err == nil || !strings.Contains(err.Error(), "failed to verify snapshot version in stream") {
			t.Fatalf("expected verify error propagation, got: %v", err)
		}
	})

	t.Run("fallback full replay error propagates", func(t *testing.T) {
		cfg := defaultTestConfig()
		snapPayload, err := json.Marshal(&userState{ID: "user-1", Name: "Alice"})
		if err != nil {
			t.Fatalf("marshal failed: %v", err)
		}
		snapStore := &mockSnapshotStore{
			getFunc: func(_ context.Context, _ pgx.Tx, _, _ string, _ int) (snapshot.Snapshot, error) {
				return snapshot.Snapshot{
					StreamType:    "User",
					StreamID:      "user-1",
					StreamVersion: 50,
					SchemaVersion: 1,
					Payload:       snapPayload,
				}, nil
			},
		}
		reader := &mockStreamReader{
			readStreamFunc: func(_ context.Context, _ pgx.Tx, streamType, streamID string, fromVersion, toVersion *int64) (store.Stream, error) {
				if fromVersion == nil && toVersion == nil {
					return store.Stream{}, errors.New("full replay read failed")
				}
				return store.Stream{StreamType: streamType, StreamID: streamID}, nil
			},
		}

		repo, err := snapshot.NewRepository(reader, snapStore, cfg)
		if err != nil {
			t.Fatalf("NewRepository failed: %v", err)
		}
		_, err = repo.Load(ctx, nil, "user-1")
		if err == nil || !strings.Contains(err.Error(), "failed to replay full stream") {
			t.Fatalf("expected full replay error propagation, got: %v", err)
		}
	})

	t.Run("snapshot miss (empty stream)", func(t *testing.T) {
		cfg := defaultTestConfig()
		snapStore := &mockSnapshotStore{}
		reader := &mockStreamReader{}

		repo, err := snapshot.NewRepository(reader, snapStore, cfg)
		if err != nil {
			t.Fatalf("NewRepository failed: %v", err)
		}
		res, err := repo.Load(ctx, nil, "user-1")
		if err != nil {
			t.Fatalf("Load failed: %v", err)
		}
		if res.SnapshotHit {
			t.Errorf("expected SnapshotHit to be false")
		}
		if res.SnapshotVersion != 0 {
			t.Errorf("expected SnapshotVersion 0, got %d", res.SnapshotVersion)
		}
		if res.StreamVersion != 0 {
			t.Errorf("expected StreamVersion 0, got %d", res.StreamVersion)
		}
		if res.EventsReplayed != 0 {
			t.Errorf("expected EventsReplayed 0, got %d", res.EventsReplayed)
		}
	})
}

func TestSnapshotPolicy(t *testing.T) {
	t.Run("EveryNEvents", func(t *testing.T) {
		policy := snapshot.EveryNEvents(100)

		// 1. Not enough events: snapshot at v100, current at v150, 20 new events (total delta 50+20=70 < 100)
		res := snapshot.Result[*userState]{
			StreamVersion:   150,
			SnapshotVersion: 100,
			SnapshotHit:     true,
		}
		if res.ShouldSnapshot(policy, 20) {
			t.Errorf("expected EveryNEvents(100) to return false for 70 events delta")
		}

		// 2. Exactly reaches threshold: delta 50 + 50 appended = 100
		if !res.ShouldSnapshot(policy, 50) {
			t.Errorf("expected EveryNEvents(100) to return true for 100 events delta")
		}

		// 3. Multi-event batch jumps over threshold: delta 50 + 65 appended = 115 >= 100
		if !res.ShouldSnapshot(policy, 65) {
			t.Errorf("expected EveryNEvents(100) to return true for 115 events delta (batch jump)")
		}

		// 4. Direct policy call
		if !policy(150, 100, 65) {
			t.Errorf("expected direct policy call to return true")
		}

		// 5. No snapshot loaded: snapshotVersion=0, streamVersion=80, 25 appended = 105 >= 100
		noSnapRes := snapshot.Result[*userState]{
			StreamVersion:   80,
			SnapshotVersion: 0,
			SnapshotHit:     false,
		}
		if !noSnapRes.ShouldSnapshot(policy, 25) {
			t.Errorf("expected EveryNEvents(100) to return true for 105 events since inception")
		}

		// 6. Invalid n (<= 0) returns false
		zeroPolicy := snapshot.EveryNEvents(0)
		if res.ShouldSnapshot(zeroPolicy, 1000) {
			t.Errorf("expected EveryNEvents(0) to return false")
		}

		// 7. Nil policy returns false
		if res.ShouldSnapshot(nil, 1000) {
			t.Errorf("expected nil policy to return false")
		}
	})

	t.Run("Never", func(t *testing.T) {
		policy := snapshot.Never()
		res := snapshot.Result[*userState]{
			StreamVersion:   1000,
			SnapshotVersion: 0,
		}
		if res.ShouldSnapshot(policy, 1000) {
			t.Errorf("expected Never() to return false")
		}
	})
}

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

func TestRepository_Codec_StreamIDValidation(t *testing.T) {
	ctx := context.Background()

	t.Run("decode receives streamID and can reject foreign stream payloads", func(t *testing.T) {
		cfg := defaultTestConfig()
		cfg.Codec = snapshot.FuncCodec(
			func(s *userState) ([]byte, error) {
				return json.Marshal(s)
			},
			func(streamID string, data []byte) (*userState, error) {
				var s userState
				if err := json.Unmarshal(data, &s); err != nil {
					return nil, err
				}
				if s.ID != streamID {
					return nil, fmt.Errorf("stream ID mismatch: expected %q, got %q", streamID, s.ID)
				}
				return &s, nil
			},
		)

		foreignPayload, err := json.Marshal(&userState{ID: "other-user", Name: "Foreign"})
		if err != nil {
			t.Fatalf("Marshal foreignPayload: %v", err)
		}
		snapStore := &mockSnapshotStore{
			getFunc: func(_ context.Context, _ pgx.Tx, streamType, streamID string, _ int) (snapshot.Snapshot, error) {
				return snapshot.Snapshot{
					StreamType:    streamType,
					StreamID:      streamID,
					StreamVersion: 5,
					SchemaVersion: 1,
					Payload:       foreignPayload,
				}, nil
			},
		}

		reader := &mockStreamReader{
			readStreamFunc: func(_ context.Context, _ pgx.Tx, streamType, streamID string, fromVersion, _ *int64) (store.Stream, error) {
				// Fallback full replay from beginning
				if fromVersion == nil {
					return store.Stream{
						StreamType: streamType,
						StreamID:   streamID,
						Events: []store.PersistedEvent{
							{StreamVersion: 1, EventType: "UserCreated", Payload: []byte(`{"name":"Local"}`)},
						},
					}, nil
				}
				return store.Stream{StreamType: streamType, StreamID: streamID}, nil
			},
		}

		repo, err := snapshot.NewRepository(reader, snapStore, cfg)
		if err != nil {
			t.Fatalf("NewRepository failed: %v", err)
		}

		res, err := repo.Load(ctx, nil, "expected-user")
		if err != nil {
			t.Fatalf("Load failed: %v", err)
		}

		// Snapshot should be discarded because Unmarshal rejected the foreign payload, falling back to full replay
		if res.SnapshotHit {
			t.Errorf("expected SnapshotHit = false due to stream ID mismatch")
		}
		if res.StreamVersion != 1 {
			t.Errorf("expected StreamVersion = 1 from replay, got %d", res.StreamVersion)
		}
		if res.State.Name != "Local" {
			t.Errorf("expected state Name = 'Local', got %q", res.State.Name)
		}
	})
}

func TestRepository_VersionValidation(t *testing.T) {
	ctx := context.Background()

	t.Run("Save succeeds when Version matches expected version", func(t *testing.T) {
		cfg := defaultTestConfig()
		cfg.Version = func(_ *userState) int64 {
			return 10
		}

		reader := &mockStreamReader{
			readStreamFunc: func(_ context.Context, _ pgx.Tx, streamType, streamID string, _, _ *int64) (store.Stream, error) {
				return store.Stream{
					StreamType: streamType,
					StreamID:   streamID,
					Events: []store.PersistedEvent{
						{StreamVersion: 10},
					},
				}, nil
			},
		}
		snapStore := &mockSnapshotStore{}
		repo, err := snapshot.NewRepository(reader, snapStore, cfg)
		if err != nil {
			t.Fatalf("NewRepository: %v", err)
		}

		err = repo.Save(ctx, nil, "u-1", 10, &userState{ID: "u-1", Name: "Alice"})
		if err != nil {
			t.Fatalf("expected Save to succeed, got: %v", err)
		}
	})

	t.Run("Save fails when Version does not match expected version", func(t *testing.T) {
		cfg := defaultTestConfig()
		cfg.Version = func(_ *userState) int64 {
			return 9 // Mismatched version
		}

		reader := &mockStreamReader{}
		snapStore := &mockSnapshotStore{}
		repo, err := snapshot.NewRepository(reader, snapStore, cfg)
		if err != nil {
			t.Fatalf("NewRepository: %v", err)
		}

		err = repo.Save(ctx, nil, "u-1", 10, &userState{ID: "u-1", Name: "Alice"})
		if err == nil || !strings.Contains(err.Error(), "state version 9 does not match save version 10") {
			t.Fatalf("expected version mismatch error, got: %v", err)
		}
	})
}

func TestRepository_OnSnapshotRejected(t *testing.T) {
	ctx := context.Background()

	t.Run("triggers OnSnapshotRejected on corrupt decode", func(t *testing.T) {
		var rejectedStreamID, rejectedReason string
		var rejectedErr error

		cfg := defaultTestConfig()
		cfg.OnSnapshotRejected = func(_ context.Context, streamID, reason string, err error) {
			rejectedStreamID = streamID
			rejectedReason = reason
			rejectedErr = err
		}

		snapStore := &mockSnapshotStore{
			getFunc: func(_ context.Context, _ pgx.Tx, streamType, streamID string, _ int) (snapshot.Snapshot, error) {
				return snapshot.Snapshot{
					StreamType:    streamType,
					StreamID:      streamID,
					StreamVersion: 5,
					SchemaVersion: 1,
					Payload:       []byte("invalid json"),
				}, nil
			},
		}

		reader := &mockStreamReader{}
		repo, err := snapshot.NewRepository(reader, snapStore, cfg)
		if err != nil {
			t.Fatalf("NewRepository: %v", err)
		}

		_, err = repo.Load(ctx, nil, "user-corrupt")
		if err != nil {
			t.Fatalf("Load: %v", err)
		}

		if rejectedStreamID != "user-corrupt" {
			t.Errorf("expected rejectedStreamID = 'user-corrupt', got %q", rejectedStreamID)
		}
		if rejectedReason != "failed to decode snapshot payload" {
			t.Errorf("expected reason 'failed to decode snapshot payload', got %q", rejectedReason)
		}
		if rejectedErr == nil {
			t.Errorf("expected non-nil rejectedErr")
		}
	})

	t.Run("triggers OnSnapshotRejected on newer schema version", func(t *testing.T) {
		var rejectedReason string

		cfg := defaultTestConfig()
		cfg.SchemaVersion = 1
		cfg.OnSnapshotRejected = func(_ context.Context, _, reason string, _ error) {
			rejectedReason = reason
		}

		snapStore := &mockSnapshotStore{
			getFunc: func(_ context.Context, _ pgx.Tx, streamType, streamID string, _ int) (snapshot.Snapshot, error) {
				return snapshot.Snapshot{
					StreamType:    streamType,
					StreamID:      streamID,
					StreamVersion: 5,
					SchemaVersion: 2, // Newer than cfg.SchemaVersion (1)
					Payload:       []byte(`{"id":"u1"}`),
				}, nil
			},
		}

		reader := &mockStreamReader{}
		repo, err := snapshot.NewRepository(reader, snapStore, cfg)
		if err != nil {
			t.Fatalf("NewRepository: %v", err)
		}

		_, err = repo.Load(ctx, nil, "user-newer-schema")
		if err != nil {
			t.Fatalf("Load: %v", err)
		}

		if rejectedReason != "snapshot schema version is newer than configured repository schema version" {
			t.Errorf("expected reason 'snapshot schema version is newer than configured repository schema version', got %q", rejectedReason)
		}
	})

	t.Run("triggers OnSnapshotRejected when snapshot version not found in event log", func(t *testing.T) {
		var rejectedReason string

		payload, err := json.Marshal(&userState{ID: "u-1", Name: "Valid"})
		if err != nil {
			t.Fatalf("Marshal payload: %v", err)
		}
		cfg := defaultTestConfig()
		cfg.OnSnapshotRejected = func(_ context.Context, _, reason string, _ error) {
			rejectedReason = reason
		}

		snapStore := &mockSnapshotStore{
			getFunc: func(_ context.Context, _ pgx.Tx, streamType, streamID string, _ int) (snapshot.Snapshot, error) {
				return snapshot.Snapshot{
					StreamType:    streamType,
					StreamID:      streamID,
					StreamVersion: 50,
					SchemaVersion: 1,
					Payload:       payload,
				}, nil
			},
		}

		reader := &mockStreamReader{
			readStreamFunc: func(_ context.Context, _ pgx.Tx, streamType, streamID string, _, _ *int64) (store.Stream, error) {
				// fromVersion 51 -> empty delta
				// verify check at 50 -> empty stream (head does not reach 50)
				return store.Stream{StreamType: streamType, StreamID: streamID}, nil
			},
		}

		repo, err := snapshot.NewRepository(reader, snapStore, cfg)
		if err != nil {
			t.Fatalf("NewRepository: %v", err)
		}

		_, err = repo.Load(ctx, nil, "u-1")
		if err != nil {
			t.Fatalf("Load: %v", err)
		}

		if rejectedReason != "snapshot version 50 not found in event log" {
			t.Errorf("expected reason 'snapshot version 50 not found in event log', got %q", rejectedReason)
		}
	})
}

func TestJSONCodec(t *testing.T) {
	t.Run("pointer type encode and decode", func(t *testing.T) {
		codec := snapshot.JSON[*userState]()
		original := &userState{ID: "u-123", Name: "Alice", Email: "alice@example.com"}

		data, err := codec.Encode(original)
		if err != nil {
			t.Fatalf("Encode failed: %v", err)
		}

		decoded, err := codec.Decode("u-123", data)
		if err != nil {
			t.Fatalf("Decode failed: %v", err)
		}

		if decoded == nil || decoded.ID != "u-123" || decoded.Name != "Alice" || decoded.Email != "alice@example.com" {
			t.Errorf("unexpected decoded state: %+v", decoded)
		}
	})

	t.Run("value type encode and decode", func(t *testing.T) {
		codec := snapshot.JSON[userState]()
		original := userState{ID: "u-456", Name: "Bob", Email: "bob@example.com"}

		data, err := codec.Encode(original)
		if err != nil {
			t.Fatalf("Encode failed: %v", err)
		}

		decoded, err := codec.Decode("u-456", data)
		if err != nil {
			t.Fatalf("Decode failed: %v", err)
		}

		if decoded.ID != "u-456" || decoded.Name != "Bob" || decoded.Email != "bob@example.com" {
			t.Errorf("unexpected decoded state: %+v", decoded)
		}
	})

	t.Run("corrupt payload decode error", func(t *testing.T) {
		codec := snapshot.JSON[*userState]()
		_, err := codec.Decode("u-123", []byte(`{invalid-json`))
		if err == nil {
			t.Error("expected error for corrupt JSON payload")
		}
	})
}

func TestFuncCodec(t *testing.T) {
	t.Run("successful encode and decode", func(t *testing.T) {
		fc := snapshot.FuncCodec(
			func(s *userState) ([]byte, error) {
				return []byte(s.ID + ":" + s.Name), nil
			},
			func(streamID string, payload []byte) (*userState, error) {
				if streamID == "" {
					return nil, errors.New("empty streamID")
				}
				parts := strings.Split(string(payload), ":")
				return &userState{ID: parts[0], Name: parts[1]}, nil
			},
		)

		encoded, err := fc.Encode(&userState{ID: "u-1", Name: "Test"})
		if err != nil {
			t.Fatalf("Encode failed: %v", err)
		}
		if string(encoded) != "u-1:Test" {
			t.Errorf("expected 'u-1:Test', got %q", string(encoded))
		}

		decoded, err := fc.Decode("u-1", encoded)
		if err != nil {
			t.Fatalf("Decode failed: %v", err)
		}
		if decoded.ID != "u-1" || decoded.Name != "Test" {
			t.Errorf("unexpected decoded state: %+v", decoded)
		}
	})

	t.Run("nil encode function returns error", func(t *testing.T) {
		fc := snapshot.FuncCodec[*userState](nil, func(_ string, _ []byte) (*userState, error) {
			return &userState{}, nil
		})

		_, err := fc.Encode(&userState{})
		if err == nil || !strings.Contains(err.Error(), "encode func cannot be nil") {
			t.Errorf("expected nil encode error, got: %v", err)
		}
	})

	t.Run("nil decode function returns error", func(t *testing.T) {
		fc := snapshot.FuncCodec[*userState](func(_ *userState) ([]byte, error) {
			return []byte("data"), nil
		}, nil)

		_, err := fc.Decode("id", []byte("data"))
		if err == nil || !strings.Contains(err.Error(), "decode func cannot be nil") {
			t.Errorf("expected nil decode error, got: %v", err)
		}
	})
}

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
