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
	getFunc func(ctx context.Context, tx pgx.Tx, streamType, streamID string) (snapshot.Snapshot, error)
	putFunc func(ctx context.Context, tx pgx.Tx, snap *snapshot.Snapshot) error
}

func (m *mockSnapshotStore) Get(ctx context.Context, tx pgx.Tx, streamType, streamID string) (snapshot.Snapshot, error) {
	if m.getFunc != nil {
		return m.getFunc(ctx, tx, streamType, streamID)
	}
	return snapshot.Snapshot{}, nil
}

func (m *mockSnapshotStore) Put(ctx context.Context, tx pgx.Tx, snap *snapshot.Snapshot) error {
	if m.putFunc != nil {
		return m.putFunc(ctx, tx, snap)
	}
	return nil
}

type mockEventStore struct {
	appendFunc func(ctx context.Context, tx pgx.Tx, expectedVersion store.ExpectedVersion, events []store.Event) (store.AppendResult, error)
}

func (m *mockEventStore) Append(ctx context.Context, tx pgx.Tx, expectedVersion store.ExpectedVersion, events []store.Event) (store.AppendResult, error) {
	if m.appendFunc != nil {
		return m.appendFunc(ctx, tx, expectedVersion, events)
	}
	return store.AppendResult{}, nil
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
		Marshal: func(s *userState) ([]byte, error) {
			return json.Marshal(s)
		},
		Unmarshal: func(data []byte) (*userState, error) {
			var s userState
			if err := json.Unmarshal(data, &s); err != nil {
				return nil, err
			}
			return &s, nil
		},
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
			name:   "nil marshal",
			reader: reader,
			store:  snapStore,
			cfgMod: func(c *snapshot.RepositoryConfig[*userState]) {
				c.Marshal = nil
			},
			wantErr:   true,
			errSubstr: "marshal cannot be nil",
		},
		{
			name:   "nil unmarshal",
			reader: reader,
			store:  snapStore,
			cfgMod: func(c *snapshot.RepositoryConfig[*userState]) {
				c.Unmarshal = nil
			},
			wantErr:   true,
			errSubstr: "unmarshal cannot be nil",
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
			getFunc: func(_ context.Context, _ pgx.Tx, _, _ string) (snapshot.Snapshot, error) {
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
			getFunc: func(_ context.Context, _ pgx.Tx, _, _ string) (snapshot.Snapshot, error) {
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
			getFunc: func(_ context.Context, _ pgx.Tx, _, _ string) (snapshot.Snapshot, error) {
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
			getFunc: func(_ context.Context, _ pgx.Tx, _, _ string) (snapshot.Snapshot, error) {
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
			getFunc: func(_ context.Context, _ pgx.Tx, _, _ string) (snapshot.Snapshot, error) {
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

	t.Run("marshal error", func(t *testing.T) {
		cfgErr := cfg
		cfgErr.Marshal = func(_ *userState) ([]byte, error) {
			return nil, errors.New("marshal error")
		}

		repo, err := snapshot.NewRepository(&mockStreamReader{}, &mockSnapshotStore{}, cfgErr)
		if err != nil {
			t.Fatalf("NewRepository failed: %v", err)
		}
		err = repo.Save(ctx, nil, "user-1", 5, &userState{ID: "user-1"})
		if err == nil {
			t.Fatal("expected marshal error, got nil")
		}
	})

	t.Run("put store error", func(t *testing.T) {
		snapStore := &mockSnapshotStore{
			putFunc: func(_ context.Context, _ pgx.Tx, _ *snapshot.Snapshot) error {
				return errors.New("put failed")
			},
		}

		repo, err := snapshot.NewRepository(&mockStreamReader{}, snapStore, cfg)
		if err != nil {
			t.Fatalf("NewRepository failed: %v", err)
		}
		err = repo.Save(ctx, nil, "user-1", 5, &userState{ID: "user-1"})
		if err == nil {
			t.Fatal("expected store put error, got nil")
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

		repo, err := snapshot.NewRepository(&mockStreamReader{}, snapStore, cfg)
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
			getFunc: func(_ context.Context, _ pgx.Tx, _, _ string) (snapshot.Snapshot, error) {
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
		if res.SchemaVersion != 1 {
			t.Errorf("expected SchemaVersion 1, got %d", res.SchemaVersion)
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
			getFunc: func(_ context.Context, _ pgx.Tx, _, _ string) (snapshot.Snapshot, error) {
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

func TestRepository_SaveAppended(t *testing.T) {
	ctx := context.Background()

	t.Run("applies events and saves snapshot at new version", func(t *testing.T) {
		cfg := defaultTestConfig()
		var savedSnap *snapshot.Snapshot
		snapStore := &mockSnapshotStore{
			putFunc: func(_ context.Context, _ pgx.Tx, snap *snapshot.Snapshot) error {
				savedSnap = snap
				return nil
			},
		}
		repo, err := snapshot.NewRepository(&mockStreamReader{}, snapStore, cfg)
		if err != nil {
			t.Fatalf("NewRepository: %v", err)
		}

		initialState := &userState{ID: "user-1", Name: "Alice"}
		ev1, err := json.Marshal(userEvent{Name: "Alice Smith"})
		if err != nil {
			t.Fatalf("marshal failed: %v", err)
		}
		ev2, err := json.Marshal(userEvent{Email: "alice@example.com"})
		if err != nil {
			t.Fatalf("marshal failed: %v", err)
		}

		appendResult := store.AppendResult{
			Events: []store.PersistedEvent{
				{StreamType: "User", StreamID: "user-1", StreamVersion: 4, Payload: ev1},
				{StreamType: "User", StreamID: "user-1", StreamVersion: 5, Payload: ev2},
			},
		}

		updatedState, err := repo.SaveAppended(ctx, nil, "user-1", initialState, appendResult)
		if err != nil {
			t.Fatalf("SaveAppended failed: %v", err)
		}

		if updatedState.Name != "Alice Smith" || updatedState.Email != "alice@example.com" {
			t.Errorf("state not properly folded: %+v", updatedState)
		}
		if savedSnap == nil {
			t.Fatal("expected snapshot to be saved")
		}
		if savedSnap.StreamVersion != 5 {
			t.Errorf("expected snapshot version 5, got %d", savedSnap.StreamVersion)
		}
	})

	t.Run("empty append result is no-op", func(t *testing.T) {
		cfg := defaultTestConfig()
		var savedSnap *snapshot.Snapshot
		snapStore := &mockSnapshotStore{
			putFunc: func(_ context.Context, _ pgx.Tx, snap *snapshot.Snapshot) error {
				savedSnap = snap
				return nil
			},
		}
		repo, err := snapshot.NewRepository(&mockStreamReader{}, snapStore, cfg)
		if err != nil {
			t.Fatalf("NewRepository: %v", err)
		}

		initialState := &userState{ID: "user-1", Name: "Alice"}
		updatedState, err := repo.SaveAppended(ctx, nil, "user-1", initialState, store.AppendResult{})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if updatedState != initialState {
			t.Errorf("expected unchanged state")
		}
		if savedSnap != nil {
			t.Error("expected no snapshot to be saved for empty append result")
		}
	})

	t.Run("apply error propagates", func(t *testing.T) {
		cfg := defaultTestConfig()
		cfg.Apply = func(_ *userState, _ store.PersistedEvent) (*userState, error) {
			return nil, errors.New("apply error")
		}
		repo, err := snapshot.NewRepository(&mockStreamReader{}, &mockSnapshotStore{}, cfg)
		if err != nil {
			t.Fatalf("NewRepository: %v", err)
		}

		initialState := &userState{ID: "user-1"}
		appendResult := store.AppendResult{
			Events: []store.PersistedEvent{
				{StreamType: "User", StreamID: "user-1", StreamVersion: 1, Payload: []byte("{}")},
			},
		}
		_, err = repo.SaveAppended(ctx, nil, "user-1", initialState, appendResult)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
	})

	t.Run("save error propagates", func(t *testing.T) {
		cfg := defaultTestConfig()
		snapStore := &mockSnapshotStore{
			putFunc: func(_ context.Context, _ pgx.Tx, _ *snapshot.Snapshot) error {
				return errors.New("db write failure")
			},
		}
		repo, err := snapshot.NewRepository(&mockStreamReader{}, snapStore, cfg)
		if err != nil {
			t.Fatalf("NewRepository: %v", err)
		}

		initialState := &userState{ID: "user-1"}
		appendResult := store.AppendResult{
			Events: []store.PersistedEvent{
				{StreamType: "User", StreamID: "user-1", StreamVersion: 1, Payload: []byte("{}")},
			},
		}
		_, err = repo.SaveAppended(ctx, nil, "user-1", initialState, appendResult)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
	})
}

func TestRepository_Execute(t *testing.T) {
	ctx := context.Background()
	cfg := defaultTestConfig()

	t.Run("nil event store or handler validation", func(t *testing.T) {
		repo, err := snapshot.NewRepository(&mockStreamReader{}, &mockSnapshotStore{}, cfg)
		if err != nil {
			t.Fatalf("NewRepository failed: %v", err)
		}
		_, _, err = repo.Execute(ctx, nil, "user-1", nil, func(_ *userState, _ int64) ([]store.Event, error) {
			return nil, nil
		}, nil)
		if err == nil {
			t.Error("expected error for nil eventStore")
		}

		_, _, err = repo.Execute(ctx, nil, "user-1", &mockEventStore{}, nil, nil)
		if err == nil {
			t.Error("expected error for nil handler")
		}
	})

	t.Run("load error propagates", func(t *testing.T) {
		snapStore := &mockSnapshotStore{
			getFunc: func(_ context.Context, _ pgx.Tx, _, _ string) (snapshot.Snapshot, error) {
				return snapshot.Snapshot{}, errors.New("load failed")
			},
		}
		repo, err := snapshot.NewRepository(&mockStreamReader{}, snapStore, cfg)
		if err != nil {
			t.Fatalf("NewRepository failed: %v", err)
		}
		_, _, err = repo.Execute(ctx, nil, "user-1", &mockEventStore{}, func(_ *userState, _ int64) ([]store.Event, error) {
			return nil, nil
		}, nil)
		if err == nil {
			t.Error("expected error from Load")
		}
	})

	t.Run("handler error propagates before append", func(t *testing.T) {
		repo, err := snapshot.NewRepository(&mockStreamReader{}, &mockSnapshotStore{}, cfg)
		if err != nil {
			t.Fatalf("NewRepository failed: %v", err)
		}
		var appendCalled bool
		es := &mockEventStore{
			appendFunc: func(_ context.Context, _ pgx.Tx, _ store.ExpectedVersion, _ []store.Event) (store.AppendResult, error) {
				appendCalled = true
				return store.AppendResult{}, nil
			},
		}
		_, _, err = repo.Execute(ctx, nil, "user-1", es, func(_ *userState, _ int64) ([]store.Event, error) {
			return nil, errors.New("domain validation failed")
		}, nil)
		if err == nil {
			t.Error("expected error from handler")
		}
		if appendCalled {
			t.Error("append should not be called when handler returns error")
		}
	})

	t.Run("no events from handler is no-op", func(t *testing.T) {
		repo, err := snapshot.NewRepository(&mockStreamReader{}, &mockSnapshotStore{}, cfg)
		if err != nil {
			t.Fatalf("NewRepository failed: %v", err)
		}
		var appendCalled bool
		es := &mockEventStore{
			appendFunc: func(_ context.Context, _ pgx.Tx, _ store.ExpectedVersion, _ []store.Event) (store.AppendResult, error) {
				appendCalled = true
				return store.AppendResult{}, nil
			},
		}
		res, appendRes, err := repo.Execute(ctx, nil, "user-1", es, func(_ *userState, _ int64) ([]store.Event, error) {
			return []store.Event{}, nil
		}, snapshot.EveryNEvents(1))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if appendCalled {
			t.Error("expected append not to be called for empty events")
		}
		if len(appendRes.Events) != 0 {
			t.Error("expected empty append result")
		}
		if res.State.ID != "user-1" {
			t.Errorf("unexpected state: %+v", res.State)
		}
	})

	t.Run("mismatched stream type or id", func(t *testing.T) {
		repo, err := snapshot.NewRepository(&mockStreamReader{}, &mockSnapshotStore{}, cfg)
		if err != nil {
			t.Fatalf("NewRepository failed: %v", err)
		}
		es := &mockEventStore{}
		_, _, err = repo.Execute(ctx, nil, "user-1", es, func(_ *userState, _ int64) ([]store.Event, error) {
			return []store.Event{{StreamType: "Order"}}, nil
		}, nil)
		if err == nil {
			t.Error("expected error for mismatched stream type")
		}

		_, _, err = repo.Execute(ctx, nil, "user-1", es, func(_ *userState, _ int64) ([]store.Event, error) {
			return []store.Event{{StreamType: "User", StreamID: "wrong-id"}}, nil
		}, nil)
		if err == nil {
			t.Error("expected error for mismatched stream id")
		}
	})

	t.Run("successful execution with snapshot policy trigger", func(t *testing.T) {
		var savedSnap *snapshot.Snapshot
		snapStore := &mockSnapshotStore{
			putFunc: func(_ context.Context, _ pgx.Tx, snap *snapshot.Snapshot) error {
				savedSnap = snap
				return nil
			},
		}
		repo, err := snapshot.NewRepository(&mockStreamReader{}, snapStore, cfg)
		if err != nil {
			t.Fatalf("NewRepository failed: %v", err)
		}

		var expectedVerPassed store.ExpectedVersion
		es := &mockEventStore{
			appendFunc: func(_ context.Context, _ pgx.Tx, expectedVersion store.ExpectedVersion, events []store.Event) (store.AppendResult, error) {
				expectedVerPassed = expectedVersion
				resEvents := make([]store.PersistedEvent, len(events))
				for i, ev := range events {
					resEvents[i] = store.PersistedEvent{
						StreamType:    ev.StreamType,
						StreamID:      ev.StreamID,
						StreamVersion: int64(i + 1),
						Payload:       ev.Payload,
					}
				}
				return store.AppendResult{Events: resEvents}, nil
			},
		}

		ev1, err := json.Marshal(userEvent{Name: "Bob"})
		if err != nil {
			t.Fatalf("marshal failed: %v", err)
		}
		ev2, err := json.Marshal(userEvent{Email: "bob@example.com"})
		if err != nil {
			t.Fatalf("marshal failed: %v", err)
		}

		res, appendRes, err := repo.Execute(ctx, nil, "user-1", es, func(_ *userState, version int64) ([]store.Event, error) {
			if version != 0 {
				t.Errorf("expected version 0 for new stream, got %d", version)
			}
			return []store.Event{
				{Payload: ev1}, // StreamType, StreamID, EventID, CreatedAt automatically defaulted
				{Payload: ev2},
			}, nil
		}, snapshot.EveryNEvents(2))

		if err != nil {
			t.Fatalf("Execute failed: %v", err)
		}
		if !expectedVerPassed.IsNoStream() {
			t.Errorf("expected NoStream for v0 load, got %v", expectedVerPassed)
		}
		if len(appendRes.Events) != 2 {
			t.Fatalf("expected 2 appended events, got %d", len(appendRes.Events))
		}
		if res.StreamVersion != 2 {
			t.Errorf("expected final stream version 2, got %d", res.StreamVersion)
		}
		if res.State.Name != "Bob" || res.State.Email != "bob@example.com" {
			t.Errorf("unexpected folded state: %+v", res.State)
		}
		if savedSnap == nil {
			t.Fatal("expected snapshot to be saved by policy EveryNEvents(2)")
		}
		if savedSnap.StreamVersion != 2 {
			t.Errorf("expected snapshot version 2, got %d", savedSnap.StreamVersion)
		}
		if !res.SnapshotHit || res.SnapshotVersion != 2 || res.EventsReplayed != 0 {
			t.Errorf("unexpected metadata on post-snapshot result: %+v", res)
		}
	})

	t.Run("append error or save error propagates", func(t *testing.T) {
		repo, err := snapshot.NewRepository(&mockStreamReader{}, &mockSnapshotStore{}, cfg)
		if err != nil {
			t.Fatalf("NewRepository failed: %v", err)
		}
		esErr := &mockEventStore{
			appendFunc: func(_ context.Context, _ pgx.Tx, _ store.ExpectedVersion, _ []store.Event) (store.AppendResult, error) {
				return store.AppendResult{}, errors.New("append failed")
			},
		}
		_, _, err = repo.Execute(ctx, nil, "user-1", esErr, func(_ *userState, _ int64) ([]store.Event, error) {
			return []store.Event{{Payload: []byte("{}")}}, nil
		}, nil)
		if err == nil {
			t.Error("expected error from append")
		}

		snapStoreErr := &mockSnapshotStore{
			putFunc: func(_ context.Context, _ pgx.Tx, _ *snapshot.Snapshot) error {
				return errors.New("put failed")
			},
		}
		repoSaveErr, err := snapshot.NewRepository(&mockStreamReader{}, snapStoreErr, cfg)
		if err != nil {
			t.Fatalf("NewRepository failed: %v", err)
		}
		esOk := &mockEventStore{
			appendFunc: func(_ context.Context, _ pgx.Tx, _ store.ExpectedVersion, events []store.Event) (store.AppendResult, error) {
				return store.AppendResult{
					Events: []store.PersistedEvent{{StreamVersion: 1, Payload: events[0].Payload}},
				}, nil
			},
		}
		_, _, err = repoSaveErr.Execute(ctx, nil, "user-1", esOk, func(_ *userState, _ int64) ([]store.Event, error) {
			return []store.Event{{Payload: []byte("{}")}}, nil
		}, snapshot.EveryNEvents(1))
		if err == nil {
			t.Error("expected error from snapshot save")
		}
	})

	t.Run("existing stream passes store.Exact expected version", func(t *testing.T) {
		reader := &mockStreamReader{
			readStreamFunc: func(_ context.Context, _ pgx.Tx, streamType, streamID string, _, _ *int64) (store.Stream, error) {
				return store.Stream{
					StreamType: streamType,
					StreamID:   streamID,
					Events: []store.PersistedEvent{
						{StreamType: streamType, StreamID: streamID, StreamVersion: 5, Payload: []byte("{}")},
					},
				}, nil
			},
		}
		repo, err := snapshot.NewRepository(reader, &mockSnapshotStore{}, cfg)
		if err != nil {
			t.Fatalf("NewRepository failed: %v", err)
		}

		var expectedVerPassed store.ExpectedVersion
		es := &mockEventStore{
			appendFunc: func(_ context.Context, _ pgx.Tx, expectedVersion store.ExpectedVersion, events []store.Event) (store.AppendResult, error) {
				expectedVerPassed = expectedVersion
				return store.AppendResult{
					Events: []store.PersistedEvent{
						{StreamType: events[0].StreamType, StreamID: events[0].StreamID, StreamVersion: 6, Payload: events[0].Payload},
					},
				}, nil
			},
		}

		res, _, err := repo.Execute(ctx, nil, "user-1", es, func(_ *userState, version int64) ([]store.Event, error) {
			if version != 5 {
				t.Errorf("expected version 5, got %d", version)
			}
			return []store.Event{{Payload: []byte("{}")}}, nil
		}, nil)

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !expectedVerPassed.IsExact() || expectedVerPassed.Value() != 5 {
			t.Errorf("expected store.Exact(5), got %v", expectedVerPassed)
		}
		if res.StreamVersion != 6 {
			t.Errorf("expected stream version 6, got %d", res.StreamVersion)
		}
	})

	t.Run("apply error during execute folding propagates", func(t *testing.T) {
		cfgErr := cfg
		cfgErr.Apply = func(u *userState, event store.PersistedEvent) (*userState, error) {
			if event.StreamVersion == 2 {
				return nil, errors.New("corrupt event data")
			}
			return u, nil
		}
		repo, err := snapshot.NewRepository(&mockStreamReader{}, &mockSnapshotStore{}, cfgErr)
		if err != nil {
			t.Fatalf("NewRepository failed: %v", err)
		}

		es := &mockEventStore{
			appendFunc: func(_ context.Context, _ pgx.Tx, _ store.ExpectedVersion, events []store.Event) (store.AppendResult, error) {
				return store.AppendResult{
					Events: []store.PersistedEvent{
						{StreamType: events[0].StreamType, StreamID: events[0].StreamID, StreamVersion: 1, Payload: events[0].Payload},
						{StreamType: events[1].StreamType, StreamID: events[1].StreamID, StreamVersion: 2, Payload: events[1].Payload},
					},
				}, nil
			},
		}

		_, _, err = repo.Execute(ctx, nil, "user-1", es, func(_ *userState, _ int64) ([]store.Event, error) {
			return []store.Event{{Payload: []byte("{}")}, {Payload: []byte("{}")}}, nil
		}, nil)
		if err == nil {
			t.Fatal("expected apply error during execute, got nil")
		}
		if !strings.Contains(err.Error(), "failed to apply appended event v2") {
			t.Errorf("unexpected error message: %v", err)
		}
	})
}
