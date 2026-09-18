//go:build integration

package integration_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/eventsalsa/store"
	storepostgres "github.com/eventsalsa/store/postgres"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/eventsalsa/snapshot"
	snapshotpostgres "github.com/eventsalsa/snapshot/postgres"
)

// TestUser is the stream state model used for testing.
type TestUser struct {
	ID    string `json:"id"`
	Name  string `json:"name"`
	Email string `json:"email"`
}

type TestEvent struct {
	Name  string `json:"name"`
	Email string `json:"email"`
}

func setupPostgres(t *testing.T) *pgxpool.Pool {
	t.Helper()
	ctx := context.Background()

	// Spin up PostgreSQL container
	ctr, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("test_snapshot"),
		tcpostgres.WithUsername("test"),
		tcpostgres.WithPassword("test"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(30*time.Second),
		),
	)
	if err != nil {
		t.Fatalf("start postgres container: %v", err)
	}
	t.Cleanup(func() {
		if err := ctr.Terminate(ctx); err != nil {
			t.Logf("terminate container: %v", err)
		}
	})

	connStr, err := ctr.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("connection string: %v", err)
	}

	db, err := pgxpool.New(ctx, connStr)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	// Create tables for the event store
	eventStoreDDL := `
	CREATE TABLE IF NOT EXISTS events (
		global_position BIGSERIAL PRIMARY KEY,
		stream_type TEXT NOT NULL,
		stream_id TEXT NOT NULL,
		stream_version BIGINT NOT NULL,
		event_id UUID NOT NULL UNIQUE,
		event_type TEXT NOT NULL,
		event_version INT NOT NULL DEFAULT 1,
		payload BYTEA NOT NULL,
		trace_id TEXT,
		correlation_id TEXT,
		causation_id TEXT,
		metadata JSONB,
		created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
		UNIQUE (stream_type, stream_id, stream_version)
	);
	CREATE TABLE IF NOT EXISTS stream_heads (
		stream_type TEXT NOT NULL,
		stream_id TEXT NOT NULL,
		stream_version BIGINT NOT NULL,
		updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
		PRIMARY KEY (stream_type, stream_id)
	);
	`
	// Create tables for snapshots
	snapshotDDL := `
	CREATE TABLE IF NOT EXISTS snapshots (
		stream_type TEXT NOT NULL,
		stream_id TEXT NOT NULL,
		stream_version BIGINT NOT NULL,
		schema_version INT NOT NULL,
		payload BYTEA NOT NULL,
		created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
		PRIMARY KEY (stream_type, stream_id)
	);
	`

	if _, err := db.Exec(ctx, eventStoreDDL); err != nil {
		t.Fatalf("create event store tables: %v", err)
	}
	if _, err := db.Exec(ctx, snapshotDDL); err != nil {
		t.Fatalf("create snapshot tables: %v", err)
	}

	return db
}

func appendTestEvent(t *testing.T, ctx context.Context, tx pgx.Tx, es store.EventStore, id, eventType, name, email string, expectedVersion store.ExpectedVersion) int64 {
	t.Helper()
	payload, err := json.Marshal(TestEvent{Name: name, Email: email})
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}

	events := []store.Event{
		{
			StreamType: "User",
			StreamID:   id,
			EventID:    uuid.New(),
			EventType:  eventType,
			Payload:    payload,
			CreatedAt:  time.Now(),
		},
	}

	res, err := es.Append(ctx, tx, expectedVersion, events)
	if err != nil {
		t.Fatalf("append event: %v", err)
	}

	return res.ToVersion()
}

func TestRawStoreGetPut(t *testing.T) {
	db := setupPostgres(t)
	ctx := context.Background()
	store := snapshotpostgres.NewStore(snapshotpostgres.DefaultStoreConfig())

	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer tx.Rollback(ctx)

	id := uuid.New().String()

	// 1. Get non-existent snapshot
	snap, err := store.Get(ctx, tx, "User", id)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if snap.StreamVersion != 0 {
		t.Errorf("expected version 0 for missing snapshot, got %d", snap.StreamVersion)
	}

	// 2. Put snapshot
	inputSnap := snapshot.Snapshot{
		StreamType:    "User",
		StreamID:      id,
		StreamVersion: 5,
		SchemaVersion: 1,
		Payload:       []byte(`{"id":"` + id + `","name":"Alice"}`),
	}
	err = store.Put(ctx, tx, &inputSnap)
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// 3. Get snapshot and verify
	snap, err = store.Get(ctx, tx, "User", id)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if snap.StreamVersion != 5 {
		t.Errorf("expected version 5, got %d", snap.StreamVersion)
	}
	if snap.SchemaVersion != 1 {
		t.Errorf("expected schema version 1, got %d", snap.SchemaVersion)
	}
	if string(snap.Payload) != string(inputSnap.Payload) {
		t.Errorf("payload mismatch: got %s, expected %s", snap.Payload, inputSnap.Payload)
	}

	// 4. Overwrite/Update snapshot
	updatedSnap := inputSnap
	updatedSnap.StreamVersion = 10
	updatedSnap.Payload = []byte(`{"id":"` + id + `","name":"Alice Smith"}`)
	err = store.Put(ctx, tx, &updatedSnap)
	if err != nil {
		t.Fatalf("Put updated failed: %v", err)
	}

	// 5. Get snapshot and verify update
	snap, err = store.Get(ctx, tx, "User", id)
	if err != nil {
		t.Fatalf("Get updated failed: %v", err)
	}
	if snap.StreamVersion != 10 {
		t.Errorf("expected updated version 10, got %d", snap.StreamVersion)
	}
	if string(snap.Payload) != string(updatedSnap.Payload) {
		t.Errorf("payload mismatch: got %s, expected %s", snap.Payload, updatedSnap.Payload)
	}
}

func TestRawStoreMonotonicVersionGuard(t *testing.T) {
	db := setupPostgres(t)
	ctx := context.Background()
	store := snapshotpostgres.NewStore(snapshotpostgres.DefaultStoreConfig())

	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer tx.Rollback(ctx)

	id := uuid.New().String()

	// 1. Put initial snapshot at v30
	snap30 := snapshot.Snapshot{
		StreamType:    "User",
		StreamID:      id,
		StreamVersion: 30,
		SchemaVersion: 1,
		Payload:       []byte(`{"name":"Alice","version":30}`),
	}
	if err := store.Put(ctx, tx, &snap30); err != nil {
		t.Fatalf("Put v30 failed: %v", err)
	}

	// Verify it was stored at v30
	snap, err := store.Get(ctx, tx, "User", id)
	if err != nil {
		t.Fatalf("Get v30 failed: %v", err)
	}
	if snap.StreamVersion != 30 {
		t.Fatalf("expected version 30, got %d", snap.StreamVersion)
	}

	// 2. Attempt to put an older snapshot at v10 (version regression)
	snap10 := snapshot.Snapshot{
		StreamType:    "User",
		StreamID:      id,
		StreamVersion: 10,
		SchemaVersion: 1,
		Payload:       []byte(`{"name":"Alice","version":10}`),
	}
	if err := store.Put(ctx, tx, &snap10); err != nil {
		t.Fatalf("Put v10 should succeed as a no-op, got error: %v", err)
	}

	// Verify the row did NOT regress to v10
	snap, err = store.Get(ctx, tx, "User", id)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if snap.StreamVersion != 30 {
		t.Errorf("version regressed! expected 30, got %d", snap.StreamVersion)
	}
	if string(snap.Payload) != string(snap30.Payload) {
		t.Errorf("payload regressed! expected %s, got %s", snap30.Payload, snap.Payload)
	}

	// 3. Put snapshot at v31 (advancing version)
	snap31 := snapshot.Snapshot{
		StreamType:    "User",
		StreamID:      id,
		StreamVersion: 31,
		SchemaVersion: 1,
		Payload:       []byte(`{"name":"Alice","version":31}`),
	}
	if err := store.Put(ctx, tx, &snap31); err != nil {
		t.Fatalf("Put v31 failed: %v", err)
	}

	snap, err = store.Get(ctx, tx, "User", id)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if snap.StreamVersion != 31 {
		t.Errorf("expected version 31, got %d", snap.StreamVersion)
	}
}

func TestRepositoryRehydration(t *testing.T) {
	db := setupPostgres(t)
	ctx := context.Background()

	es := storepostgres.NewStore(storepostgres.DefaultStoreConfig())
	ss := snapshotpostgres.NewStore(snapshotpostgres.DefaultStoreConfig())

	applyCallCount := 0
	config := snapshot.RepositoryConfig[*TestUser]{
		StreamType:    "User",
		SchemaVersion: 1,
		Initializer: func(id string) *TestUser {
			return &TestUser{ID: id}
		},
		Apply: func(u *TestUser, e store.PersistedEvent) (*TestUser, error) {
			applyCallCount++
			var payload TestEvent
			if err := json.Unmarshal(e.Payload, &payload); err != nil {
				return nil, err
			}
			if payload.Name != "" {
				u.Name = payload.Name
			}
			if payload.Email != "" {
				u.Email = payload.Email
			}
			return u, nil
		},
		Marshal: func(u *TestUser) ([]byte, error) {
			return json.Marshal(u)
		},
		Unmarshal: func(data []byte) (*TestUser, error) {
			var u TestUser
			if err := json.Unmarshal(data, &u); err != nil {
				return nil, err
			}
			return &u, nil
		},
	}

	repo, err := snapshot.NewRepository(es, ss, config)
	if err != nil {
		t.Fatalf("create repository: %v", err)
	}

	id := uuid.New().String()

	// --- Case A: Load with zero history ---
	tx, _ := db.Begin(ctx)
	res, err := repo.Load(ctx, tx, id)
	_ = tx.Rollback(ctx)
	if err != nil {
		t.Fatalf("Load A failed: %v", err)
	}
	if res.StreamVersion != 0 {
		t.Errorf("expected version 0, got %d", res.StreamVersion)
	}
	if res.State.ID != id {
		t.Errorf("expected ID %s, got %s", id, res.State.ID)
	}
	if applyCallCount != 0 {
		t.Errorf("expected 0 apply calls, got %d", applyCallCount)
	}

	// --- Case B: Load with events but no snapshot ---
	tx, _ = db.Begin(ctx)
	appendTestEvent(t, ctx, tx, es, id, "UserCreated", "Bob", "bob@example.com", store.NoStream())
	appendTestEvent(t, ctx, tx, es, id, "UserUpdated", "Bob S.", "", store.Exact(1))
	_ = tx.Commit(ctx)

	tx, _ = db.Begin(ctx)
	applyCallCount = 0
	res, err = repo.Load(ctx, tx, id)
	_ = tx.Rollback(ctx)
	if err != nil {
		t.Fatalf("Load B failed: %v", err)
	}
	if res.StreamVersion != 2 {
		t.Errorf("expected version 2, got %d", res.StreamVersion)
	}
	if res.State.Name != "Bob S." || res.State.Email != "bob@example.com" {
		t.Errorf("incorrect state rehydrated: %+v", res.State)
	}
	if applyCallCount != 2 {
		t.Errorf("expected 2 apply calls, got %d", applyCallCount)
	}

	// --- Case C: Save snapshot and load ---
	tx, _ = db.Begin(ctx)
	err = repo.Save(ctx, tx, id, res.StreamVersion, res.State)
	if err != nil {
		t.Fatalf("Save snapshot failed: %v", err)
	}
	_ = tx.Commit(ctx)

	// Append two more events (v3 and v4)
	tx, _ = db.Begin(ctx)
	appendTestEvent(t, ctx, tx, es, id, "UserUpdated", "Bobby", "", store.Exact(2))
	appendTestEvent(t, ctx, tx, es, id, "UserUpdated", "", "bobby@example.com", store.Exact(3))
	_ = tx.Commit(ctx)

	// Load now: should read snapshot (v2) and only apply v3 and v4 (2 apply calls)
	tx, _ = db.Begin(ctx)
	applyCallCount = 0
	res, err = repo.Load(ctx, tx, id)
	_ = tx.Rollback(ctx)
	if err != nil {
		t.Fatalf("Load C failed: %v", err)
	}
	if res.StreamVersion != 4 {
		t.Errorf("expected version 4, got %d", res.StreamVersion)
	}
	if res.State.Name != "Bobby" || res.State.Email != "bobby@example.com" {
		t.Errorf("incorrect state rehydrated: %+v", res.State)
	}
	if applyCallCount != 2 {
		t.Errorf("expected 2 apply calls (delta only), got %d", applyCallCount)
	}

	// --- Case D: Schema mismatch fallback ---
	// Create repo with SchemaVersion 2
	config.SchemaVersion = 2
	repoV2, err := snapshot.NewRepository(es, ss, config)
	if err != nil {
		t.Fatalf("create repository V2: %v", err)
	}

	// Load: snapshot is version 1, repo is version 2. Snapshot must be ignored.
	// It should replay all 4 events from version 1 (4 apply calls)
	tx, _ = db.Begin(ctx)
	applyCallCount = 0
	res, err = repoV2.Load(ctx, tx, id)
	_ = tx.Rollback(ctx)
	if err != nil {
		t.Fatalf("Load D failed: %v", err)
	}
	if res.StreamVersion != 4 {
		t.Errorf("expected version 4, got %d", res.StreamVersion)
	}
	if applyCallCount != 4 {
		t.Errorf("expected 4 apply calls (full replay due to schema mismatch), got %d", applyCallCount)
	}
}

func TestRepositoryErrorBoundaries(t *testing.T) {
	db := setupPostgres(t)
	ctx := context.Background()

	es := storepostgres.NewStore(storepostgres.DefaultStoreConfig())
	ss := snapshotpostgres.NewStore(snapshotpostgres.DefaultStoreConfig())

	config := snapshot.RepositoryConfig[*TestUser]{
		StreamType:    "User",
		SchemaVersion: 1,
		Initializer: func(id string) *TestUser {
			return &TestUser{ID: id}
		},
		Apply: func(u *TestUser, e store.PersistedEvent) (*TestUser, error) {
			var payload TestEvent
			if err := json.Unmarshal(e.Payload, &payload); err != nil {
				return nil, err
			}
			if payload.Name != "" {
				u.Name = payload.Name
			}
			if payload.Email != "" {
				u.Email = payload.Email
			}
			return u, nil
		},
		Marshal: func(u *TestUser) ([]byte, error) {
			return json.Marshal(u)
		},
		Unmarshal: func(data []byte) (*TestUser, error) {
			var u TestUser
			if err := json.Unmarshal(data, &u); err != nil {
				return nil, err
			}
			return &u, nil
		},
	}

	id := uuid.New().String()

	// 1. Validation errors in NewRepository
	t.Run("nil reader", func(t *testing.T) {
		_, err := snapshot.NewRepository(nil, ss, config)
		if err == nil {
			t.Error("expected error for nil reader")
		}
	})
	t.Run("nil snapshot store", func(t *testing.T) {
		_, err := snapshot.NewRepository(es, nil, config)
		if err == nil {
			t.Error("expected error for nil snapshot store")
		}
	})
	t.Run("empty stream type", func(t *testing.T) {
		cfg := config
		cfg.StreamType = ""
		_, err := snapshot.NewRepository(es, ss, cfg)
		if err == nil {
			t.Error("expected error for empty StreamType")
		}
	})
	t.Run("nil initializer", func(t *testing.T) {
		cfg := config
		cfg.Initializer = nil
		_, err := snapshot.NewRepository(es, ss, cfg)
		if err == nil {
			t.Error("expected error for nil Initializer")
		}
	})

	repo, err := snapshot.NewRepository(es, ss, config)
	if err != nil {
		t.Fatalf("create repository: %v", err)
	}

	// 2. Save with invalid version
	tx, _ := db.Begin(ctx)
	defer tx.Rollback(ctx)

	err = repo.Save(ctx, tx, id, 0, &TestUser{ID: id})
	if err == nil {
		t.Error("expected error when saving snapshot with version 0")
	}

	err = repo.Save(ctx, tx, id, -5, &TestUser{ID: id})
	if err == nil {
		t.Error("expected error when saving snapshot with negative version")
	}

	// 3. Unmarshal failure
	// We put corrupt payload manually
	corruptSnap := snapshot.Snapshot{
		StreamType:    "User",
		StreamID:      id,
		StreamVersion: 3,
		SchemaVersion: 1,
		Payload:       []byte(`invalid-json-payload{}`),
	}
	if err := ss.Put(ctx, tx, &corruptSnap); err != nil {
		t.Fatalf("failed to write corrupt snapshot: %v", err)
	}

	// In default resilient mode, Load discards corrupt snapshot and falls back to full replay
	res, err := repo.Load(ctx, tx, id)
	if err != nil {
		t.Errorf("expected resilient load fallback, got error: %v", err)
	}
	if res.StreamVersion != 0 {
		t.Errorf("expected version 0 (empty stream), got %d", res.StreamVersion)
	}
	if res.State.ID != id {
		t.Errorf("expected user ID %q, got %q", id, res.State.ID)
	}

	// In strict mode (FailOnCorruptSnapshot: true), Load returns the unmarshal error
	strictConfig := config
	strictConfig.FailOnCorruptSnapshot = true
	strictRepo, err := snapshot.NewRepository(es, ss, strictConfig)
	if err != nil {
		t.Fatalf("create strict repository: %v", err)
	}
	_, err = strictRepo.Load(ctx, tx, id)
	if err == nil {
		t.Error("expected load error in strict mode due to corrupt JSON unmarshal")
	}

	// 4. Save rejects snapshot when version does not exist in stream log
	err = repo.Save(ctx, tx, id, 100, &TestUser{ID: id, Name: "NonExistent"})
	if err == nil {
		t.Fatal("expected error saving at non-existent stream version, got nil")
	}

	// 5. Snapshot ahead of stream log falls back to full stream replay
	aheadID := uuid.New().String()
	// Append 2 events
	appendTestEvent(t, ctx, tx, es, aheadID, "UserCreated", "RealAlice", "alice@example.com", store.NoStream())
	appendTestEvent(t, ctx, tx, es, aheadID, "UserUpdated", "RealAlice Updated", "", store.Exact(1))

	// Manually inject a rogue snapshot claiming version 5000
	rogueSnap := snapshot.Snapshot{
		StreamType:    "User",
		StreamID:      aheadID,
		StreamVersion: 5000,
		SchemaVersion: 1,
		Payload:       []byte(`{"id":"` + aheadID + `","name":"PhantomAlice"}`),
	}
	if err := ss.Put(ctx, tx, &rogueSnap); err != nil {
		t.Fatalf("failed to insert rogue snapshot: %v", err)
	}

	resAhead, err := repo.Load(ctx, tx, aheadID)
	if err != nil {
		t.Fatalf("Load with ahead snapshot failed: %v", err)
	}
	if resAhead.SnapshotHit {
		t.Errorf("expected SnapshotHit to be false for snapshot ahead of stream")
	}
	if resAhead.StreamVersion != 2 {
		t.Errorf("expected StreamVersion 2, got %d", resAhead.StreamVersion)
	}
	if resAhead.EventsReplayed != 2 {
		t.Errorf("expected EventsReplayed 2, got %d", resAhead.EventsReplayed)
	}
	if resAhead.State.Name != "RealAlice Updated" {
		t.Errorf("expected state to be replayed from events ('RealAlice Updated'), got %q", resAhead.State.Name)
	}

	// 6. Orphan snapshot on empty stream falls back to initial state
	orphanID := uuid.New().String()
	orphanSnap := snapshot.Snapshot{
		StreamType:    "User",
		StreamID:      orphanID,
		StreamVersion: 10,
		SchemaVersion: 1,
		Payload:       []byte(`{"id":"` + orphanID + `","name":"GhostAlice"}`),
	}
	if err := ss.Put(ctx, tx, &orphanSnap); err != nil {
		t.Fatalf("failed to insert orphan snapshot: %v", err)
	}

	resOrphan, err := repo.Load(ctx, tx, orphanID)
	if err != nil {
		t.Fatalf("Load with orphan snapshot failed: %v", err)
	}
	if resOrphan.SnapshotHit {
		t.Errorf("expected SnapshotHit to be false for orphan snapshot")
	}
	if resOrphan.StreamVersion != 0 {
		t.Errorf("expected StreamVersion 0, got %d", resOrphan.StreamVersion)
	}
	if resOrphan.EventsReplayed != 0 {
		t.Errorf("expected EventsReplayed 0, got %d", resOrphan.EventsReplayed)
	}
	if resOrphan.State.Name != "" {
		t.Errorf("expected empty name for initial state, got %q", resOrphan.State.Name)
	}
}
