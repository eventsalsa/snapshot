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
		schema_version INT NOT NULL,
		stream_version BIGINT NOT NULL,
		payload BYTEA NOT NULL,
		created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
		PRIMARY KEY (stream_type, stream_id, schema_version)
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
	snap, err := store.Get(ctx, tx, "User", id, 1)
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
	snap, err = store.Get(ctx, tx, "User", id, 1)
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
	snap, err = store.Get(ctx, tx, "User", id, 1)
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
	snap, err := store.Get(ctx, tx, "User", id, 1)
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
	snap, err = store.Get(ctx, tx, "User", id, 1)
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

	snap, err = store.Get(ctx, tx, "User", id, 1)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if snap.StreamVersion != 31 {
		t.Errorf("expected version 31, got %d", snap.StreamVersion)
	}
}

func TestRawStoreMultiVersionCoexistence(t *testing.T) {
	db := setupPostgres(t)
	ctx := context.Background()
	store := snapshotpostgres.NewStore(snapshotpostgres.DefaultStoreConfig())

	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer tx.Rollback(ctx)

	id := uuid.New().String()

	// 1. Put schema_version 1 at stream_version 10
	snapV1 := snapshot.Snapshot{
		StreamType:    "User",
		StreamID:      id,
		StreamVersion: 10,
		SchemaVersion: 1,
		Payload:       []byte(`{"name":"Alice","schema":1}`),
	}
	if err := store.Put(ctx, tx, &snapV1); err != nil {
		t.Fatalf("Put schema 1 failed: %v", err)
	}

	// 2. Put schema_version 2 at stream_version 12 (different schema version for same stream)
	snapV2 := snapshot.Snapshot{
		StreamType:    "User",
		StreamID:      id,
		StreamVersion: 12,
		SchemaVersion: 2,
		Payload:       []byte(`{"full_name":"Alice Wonderland","schema":2}`),
	}
	if err := store.Put(ctx, tx, &snapV2); err != nil {
		t.Fatalf("Put schema 2 failed: %v", err)
	}

	// 3. Verify both rows coexist in PostgreSQL
	gotV1, err := store.Get(ctx, tx, "User", id, 1)
	if err != nil {
		t.Fatalf("Get schema 1 failed: %v", err)
	}
	if gotV1.SchemaVersion != 1 || gotV1.StreamVersion != 10 {
		t.Errorf("expected schema 1 at stream version 10, got schema %d at version %d", gotV1.SchemaVersion, gotV1.StreamVersion)
	}
	if string(gotV1.Payload) != string(snapV1.Payload) {
		t.Errorf("schema 1 payload mismatch: got %s", gotV1.Payload)
	}

	gotV2, err := store.Get(ctx, tx, "User", id, 2)
	if err != nil {
		t.Fatalf("Get schema 2 failed: %v", err)
	}
	if gotV2.SchemaVersion != 2 || gotV2.StreamVersion != 12 {
		t.Errorf("expected schema 2 at stream version 12, got schema %d at version %d", gotV2.SchemaVersion, gotV2.StreamVersion)
	}

	// Query with maxSchemaVersion <= 0 rejects with error
	_, err = store.Get(ctx, tx, "User", id, 0)
	if err == nil {
		t.Fatal("expected error when maxSchemaVersion <= 0, got nil")
	}

	// 4. Advance schema 1 to stream_version 15 without altering schema 2
	snapV1Adv := snapshot.Snapshot{
		StreamType:    "User",
		StreamID:      id,
		StreamVersion: 15,
		SchemaVersion: 1,
		Payload:       []byte(`{"name":"Alice Updated","schema":1}`),
	}
	if err := store.Put(ctx, tx, &snapV1Adv); err != nil {
		t.Fatalf("Put advanced schema 1 failed: %v", err)
	}

	gotV1Adv, err := store.Get(ctx, tx, "User", id, 1)
	if err != nil {
		t.Fatalf("Get advanced schema 1 failed: %v", err)
	}
	if gotV1Adv.StreamVersion != 15 {
		t.Errorf("expected schema 1 at stream version 15, got %d", gotV1Adv.StreamVersion)
	}

	// Verify schema 2 is completely unchanged
	gotV2Check, err := store.Get(ctx, tx, "User", id, 2)
	if err != nil {
		t.Fatalf("Get schema 2 check failed: %v", err)
	}
	if gotV2Check.StreamVersion != 12 {
		t.Errorf("schema 2 was modified unexpectedly! expected stream version 12, got %d", gotV2Check.StreamVersion)
	}

	// 5. Monotonic check is scoped per (stream_type, stream_id, schema_version)
	// A lower stream_version 14 for schema 1 should not overwrite stream_version 15
	snapV1Old := snapshot.Snapshot{
		StreamType:    "User",
		StreamID:      id,
		StreamVersion: 14,
		SchemaVersion: 1,
		Payload:       []byte(`{"name":"Alice Stale","schema":1}`),
	}
	if err := store.Put(ctx, tx, &snapV1Old); err != nil {
		t.Fatalf("Put stale schema 1 failed: %v", err)
	}

	gotV1AfterStale, err := store.Get(ctx, tx, "User", id, 1)
	if err != nil {
		t.Fatalf("Get schema 1 after stale failed: %v", err)
	}
	if gotV1AfterStale.StreamVersion != 15 {
		t.Errorf("expected monotonic protection: stream version remained 15, got %d", gotV1AfterStale.StreamVersion)
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
		Unmarshal: func(streamID string, data []byte) (*TestUser, error) {
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
		Unmarshal: func(streamID string, data []byte) (*TestUser, error) {
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

type TestUserV2 struct {
	ID       string `json:"id"`
	FullName string `json:"full_name"`
	Email    string `json:"email"`
}

func TestRepositoryMultiVersionRollingDeploymentAndUpcasting(t *testing.T) {
	db := setupPostgres(t)
	ctx := context.Background()

	es := storepostgres.NewStore(storepostgres.DefaultStoreConfig())
	ss := snapshotpostgres.NewStore(snapshotpostgres.DefaultStoreConfig())

	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer tx.Rollback(ctx)

	id := uuid.New().String()

	// V1 configuration
	cfgV1 := snapshot.RepositoryConfig[*TestUser]{
		StreamType:    "User",
		SchemaVersion: 1,
		Initializer: func(id string) *TestUser {
			return &TestUser{ID: id}
		},
		Apply: func(state *TestUser, event store.PersistedEvent) (*TestUser, error) {
			var ev TestEvent
			if err := json.Unmarshal(event.Payload, &ev); err != nil {
				return nil, err
			}
			if ev.Name != "" {
				state.Name = ev.Name
			}
			if ev.Email != "" {
				state.Email = ev.Email
			}
			return state, nil
		},
		Marshal: func(state *TestUser) ([]byte, error) {
			return json.Marshal(state)
		},
		Unmarshal: func(streamID string, data []byte) (*TestUser, error) {
			var state TestUser
			if err := json.Unmarshal(data, &state); err != nil {
				return nil, err
			}
			return &state, nil
		},
	}

	// V2 configuration with Upcaster from Schema 1 -> Schema 2
	cfgV2 := snapshot.RepositoryConfig[*TestUserV2]{
		StreamType:    "User",
		SchemaVersion: 2,
		Initializer: func(id string) *TestUserV2 {
			return &TestUserV2{ID: id}
		},
		Apply: func(state *TestUserV2, event store.PersistedEvent) (*TestUserV2, error) {
			var ev TestEvent
			if err := json.Unmarshal(event.Payload, &ev); err != nil {
				return nil, err
			}
			if ev.Name != "" {
				state.FullName = ev.Name
			}
			if ev.Email != "" {
				state.Email = ev.Email
			}
			return state, nil
		},
		Marshal: func(state *TestUserV2) ([]byte, error) {
			return json.Marshal(state)
		},
		Unmarshal: func(streamID string, data []byte) (*TestUserV2, error) {
			var state TestUserV2
			if err := json.Unmarshal(data, &state); err != nil {
				return nil, err
			}
			return &state, nil
		},
		Upcasters: map[int]snapshot.Upcaster{
			1: func(fromVer int, payload []byte) ([]byte, error) {
				var v1 TestUser
				if err := json.Unmarshal(payload, &v1); err != nil {
					return nil, err
				}
				v2 := TestUserV2{
					ID:       v1.ID,
					FullName: v1.Name + " (Upcasted)",
					Email:    v1.Email,
				}
				return json.Marshal(v2)
			},
		},
	}

	repoV1, err := snapshot.NewRepository(es, ss, cfgV1)
	if err != nil {
		t.Fatalf("NewRepository v1: %v", err)
	}
	repoV2, err := snapshot.NewRepository(es, ss, cfgV2)
	if err != nil {
		t.Fatalf("NewRepository v2: %v", err)
	}

	// 1. Create stream with 3 events
	appendTestEvent(t, ctx, tx, es, id, "UserCreated", "Bob", "bob@example.com", store.NoStream())
	appendTestEvent(t, ctx, tx, es, id, "UserUpdated", "Bob Smith", "", store.Exact(1))
	appendTestEvent(t, ctx, tx, es, id, "UserUpdated", "Bob Smith Jr.", "", store.Exact(2))

	// 2. V1 pod loads stream and saves a v1 snapshot at stream_version 3
	loadV1, err := repoV1.Load(ctx, tx, id)
	if err != nil {
		t.Fatalf("V1 Load failed: %v", err)
	}
	if loadV1.StreamVersion != 3 || loadV1.State.Name != "Bob Smith Jr." {
		t.Fatalf("unexpected V1 state: version=%d, name=%s", loadV1.StreamVersion, loadV1.State.Name)
	}
	if err := repoV1.Save(ctx, tx, id, loadV1.StreamVersion, loadV1.State); err != nil {
		t.Fatalf("V1 Save failed: %v", err)
	}

	// 3. Append 2 delta events (v4 and v5)
	appendTestEvent(t, ctx, tx, es, id, "UserUpdated", "", "bob.jr@example.com", store.Exact(3))
	appendTestEvent(t, ctx, tx, es, id, "UserUpdated", "Robert Smith Jr.", "", store.Exact(4))

	// 4. V2 pod loads stream: should load v1 snapshot, upcast it, and replay delta events 4 and 5
	loadV2, err := repoV2.Load(ctx, tx, id)
	if err != nil {
		t.Fatalf("V2 Load failed: %v", err)
	}
	if !loadV2.SnapshotHit {
		t.Error("expected V2 SnapshotHit = true")
	}
	if !loadV2.Upcasted {
		t.Error("expected V2 Upcasted = true")
	}
	if loadV2.SnapshotSchemaVersion != 1 {
		t.Errorf("expected SnapshotSchemaVersion = 1, got %d", loadV2.SnapshotSchemaVersion)
	}
	if loadV2.SnapshotVersion != 3 {
		t.Errorf("expected SnapshotVersion = 3, got %d", loadV2.SnapshotVersion)
	}
	if loadV2.StreamVersion != 5 {
		t.Errorf("expected StreamVersion = 5, got %d", loadV2.StreamVersion)
	}
	if loadV2.EventsReplayed != 2 {
		t.Errorf("expected EventsReplayed = 2, got %d", loadV2.EventsReplayed)
	}
	if loadV2.State.FullName != "Robert Smith Jr." {
		t.Errorf("expected FullName 'Robert Smith Jr.', got %q", loadV2.State.FullName)
	}
	if loadV2.State.Email != "bob.jr@example.com" {
		t.Errorf("expected Email 'bob.jr@example.com', got %q", loadV2.State.Email)
	}

	// 5. V2 pod saves snapshot at version 5 (schema_version 2)
	if err := repoV2.Save(ctx, tx, id, loadV2.StreamVersion, loadV2.State); err != nil {
		t.Fatalf("V2 Save failed: %v", err)
	}

	// 6. Verify Rolling Deployment Coexistence:
	// A surviving V1 pod can still load the stream without seeing or crashing on V2 snapshots!
	loadV1Survivor, err := repoV1.Load(ctx, tx, id)
	if err != nil {
		t.Fatalf("V1 survivor Load failed: %v", err)
	}
	if !loadV1Survivor.SnapshotHit {
		t.Error("expected V1 survivor SnapshotHit = true")
	}
	if loadV1Survivor.Upcasted {
		t.Error("expected V1 survivor Upcasted = false")
	}
	if loadV1Survivor.SnapshotSchemaVersion != 1 {
		t.Errorf("expected V1 SnapshotSchemaVersion = 1, got %d", loadV1Survivor.SnapshotSchemaVersion)
	}
	if loadV1Survivor.SnapshotVersion != 3 {
		t.Errorf("expected V1 SnapshotVersion = 3, got %d", loadV1Survivor.SnapshotVersion)
	}
	if loadV1Survivor.StreamVersion != 5 {
		t.Errorf("expected V1 StreamVersion = 5, got %d", loadV1Survivor.StreamVersion)
	}
	if loadV1Survivor.EventsReplayed != 2 {
		t.Errorf("expected V1 EventsReplayed = 2, got %d", loadV1Survivor.EventsReplayed)
	}
	if loadV1Survivor.State.Name != "Robert Smith Jr." {
		t.Errorf("expected V1 state name 'Robert Smith Jr.', got %q", loadV1Survivor.State.Name)
	}

	// And a V2 pod loads with a direct hit on the V2 snapshot (no upcasting, 0 delta events)
	loadV2Direct, err := repoV2.Load(ctx, tx, id)
	if err != nil {
		t.Fatalf("V2 direct Load failed: %v", err)
	}
	if !loadV2Direct.SnapshotHit {
		t.Error("expected V2 direct SnapshotHit = true")
	}
	if loadV2Direct.Upcasted {
		t.Error("expected V2 direct Upcasted = false")
	}
	if loadV2Direct.SnapshotSchemaVersion != 2 {
		t.Errorf("expected V2 direct SnapshotSchemaVersion = 2, got %d", loadV2Direct.SnapshotSchemaVersion)
	}
	if loadV2Direct.SnapshotVersion != 5 {
		t.Errorf("expected V2 direct SnapshotVersion = 5, got %d", loadV2Direct.SnapshotVersion)
	}
	if loadV2Direct.EventsReplayed != 0 {
		t.Errorf("expected V2 direct EventsReplayed = 0, got %d", loadV2Direct.EventsReplayed)
	}
}
