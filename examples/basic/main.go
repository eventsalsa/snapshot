package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/eventsalsa/store"
	storepostgres "github.com/eventsalsa/store/postgres"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/eventsalsa/snapshot"
	snapshotpostgres "github.com/eventsalsa/snapshot/postgres"
)

// User is our clean domain aggregate.
// It has no eventsalsa dependencies or embedded types.
type User struct {
	ID    string `json:"id"`
	Name  string `json:"name"`
	Email string `json:"email"`
}

// UserCreated event payload.
type UserCreated struct {
	Name  string `json:"name"`
	Email string `json:"email"`
}

// UserEmailChanged event payload.
type UserEmailChanged struct {
	Email string `json:"email"`
}

// UserNameChanged event payload.
type UserNameChanged struct {
	Name string `json:"name"`
}

func main() {
	// Standard database connection pool.
	// Adjust connection string as needed for your local postgres setup.
	connStr := "host=localhost port=5432 user=postgres password=postgres dbname=eventsalsa_example sslmode=disable"
	ctx := context.Background()

	db, err := pgxpool.New(ctx, connStr)
	if err != nil {
		log.Fatalf("Failed to connect to database. Make sure PostgreSQL is running: %v", err)
	}
	defer db.Close()

	// Initialize tables for the example
	setupTables(ctx, db)

	// 1. Initialize Event Store
	eventStore := storepostgres.NewStore(storepostgres.DefaultStoreConfig())

	// 2. Initialize Snapshot Store
	snapshotStore := snapshotpostgres.NewStore(snapshotpostgres.DefaultStoreConfig())

	// 3. Define Repository Configuration
	repoConfig := snapshot.RepositoryConfig[*User]{
		AggregateType: "User",
		SchemaVersion: 1, // Current schema version of the User model

		Initializer: func(id string) *User {
			return &User{ID: id}
		},

		Apply: func(u *User, event store.PersistedEvent) (*User, error) {
			fmt.Printf("  [Apply Event] Replaying %s (v%d)\n", event.EventType, event.AggregateVersion)
			switch event.EventType {
			case "UserCreated":
				var payload UserCreated
				if err := json.Unmarshal(event.Payload, &payload); err != nil {
					return nil, err
				}
				u.Name = payload.Name
				u.Email = payload.Email
			case "UserEmailChanged":
				var payload UserEmailChanged
				if err := json.Unmarshal(event.Payload, &payload); err != nil {
					return nil, err
				}
				u.Email = payload.Email
			case "UserNameChanged":
				var payload UserNameChanged
				if err := json.Unmarshal(event.Payload, &payload); err != nil {
					return nil, err
				}
				u.Name = payload.Name
			}
			return u, nil
		},

		Marshal: func(u *User) ([]byte, error) {
			return json.Marshal(u)
		},

		Unmarshal: func(data []byte) (*User, error) {
			var u User
			if err := json.Unmarshal(data, &u); err != nil {
				return nil, err
			}
			return &u, nil
		},
	}

	repo, err := snapshot.NewRepository(eventStore, snapshotStore, repoConfig)
	if err != nil {
		log.Fatalf("Failed to create snapshot repository: %v", err)
	}

	userID := uuid.New().String()

	// --- Step 1: Load Aggregate (from empty) ---
	fmt.Println("\n--- Step 1: Loading non-existent User ---")
	tx, err := db.Begin(ctx)
	if err != nil {
		log.Fatalf("Failed to begin tx: %v", err)
	}

	u, version, err := repo.Load(ctx, tx, userID)
	if err != nil {
		log.Fatalf("Load failed: %v", err)
	}
	_ = tx.Rollback(ctx)

	fmt.Printf("Loaded state: %+v, version: %d\n", u, version)

	// --- Step 2: Save Event 1 (UserCreated) ---
	fmt.Println("\n--- Step 2: Appending UserCreated event ---")
	tx, err = db.Begin(ctx)
	if err != nil {
		log.Fatalf("Failed to begin tx: %v", err)
	}

	payload, _ := json.Marshal(UserCreated{Name: "Alice", Email: "alice@example.com"})
	events := []store.Event{
		{
			AggregateType: "User",
			AggregateID:   userID,
			EventID:       uuid.New(),
			EventType:     "UserCreated",
			Payload:       payload,
			CreatedAt:     time.Now(),
		},
	}
	result, err := eventStore.Append(ctx, tx, store.NoStream(), events)
	if err != nil {
		log.Fatalf("Append failed: %v", err)
	}
	_ = tx.Commit(ctx)
	fmt.Printf("Created User version: %d\n", result.ToVersion())

	// --- Step 3: Append multiple changes (v2 to v5) ---
	fmt.Println("\n--- Step 3: Appending more changes (v2 -> v5) ---")
	tx, err = db.Begin(ctx)
	if err != nil {
		log.Fatalf("Failed to begin tx: %v", err)
	}

	currVer := result.ToVersion()
	emails := []string{"alice.smith@example.com", "alice.s@example.com", "alice.s.work@example.com", "alice.smith.hq@example.com"}
	for _, email := range emails {
		payload, _ = json.Marshal(UserEmailChanged{Email: email})
		ev := []store.Event{
			{
				AggregateType: "User",
				AggregateID:   userID,
				EventID:       uuid.New(),
				EventType:     "UserEmailChanged",
				Payload:       payload,
				CreatedAt:     time.Now(),
			},
		}
		res, err := eventStore.Append(ctx, tx, store.Exact(currVer), ev)
		if err != nil {
			log.Fatalf("Append v%d failed: %v", currVer+1, err)
		}
		currVer = res.ToVersion()
	}
	_ = tx.Commit(ctx)
	fmt.Printf("User is now at version: %d\n", currVer)

	// --- Step 4: Rehydrate from events, then save a snapshot ---
	fmt.Println("\n--- Step 4: Rehydrating User and saving snapshot at version 5 ---")
	tx, err = db.Begin(ctx)
	if err != nil {
		log.Fatalf("Failed to begin tx: %v", err)
	}

	u, version, err = repo.Load(ctx, tx, userID)
	if err != nil {
		log.Fatalf("Load failed: %v", err)
	}

	fmt.Printf("Materialized state for snapshot: %+v (v%d)\n", u, version)

	err = repo.Save(ctx, tx, userID, version, u)
	if err != nil {
		log.Fatalf("Save snapshot failed: %v", err)
	}
	_ = tx.Commit(ctx)
	fmt.Println("Snapshot saved successfully!")

	// --- Step 5: Add more events (v6 & v7) ---
	fmt.Println("\n--- Step 5: Appending events (v6 & v7) ---")
	tx, err = db.Begin(ctx)
	if err != nil {
		log.Fatalf("Failed to begin tx: %v", err)
	}

	names := []string{"Alice S.", "Alice Smith"}
	for _, name := range names {
		payload, _ = json.Marshal(UserNameChanged{Name: name})
		ev := []store.Event{
			{
				AggregateType: "User",
				AggregateID:   userID,
				EventID:       uuid.New(),
				EventType:     "UserNameChanged",
				Payload:       payload,
				CreatedAt:     time.Now(),
			},
		}
		res, err := eventStore.Append(ctx, tx, store.Exact(version), ev)
		if err != nil {
			log.Fatalf("Append failed: %v", err)
		}
		version = res.ToVersion()
	}
	_ = tx.Commit(ctx)
	fmt.Printf("User is now at version: %d\n", version)

	// --- Step 6: Load aggregate (should hit snapshot v5 and only replay v6 & v7) ---
	fmt.Println("\n--- Step 6: Loading User (should use snapshot v5 and only replay v6 & v7) ---")
	tx, err = db.Begin(ctx)
	if err != nil {
		log.Fatalf("Failed to begin tx: %v", err)
	}

	u, version, err = repo.Load(ctx, tx, userID)
	if err != nil {
		log.Fatalf("Load failed: %v", err)
	}
	_ = tx.Rollback(ctx)

	fmt.Printf("Rehydrated state: %+v, final version: %d\n", u, version)

	// --- Step 7: Simulate Schema Migration (SchemaVersion bumped to 2) ---
	fmt.Println("\n--- Step 7: Simulating Schema Version Mismatch (bumping SchemaVersion to 2) ---")
	// We create a new repository with SchemaVersion 2
	repoConfig.SchemaVersion = 2
	repo2, _ := snapshot.NewRepository(eventStore, snapshotStore, repoConfig)

	tx, err = db.Begin(ctx)
	if err != nil {
		log.Fatalf("Failed to begin tx: %v", err)
	}

	// This load should detect that the stored snapshot has SchemaVersion 1,
	// discard it, and replay all events (v1 -> v7) from scratch.
	u, version, err = repo2.Load(ctx, tx, userID)
	if err != nil {
		log.Fatalf("Load with schema v2 failed: %v", err)
	}
	_ = tx.Rollback(ctx)

	fmt.Printf("Rehydrated state (all replayed): %+v, final version: %d\n", u, version)
}

func setupTables(ctx context.Context, db *pgxpool.Pool) {
	// DDL schema for events and aggregate_heads (from store migrations)
	eventStoreSchema := `
	CREATE TABLE IF NOT EXISTS events (
		global_position BIGSERIAL PRIMARY KEY,
		aggregate_type TEXT NOT NULL,
		aggregate_id TEXT NOT NULL,
		aggregate_version BIGINT NOT NULL,
		event_id UUID NOT NULL UNIQUE,
		event_type TEXT NOT NULL,
		event_version INT NOT NULL DEFAULT 1,
		payload BYTEA NOT NULL,
		trace_id TEXT,
		correlation_id TEXT,
		causation_id TEXT,
		metadata JSONB,
		created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
		UNIQUE (aggregate_type, aggregate_id, aggregate_version)
	);
	CREATE TABLE IF NOT EXISTS aggregate_heads (
		aggregate_type TEXT NOT NULL,
		aggregate_id TEXT NOT NULL,
		aggregate_version BIGINT NOT NULL,
		updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
		PRIMARY KEY (aggregate_type, aggregate_id)
	);
	`

	// DDL schema for aggregate_snapshots
	snapshotSchema := `
	CREATE TABLE IF NOT EXISTS aggregate_snapshots (
		aggregate_type TEXT NOT NULL,
		aggregate_id TEXT NOT NULL,
		aggregate_version BIGINT NOT NULL,
		schema_version INT NOT NULL,
		payload BYTEA NOT NULL,
		created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
		PRIMARY KEY (aggregate_type, aggregate_id)
	);
	`

	fmt.Println("Setting up Postgres schemas if they do not exist...")

	tx, err := db.Begin(ctx)
	if err != nil {
		log.Fatalf("Failed to begin setup tx: %v", err)
	}
	defer tx.Rollback(ctx)

	_, err = tx.Exec(ctx, eventStoreSchema)
	if err != nil {
		log.Fatalf("Failed to create event store tables: %v", err)
	}

	_, err = tx.Exec(ctx, snapshotSchema)
	if err != nil {
		log.Fatalf("Failed to create snapshot tables: %v", err)
	}

	// Clean up any old data for this example run
	_, _ = tx.Exec(ctx, "TRUNCATE TABLE events CASCADE;")
	_, _ = tx.Exec(ctx, "TRUNCATE TABLE aggregate_heads CASCADE;")
	_, _ = tx.Exec(ctx, "TRUNCATE TABLE aggregate_snapshots CASCADE;")

	err = tx.Commit(ctx)
	if err != nil {
		log.Fatalf("Failed to commit setup: %v", err)
	}
}
