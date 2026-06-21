# eventsalsa/snapshot

[![CI](https://github.com/eventsalsa/snapshot/actions/workflows/ci.yml/badge.svg)](https://github.com/eventsalsa/snapshot/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/eventsalsa/snapshot.svg)](https://pkg.go.dev/github.com/eventsalsa/snapshot)

`github.com/eventsalsa/snapshot` is a generic, PostgreSQL-backed aggregate state snapshotting module for Go event-sourced applications. 

It is designed to be used alongside [`github.com/eventsalsa/store`](https://github.com/eventsalsa/store). By saving a snapshot of the aggregate state at a specific version, you avoid loading the full stream of events from version 1. Instead, the snapshot repository loads the latest snapshot, queries only subsequent events (the delta), and replays them to reconstruct the active state.

## Features

- **Generic & Type-Safe**: Uses Go generics (`[T any]`) to define repositories, avoiding any coupling or structural inheritance inside your domain models.
- **Pgx Transactional Integrity**: All operations run within the caller-provided `pgx.Tx` transaction, matching the design of `eventsalsa/store`.
- **Automatic Schema Evolution**: Includes a `schema_version` column. If the snapshot stored in the database has a schema version that mismatches the code's expected version, the library automatically discards it and falls back to a full replay of events, ensuring structural safety.
- **Low-Level and High-Level APIs**: Exposes a raw byte `Store` interface alongside a high-level orchestrated `Repository[T]`.
- **Migration Generation**: Comes with a built-in SQL migration generator and CLI tool to generate PostgreSQL tables.

---

## Installation

```bash
go get github.com/eventsalsa/snapshot
```

---

## Quick Start

### 1. Generate & Apply DB Migration

Use the CLI tool to generate the migration file containing the DDL for the `aggregate_snapshots` table:

```bash
go run github.com/eventsalsa/snapshot/cmd/migrate-gen -output migrations
```

This generates a file named like `migrations/<timestamp>_init_snapshots.sql`:

```sql
CREATE TABLE IF NOT EXISTS aggregate_snapshots (
    aggregate_type TEXT NOT NULL,
    aggregate_id TEXT NOT NULL,
    aggregate_version BIGINT NOT NULL,
    schema_version INT NOT NULL,
    payload BYTEA NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    PRIMARY KEY (aggregate_type, aggregate_id)
);

CREATE INDEX IF NOT EXISTS idx_aggregate_snapshots_schema_version 
    ON aggregate_snapshots (schema_version);
```

Apply this migration using your preferred PostgreSQL migration runner.

### 2. Define a Domain Model (Clean)

Your aggregate structs remain completely clean of library code:

```go
type User struct {
	ID    string
	Name  string
	Email string
}
```

### 3. Set Up the Repository

Initialize the event store, snapshot store, and the generic snapshot repository:

```go
import (
	"context"
	"encoding/json"
	
	"github.com/eventsalsa/snapshot"
	snapshotpostgres "github.com/eventsalsa/snapshot/postgres"
	"github.com/eventsalsa/store"
	storepostgres "github.com/eventsalsa/store/postgres"
)

// 1. Create the stores (normally singletons in your app)
eventStore := storepostgres.NewStore(storepostgres.DefaultStoreConfig())
snapshotStore := snapshotpostgres.NewStore(snapshotpostgres.DefaultStoreConfig())

// 2. Define repository configuration
config := snapshot.RepositoryConfig[*User]{
	AggregateType: "User",
	SchemaVersion: 1, // Current shape version of the User struct

	Initializer: func(id string) *User {
		return &User{ID: id}
	},

	Apply: func(u *User, event store.PersistedEvent) (*User, error) {
		// Apply event payload to mutate aggregate state
		switch event.EventType {
		case "UserCreated":
			// ...
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

// 3. Create the generic repository
userRepo, err := snapshot.NewRepository(eventStore, snapshotStore, config)
```

### 4. Load & Save in Command Handlers

Your command handlers run within database transactions:

```go
tx, err := db.Begin(ctx)
if err != nil {
	return err
}
defer tx.Rollback(ctx)

// Load rehydrates state from snapshot + delta events
user, version, err := userRepo.Load(ctx, tx, userID)
if err != nil {
	return err
}

// ... Execute business logic producing events ...

// Commit events to the event store
result, err := eventStore.Append(ctx, tx, store.Exact(version), newEvents)
if err != nil {
	return err
}

// Manual/Explicit snapshot trigger (e.g. every 100 events)
if result.ToVersion() % 100 == 0 {
	err = userRepo.Save(ctx, tx, userID, result.ToVersion(), user)
	if err != nil {
		return err
	}
}

return tx.Commit(ctx)
```

---

## Best Practices & Architecture Details

### Schema Versioning & Aggregates Evolution

When your aggregate root struct modifications break compatibility with previously serialized snapshot payloads:
1. Increment the `SchemaVersion` integer in your `RepositoryConfig`.
2. When the application loads the aggregate, the repository detects that the stored snapshot's schema version mismatches the configuration.
3. It discards the snapshot and replays the entire event stream from version 1.
4. When a snapshot is saved next, it will overwrite the old snapshot with the new schema version and structure.

*Note: For extremely long streams where full replay is slow, it is recommended to keep `SchemaVersion` at 1 and handle fallback migrations manually within your custom `Unmarshal` function.*

### Decoupled Encryption

To secure snapshot payloads containing PII or sensitive secrets, wrap your custom `Marshal` and `Unmarshal` callbacks with `eventsalsa/encryption` (or any other encryption mechanism):

```go
config := snapshot.RepositoryConfig[*User]{
	Marshal: func(u *User) ([]byte, error) {
		plaintext, err := json.Marshal(u)
		if err != nil {
			return nil, err
		}
		return myEncryptor.Encrypt(plaintext)
	},
	Unmarshal: func(data []byte) (*User, error) {
		plaintext, err := myEncryptor.Decrypt(data)
		if err != nil {
			return nil, err
		}
		var u User
		err = json.Unmarshal(plaintext, &u)
		return &u, err
	},
}
```

This keeps the core snapshot infrastructure simple, focused, and free of encryption library dependencies.

---

## Verification

Run checks, code formatters, linters, unit tests, and integration tests locally:

```bash
make check
```
