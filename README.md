# eventsalsa/snapshot

[![CI](https://github.com/eventsalsa/snapshot/actions/workflows/ci.yml/badge.svg)](https://github.com/eventsalsa/snapshot/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/eventsalsa/snapshot.svg)](https://pkg.go.dev/github.com/eventsalsa/snapshot)

`github.com/eventsalsa/snapshot` is a generic, PostgreSQL-backed stream state snapshotting module for Go event-sourced applications. 

It is designed to be used alongside [`github.com/eventsalsa/store`](https://github.com/eventsalsa/store). By saving a snapshot of the stream state at a specific version, you avoid reading the full stream of events from version 1. The repository loads the latest snapshot, reads only subsequent events (the delta), and applies them to reconstruct the active state.

## Features

- **Generic & Type-Safe**: Uses Go generics (`[T any]`) to define repositories, avoiding any coupling or structural inheritance inside your stream state types.
- **Pgx Transactional Integrity**: All operations run within caller-provided `pgx.Tx` transactions, matching the design of `eventsalsa/store`.
- **Compound Primary Key & Multi-Version Rolling Deploys**: Primary key `(stream_type, stream_id, schema_version)` allows older and newer application versions to read and write their own snapshots concurrently without overwriting each other.
- **Sequential In-Memory Upcasters**: Convert older snapshot payloads on read via `Upcasters: map[int]Upcaster` to eliminate $O(N)$ event replay spikes during schema version migrations.
- **Resilient Fallback**: Discards corrupt or unparseable snapshot payloads and recovers automatically via event log replay.
- **Version Parity Guard**: Optional `Version` callback verifies aggregate state version against the snapshot version to prevent state desynchronization.
- **Low-Level and High-Level APIs**: Exposes a raw byte `Store` interface alongside a high-level `Repository[T]`.
- **Migration Generation**: Comes with a built-in SQL migration generator and CLI tool to generate PostgreSQL tables.

---

## Installation

```bash
go get github.com/eventsalsa/snapshot
```

---

## Quick Start

### 1. Generate & Apply DB Migration

Use the CLI tool to generate the migration file containing the DDL for the `snapshots` table:

```bash
go run github.com/eventsalsa/snapshot/cmd/migrate-gen -output migrations
```

This generates a file named like `migrations/<timestamp>_init_snapshots.sql`:

```sql
CREATE TABLE IF NOT EXISTS snapshots (
    stream_type TEXT NOT NULL,
    stream_id TEXT NOT NULL,
    stream_version BIGINT NOT NULL,
    schema_version INT NOT NULL,
    payload BYTEA NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    PRIMARY KEY (stream_type, stream_id, schema_version)
);

CREATE INDEX IF NOT EXISTS idx_snapshots_schema_version 
    ON snapshots (schema_version);
```

Apply this migration using your preferred PostgreSQL migration runner.

### 2. Define Stream State

Define the state struct representing your materialized stream:

```go
type UserState struct {
	ID    string `json:"id"`
	Name  string `json:"name"`
	Email string `json:"email"`
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

// 1. Create the stores (normally singletons in your application)
eventStore := storepostgres.NewStore(storepostgres.DefaultStoreConfig())
snapshotStore := snapshotpostgres.NewStore(snapshotpostgres.DefaultStoreConfig())

// 2. Define repository configuration
config := snapshot.RepositoryConfig[*UserState]{
	StreamType:    "User",
	SchemaVersion: 1, // Current version of the UserState struct shape

	Initializer: func(id string) *UserState {
		return &UserState{ID: id}
	},

	Apply: func(state *UserState, event store.PersistedEvent) (*UserState, error) {
		switch event.EventType {
		case "UserCreated":
			// mutate state
		case "UserEmailChanged":
			// mutate state
		}
		return state, nil
	},

	Marshal: func(state *UserState) ([]byte, error) {
		return json.Marshal(state)
	},

	Unmarshal: func(streamID string, data []byte) (*UserState, error) {
		var state UserState
		if err := json.Unmarshal(data, &state); err != nil {
			return nil, err
		}
		return &state, nil
	},
}

// 3. Create the generic repository
userRepo, err := snapshot.NewRepository(eventStore, snapshotStore, config)
```

### 4. Loading & Saving Snapshots

#### Loading Rehydrated State

`Load` retrieves the latest valid snapshot and replays subsequent delta events to reconstruct state up to the latest stream version:

```go
tx, err := db.Begin(ctx)
if err != nil {
	return err
}
defer tx.Rollback(ctx)

res, err := userRepo.Load(ctx, tx, userID)
if err != nil {
	return err
}

// res contains:
// - res.State:                 the rehydrated *UserState
// - res.StreamVersion:         current stream version after delta replay (e.g. 150)
// - res.SnapshotVersion:       version where snapshot was loaded from (e.g. 100, or 0 if miss)
// - res.SnapshotHit:           true if a valid snapshot was used
// - res.EventsReplayed:        number of delta events read and applied (e.g. 50)
// - res.SnapshotSchemaVersion: raw schema version of the loaded snapshot (e.g. 1, or 0 if miss)
// - res.Upcasted:              true if an older snapshot was migrated via upcasters
```

#### Saving Snapshots

Persist a snapshot of your stream state at a known version:

```go
err = userRepo.Save(ctx, tx, userID, streamVersion, state)
if err != nil {
	return err
}
```

#### Snapshotting After Appends with Policy Evaluation

Use `res.ShouldSnapshot` along with a `snapshot.Policy` to determine whether a snapshot should be taken following an append:

```go
// 1. Load current state
res, err := userRepo.Load(ctx, tx, userID)
if err != nil {
	return err
}

// 2. Execute domain logic on aggregate and append new events
appendRes, err := eventStore.Append(ctx, tx, store.Exact(res.StreamVersion), events)
if err != nil {
	return err
}

// 3. Check snapshot policy and save updated state
policy := snapshot.EveryNEvents(100)
if res.ShouldSnapshot(policy, int64(len(events))) {
	err = userRepo.Save(ctx, tx, userID, appendRes.ToVersion(), updatedUserState)
	if err != nil {
		return err
	}
}

return tx.Commit(ctx)
```

---

## Best Practices & Architecture Details

### Schema Versioning & Stream Evolution

When your stream state struct shape changes across versions:
1. Increment the `SchemaVersion` integer in your `RepositoryConfig`.
2. Configure sequential `Upcasters: map[int]Upcaster` to transform older payloads (e.g., version 1 to 2, 2 to 3) on the fly without reading the event log from scratch.
3. If an upcaster in the chain is missing or encounters an error, the repository safely falls back to replaying the entire event stream from version 1.
4. During rolling deployments, older and newer service instances operate simultaneously against the compound primary key `(stream_type, stream_id, schema_version)` without overwriting each other's snapshots.

### Decoupled Encryption

In eventsalsa, sensitive fields (PII or secrets) are protected at the **field level** using custom value objects (e.g. `user.EncryptedEmail` strings) as explained in `eventsalsa/encryption` documentation. 

Because of this design, the state fields themselves already store ciphertext in memory. Therefore:
- Standard serialization (via `Marshal`) automatically preserves field-level encryption inside the snapshot payload.
- You should not encrypt the full snapshot payload. Doing so is unnecessary, breaks payload inspectability, and deviates from eventsalsa's fine-grained field-level encryption boundaries.

---

## Verification

Run checks, code formatters, linters, unit tests, and integration tests locally:

```bash
make check
```
