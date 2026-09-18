# eventsalsa/snapshot

[![CI](https://github.com/eventsalsa/snapshot/actions/workflows/ci.yml/badge.svg)](https://github.com/eventsalsa/snapshot/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/eventsalsa/snapshot.svg)](https://pkg.go.dev/github.com/eventsalsa/snapshot)

`github.com/eventsalsa/snapshot` is a generic, PostgreSQL-backed stream state snapshotting module for Go event-sourced applications. 

It is designed to be used alongside [`github.com/eventsalsa/store`](https://github.com/eventsalsa/store). By saving a snapshot of the stream state at a specific version, you avoid reading the full stream of events from version 1. The repository loads the latest snapshot, reads only subsequent events (the delta), and applies them to reconstruct the active state.

## Features

- **Generic & Type-Safe**: Uses Go generics (`[T any]`) to define repositories, avoiding any coupling or structural inheritance inside your stream state types.
- **Pgx Transactional Integrity**: All operations run within caller-provided `pgx.Tx` transactions, matching the design of `eventsalsa/store`.
- **Automatic Schema Evolution**: Includes a `schema_version` column. If the snapshot stored in the database has a schema version that mismatches the code's expected version, the repository automatically discards it and falls back to a full replay of events from version 1.
- **Resilient Fallback**: Discards corrupt or unparseable snapshot payloads and recovers automatically via event log replay.
- **Append-Safe Persistence**: Provides `SaveAppended` to fold newly appended events into aggregate state before snapshot write, preventing state regression.
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
    
    PRIMARY KEY (stream_type, stream_id)
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

	Unmarshal: func(data []byte) (*UserState, error) {
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
// - res.State:           the rehydrated *UserState
// - res.StreamVersion:   current stream version (e.g. 150)
// - res.SnapshotVersion: version where snapshot was loaded from (e.g. 100, or 0 if miss)
// - res.SnapshotHit:     true if a matching snapshot was used
// - res.EventsReplayed:  number of delta events read and applied (e.g. 50)
```

#### Saving Snapshots

You can save a snapshot directly at a known stream version:

```go
err = userRepo.Save(ctx, tx, userID, streamVersion, state)
if err != nil {
	return err
}
```

#### Append-Safe Persistence with `SaveAppended`

When appending events to a stream, use `SaveAppended` to persist a snapshot safely. It folds the newly appended events from `store.AppendResult` into the state before writing the snapshot, ensuring the snapshot matches the new stream version:

```go
// 1. Load current state
res, err := userRepo.Load(ctx, tx, userID)
if err != nil {
	return err
}

// 2. Append new events to the event store
appendRes, err := eventStore.Append(ctx, tx, store.Exact(res.StreamVersion), events)
if err != nil {
	return err
}

// 3. Check snapshot policy and save safely
policy := snapshot.EveryNEvents(100)
if res.ShouldSnapshot(policy, int64(len(events))) {
	updatedState, err := userRepo.SaveAppended(ctx, tx, userID, res.State, appendRes)
	if err != nil {
		return err
	}
	_ = updatedState
}

return tx.Commit(ctx)
```

> [!WARNING]
> Do not call `Save` using pre-append state with `appendRes.ToVersion()`. Passing state that has not had the newly appended events applied will record stale state in the snapshot. Always apply the newly appended events or use `SaveAppended`.

---

## Best Practices & Architecture Details

### Schema Versioning & Stream Evolution

When your stream state struct shape changes in backwards-incompatible ways:
1. Increment the `SchemaVersion` integer in your `RepositoryConfig`.
2. When loading state, the repository detects that the stored snapshot's `schema_version` does not match `SchemaVersion`.
3. It safely discards the snapshot and replays the entire event stream from version 1.
4. When a new snapshot is saved, it writes the updated `schema_version` and serialized payload.

### Decoupled Encryption

In eventsalsa, sensitive fields (PII or secrets) are protected at the **field level** using custom value objects (e.g. `user.EncryptedEmail` strings) as explained in `eventsalsa/encryption` documentation. 

Because of this design, the state fields themselves already store ciphertext in memory. Therefore:
- Standard serialization (via `Marshal`) automatically preserves field-level encryption inside the snapshot payload.
- You should not encrypt the full snapshot payload. Doing so is unnecessary, breaks payload inspectability, and deviates from eventsalsa's fine-grained field-level encryption boundaries.

---

## Release Automation

This repository uses [Release Please](https://github.com/googleapis/release-please) to automate changelogs, version bumps, and git releases based on Conventional Commits.

### How it Works

1. On every push to the `main` branch, the `release-please` workflow checks the commit history since the last release.
2. If there are new features (`feat:`) or bug fixes (`fix:`), it opens or updates a **Release PR** containing version increments and an updated `CHANGELOG.md`.
3. When the Release PR is merged into `main`, it automatically:
   - Tags the merge commit with the version tag (e.g. `v1.0.0`).
   - Creates a corresponding GitHub Release with compilation notes and changelogs.

### Downstream Workflow Triggering (Optional)

By default, the workflow uses the standard `${{ secrets.GITHUB_TOKEN }}`. Because GitHub blocks actions taken by the default token from triggering other workflows:
- Downstream workflows (like CI tests) will **not** trigger on the release PR or the release tag automatically.
- If you require CI checks to run on the release PR and tag, configure a custom Personal Access Token (PAT) or GitHub App in your repository settings and reference it as `token: ${{ secrets.MY_CUSTOM_PAT }}` in `.github/workflows/release.yml`.

---

## Verification

Run checks, code formatters, linters, unit tests, and integration tests locally:

```bash
make check
```
