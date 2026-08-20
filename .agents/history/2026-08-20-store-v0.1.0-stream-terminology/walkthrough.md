# Walkthrough: Upgrade to `eventsalsa/store:v0.1.0` and Stream Terminology Alignment

All 4 phases of the migration have been implemented, tested, and verified.

## Completed Changes

### 1. Dependency Upgrade
- Upgraded `github.com/eventsalsa/store` from `v0.0.4` to `v0.1.0` in [go.mod](go.mod).

### 2. Core API Migration ([snapshot.go](snapshot.go), [doc.go](doc.go))
- Renamed all aggregate terminology to stream terminology:
  - `Snapshot`: `StreamType`, `StreamID`, `StreamVersion`
  - `Store`: `Get(ctx, tx, streamType, streamID)`
  - `RepositoryConfig[T]`: `StreamType`
  - `Repository[T]`: uses `store.StreamReader` and calls `ReadStream(...)`

### 3. PostgreSQL Driver ([postgres/store.go](postgres/store.go))
- Changed default table name from `aggregate_snapshots` to `snapshots`.
- Updated SQL queries to select/upsert columns `stream_type`, `stream_id`, and `stream_version`.
- Aligned all structured logging attributes with `stream_*` keys.

### 4. Migrations & Tooling ([migrations/](migrations), [cmd/migrate-gen/](cmd/migrate-gen))
- Generated PostgreSQL migration DDLs targeting `snapshots` table with index `idx_snapshots_schema_version`.
- Updated embedded SQL migration and CLI default flags to `"snapshots"`.

### 5. Tests & Examples ([snapshot_test.go](snapshot_test.go), [migrations/generator_test.go](migrations/generator_test.go), [integration_test/](integration_test), [examples/basic/](examples/basic))
- Added 100% statement coverage unit tests in `snapshot_test.go` covering rehydration, snapshot saving, validation boundaries, and schema version mismatch fallback.
- Added unit tests in `migrations/generator_test.go` for DDL generation and embedded SQL integrity.
- Updated integration test suite with testcontainers PostgreSQL to test real database execution.
- Updated the runnable example in `examples/basic/main.go`.

### 6. Documentation ([README.md](README.md))
- Fully aligned documentation, quickstart examples, and DDL references with stream state snapshotting terminology.

---

## Verification Results

| Suite | Status | Details |
|---|---|---|
| `make fmt` | Passed | Formatted and organized imports |
| `make test-unit` | Passed | 100% statement coverage on core package |
| `make test-integration` | Passed | Passed against real PostgreSQL containers |
| `make lint` | Passed | 0 issues reported by `golangci-lint` |
| `make build` | Passed | All binaries and packages build cleanly |
