# 8-Phase Production Hardening Walkthrough: `eventsalsa/snapshot`

## Overview

This document provides the complete record of the production hardening and architectural enhancements delivered across all 8 phases of [`eventsalsa/snapshot`](.).

The initiative addressed 7 key vulnerabilities and operational risks discovered during initial production probing:
1. **Append Safety & Domain Purity**: Eliminating silent data loss patterns in consumer code while maintaining clean architecture separation.
2. **Authoritative Stream Head Validation**: Preventing cache corruption and phantom state reconstruction by verifying snapshot versions against the event log.
3. **PostgreSQL Monotonicity Guards**: Protecting snapshot rows from lagging or out-of-order write regressions.
4. **Resilient Deserialization**: Preventing read outages on corrupt snapshot payloads by falling back to event replay.
5. **Multi-Version Rolling Deployments & Upcasters**: Preventing row thrashing across canary/rolling deployments via compound primary keys and eliminating $O(N)$ replay spikes via sequential in-memory upcasters.
6. **Result Observability & Policy Helpers**: Exposing complete rehydration metadata (`Result[T]`) for fine-grained snapshotting policies.
7. **Strict Configuration Validation**: Rejecting uninitialized zero-value configurations (`SchemaVersion <= 0`).

---

## Phase Summary

### Phase 1: Startup Invariant & Strict Configuration Validation
- **PR**: [#12](https://github.com/eventsalsa/snapshot/pull/12)
- Added validation for `config.SchemaVersion >= 1` in `NewRepository`.
- Blocked zero-value configurations which previously silently disabled schema version checks.

### Phase 2: PostgreSQL Store Monotonicity Guard
- **PR**: [#13](https://github.com/eventsalsa/snapshot/pull/13)
- Added `WHERE EXCLUDED.stream_version >= snapshots.stream_version` to `postgres.Store.Put`.
- Guaranteed that out-of-order writes or lagging workers cannot regress snapshot versions.

### Phase 3: Resilient Snapshot Deserialization with Fallback
- **PR**: [#14](https://github.com/eventsalsa/snapshot/pull/14)
- Updated `Repository.Load` to catch unmarshaling errors, log a warning/error, and fall back to full stream replay from version 1.
- Added `FailOnCorruptSnapshot` config toggle for applications requiring strict fail-fast behavior.

### Phase 4: Snapshot Result API & Policy Helpers
- **PR**: [#15](https://github.com/eventsalsa/snapshot/pull/15)
- Replaced dual `Load` / `LoadWithMeta` with unified `Load(ctx, tx, id) (Result[T], error)`.
- Introduced `Result[T]` returning `State`, `StreamVersion`, `SnapshotVersion`, `SnapshotHit`, `EventsReplayed`, and `SchemaVersion`.
- Added snapshot trigger policy helpers: `EveryNEvents(n)`, `Never()`, and `Result.ShouldSnapshot(policy, appendedCount)`.

### Phase 5: Intermediate Upgrade: `eventsalsa/store` to `v0.2.0`
- **PR**: [#11](https://github.com/eventsalsa/snapshot/pull/11)
- Upgraded `github.com/eventsalsa/store` to `v0.2.0` and `jackc/pgx/v5` to `v5.11.0`.

### Phase 6: Append-Safe Workflow & Pure Infrastructure Boundary
- **PR**: [#16](https://github.com/eventsalsa/snapshot/pull/16)
- Introduced `SaveAppended(ctx, tx, id, state, appendResult)` to fold newly appended events into aggregate state before saving snapshots.
- Sanitized `README.md` and `examples/basic/main.go` to remove domain-layer command handler conflation, preserving pure infrastructure boundaries.

### Phase 7: Authoritative Stream Head & Version Boundary Validation
- **PR**: [#17](https://github.com/eventsalsa/snapshot/pull/17)
- Added verification against event log head during `Load` and `Save`.
- Invalidates snapshots claiming versions ahead of the event stream or orphan snapshots on non-existent streams, falling back to clean reconstruction.
- Rejects `Save` calls with versions not yet committed to the event log.

### Phase 8: Rolling Deploy Multi-Version Support (Compound PK) & Upcasters
- **PR**: [#18](https://github.com/eventsalsa/snapshot/pull/18)
- Updated PostgreSQL table primary key to `(stream_type, stream_id, schema_version)`.
- Updated `Store.Get` to query `WHERE schema_version <= maxSchemaVersion ORDER BY schema_version DESC LIMIT 1`.
- Added sequential in-memory `Upcasters map[int]Upcaster` to transform older payloads on the fly without $O(N)$ event replay spikes.
- Added `SnapshotSchemaVersion` and `Upcasted` metadata fields to `Result[T]`.
- Consolidated compound primary key into canonical initialization migration `migrations/20260621084951_init_snapshots.sql`.

---

## Verification Summary

All phases were verified with 100% statement coverage on core packages, full PostgreSQL 16 testcontainer integration suites, and strict linting:
- `make fmt`: Clean.
- `golangci-lint run --timeout=5m`: **0 issues**.
- Statement coverage: **100.0%** on `github.com/eventsalsa/snapshot`.
- PostgreSQL 16 testcontainers: All test suites passed with zero regressions.
