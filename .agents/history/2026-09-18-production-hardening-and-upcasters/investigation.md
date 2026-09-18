# Investigation & Decision Report: `eventsalsa/snapshot` Probe Test-Drive

**Date:** 2026-09-17  
**Target Subject:** `eventsalsa/snapshot`  

---

## 1. Executive Summary

An independent probe test-drive was conducted on `github.com/eventsalsa/snapshot` on top of `eventsalsa/store` and PostgreSQL 16.

The probe exercised a comprehensive integration suite with counting decorators around `store.StreamReader` to measure actual event log reads versus full replays.

### Key Takeaways
1. **Core Mechanism Verified Sound**: The snapshot delta-replay concept was verified sound. On a 2,000-event stream, `Load` replayed 0 events instead of 2,000, with byte-identical state to a full replay. Concurrency under optimistic lock contention, transactional atomicity (rollback safety), stream type isolation, and custom table configuration all passed.
2. **Append Safety Bug in Documentation**: The README Quick-Start snippet saved the *pre-append* state with the *post-append* version, causing silent loss of newly appended events on subsequent reads.
3. **Missing Guardrails & Validation**: `Repository.Save` and `Repository.Load` accepted out-of-bounds versions (ahead of log head), unvalidated state, and lacked monotonic write protection in the Postgres store.
4. **Schema Evolution Sharp Edges**: While schema mismatches successfully triggered full replays, no replacement snapshot was scheduled, leading to O(N) replay performance degradation indefinitely. Furthermore, rolling deployments experienced schema thrashing.

---

## 2. Key Findings & Resolutions Across All 8 Phases

### 1. Append Safety & Domain Purity (Item 1)
- **Problem**: Consumers saved pre-append state with post-append version, causing silent data loss.
- **Resolution**: Implemented `SaveAppended(ctx, tx, id, state, appendResult)` which folds newly appended events into aggregate state before saving. Kept repository pure infrastructure without leaking into domain layers. Merged in PR #16.

### 2. Stream Head & Version Validation (Item 2)
- **Problem**: Snapshots claiming versions ahead of the event log head or orphan snapshots on empty streams were trusted unconditionally.
- **Resolution**: Added authoritative stream head and version validation in `Load` and `Save`. Snapshots ahead of log head or orphan snapshots fall back cleanly to event replay. Merged in PR #17.

### 3. PostgreSQL Store Monotonicity Guard (Item 3)
- **Problem**: `Put` did an unconditional upsert, allowing stale or out-of-order writes to regress snapshot versions.
- **Resolution**: Added `WHERE EXCLUDED.stream_version >= snapshots.stream_version`. Merged in PR #13.

### 4. Corrupted Snapshot Payload Resilience (Item 4)
- **Problem**: Deserialization failures on corrupted payloads caused read outages even though the event log was intact.
- **Resolution**: Added resilient fallback to full stream replay (with `FailOnCorruptSnapshot` toggle for strict mode). Merged in PR #14.

### 5. Multi-Version Rolling Deployments & Upcasters (Item 5)
- **Problem**: Rolling deployments (v1 and v2 pods) overwrote each other's snapshot row in the database, and schema version bumps triggered persistent $O(N)$ replay spikes.
- **Resolution**: Established compound primary key `(stream_type, stream_id, schema_version)` in PostgreSQL and sequential in-memory `Upcaster` chains (`Upcasters: map[int]Upcaster`). Merged in PR #18.

### 6. Snapshot Metadata & Observability in `Load` (Item 6)
- **Problem**: `Load` returned only `(state, version, err)` without observability into cache hit status or delta event count.
- **Resolution**: Unified return type `Result[T]` exposing state, stream version, snapshot version, snapshot hit indicator, schema version, delta events replayed, snapshot schema version, and upcasted status, along with policy evaluation helpers. Merged in PR #15 and PR #18.

### 7. Strict Validation of `SchemaVersion` (Item 7)
- **Problem**: `SchemaVersion: 0` silently disabled schema evolution checks.
- **Resolution**: Added strict startup validation requiring `SchemaVersion >= 1`. Merged in PR #12.

### 8. Dependency Tree Alignment
- **Problem**: Snapshot was pinned to older `eventsalsa/store v0.1.0`.
- **Resolution**: Upgraded to `eventsalsa/store v0.2.0` and `jackc/pgx/v5 v5.11.0`. Merged in Dependabot PR #11.
