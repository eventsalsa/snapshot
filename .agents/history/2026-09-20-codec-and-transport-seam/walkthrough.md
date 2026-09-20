# Walkthrough: `snapshot.Codec`, Transport Seam, and Modular Refactoring

## Overview
This initiative introduces a clean, decoupled architecture for snapshot payload serialization and wire transport as proposed in the design memo:
1. **`Codec[T]` Seam**: Replaces separate `Marshal` and `Unmarshal` functions with a cohesive `Codec[T]` interface, providing a standard `snapshot.JSON[T]()` default codec and `snapshot.FuncCodec[T]` functional adapter.
2. **Transport Seam (`PayloadTransformer`)**: Introduces an outer boundary for wire byte transformations (compression, envelope encryption, framing) that operates strictly outside the schema versioning boundary.
3. **Pipeline Invariant**: Enforces that **Upcasters always operate on restored plaintext bytes** at known schema versions:
   - **Write Path (Outbound)**: `State T` $\to$ `Codec.Encode` (plaintext @ current schema) $\to$ `Transformers.Transform` (stored bytes) $\to$ `Store.Put`
   - **Read Path (Inbound)**: `Store.Get` $\to$ `Transformers.Restore` (plaintext @ row schema) $\to$ `Upcasters` (plaintext @ current schema) $\to$ `Codec.Decode` $\to$ `State T`
4. **Modular Architecture Split**: Decomposed the growing `snapshot.go` and `snapshot_test.go` monoliths into 12 domain-specific source and test modules.

---

## Phase 1: `Codec[T]` Interface & Default `JSON[T]`

- **PR**: [eventsalsa/snapshot#25](https://github.com/eventsalsa/snapshot/pull/25) (Merged)
- **Changes**:
  - Defined `Codec[T]` interface (`Encode`, `Decode`).
  - Implemented `snapshot.JSON[T]()` backed by Go standard library `encoding/json`.
  - Implemented `snapshot.FuncCodec[T]` functional adapter.
  - Replaced `Marshal` and `Unmarshal` in `RepositoryConfig[T]` with `Codec Codec[T]`.
  - Updated `Save` and `resolveSnapshot` to delegate to `Codec`.
  - Migrated example and integration tests.

---

## Phase 2: `PayloadTransformer` Transport Seam & `snapshot.Gzip`

- **PR**: [eventsalsa/snapshot#26](https://github.com/eventsalsa/snapshot/pull/26) (In Review)
- **Changes**:
  - Defined `PayloadTransformer` interface (`Transform`, `Restore`).
  - Implemented `snapshot.Gzip(level ...int)` with configurable compression levels.
  - Implemented `snapshot.FuncTransformer(transform, restore)` adapter.
  - Added `Transformers []PayloadTransformer` to `RepositoryConfig[T]`.
  - Integrated forward pipeline execution in `Repository.Save` ($0 \dots N-1$).
  - Integrated reverse pipeline execution in `Repository.resolveSnapshot` ($N-1 \dots 0$) before upcasters run.
  - Implemented resilient fallback (stream event replay and `OnSnapshotRejected`) and strict fail-fast on corruption.
  - Added comprehensive test suites verifying ordering, upcasting on decompressed plaintext, and error boundaries.

---

## Modular Architecture Refactoring

To keep the codebase maintainable and maintain high cohesion with small, single-purpose files, both production and test files were decomposed:

### Production Modules
- `snapshot.go` (66 lines): Core models (`Snapshot`, `Store`, `Result[T]`, `LoadResult[T]`, `Upcaster`).
- `codec.go` (68 lines): `Codec[T]` interface, `JSON[T]()`, and `FuncCodec[T]()`.
- `transformer.go` (95 lines): `PayloadTransformer` interface, `Gzip()`, and `FuncTransformer()`.
- `policy.go` (30 lines): `Policy`, `EveryNEvents`, `Never`, and `ShouldSnapshot`.
- `repository.go` (413 lines): `Repository[T]`, `RepositoryConfig[T]`, `NewRepository`, `Load`, `Save`, and pipeline orchestration.

### Test Modules
- `helpers_test.go` (90 lines): Shared test state, models, and mocks.
- `snapshot_test.go` (53 lines): Direct tests for `Snapshot` and `Result.Data()`.
- `codec_test.go` (194 lines): `JSONCodec`, `FuncCodec`, and stream ID validation.
- `transformer_test.go` (535 lines): Gzip, functional transformers, pipeline ordering, error fallbacks.
- `policy_test.go` (70 lines): Snapshot policy thresholds and boundary conditions.
- `upcaster_test.go` (477 lines): Multi-version schema upcasting chains and error propagation.
- `repository_test.go` (1263 lines): Repository config validation, save, load, delta replay, and orphan recovery.

---

## Verification

- **Code Formatting**: `make fmt` passed cleanly.
- **Linting**: `golangci-lint run` passed with 0 issues across all 12 modules.
- **Unit Tests**: `make test-unit` passed with 97.3% statement coverage.
- **Integration Tests**: `make test-integration` passed across all suites against real PostgreSQL containers.
