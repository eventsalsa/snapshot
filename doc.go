// Package snapshot provides stream state snapshotting for event-sourced systems.
//
// This package enables periodic state persistence of stream states to optimize load times.
// By loading the latest snapshot and replaying only subsequent events instead of replaying
// the entire stream from version 1, read performance remains fast even for streams with
// long histories.
//
// Key features:
//
//   - Low-level persistence: Store interface for raw byte snapshot operations.
//   - High-level orchestration: Repository[T] generic wrapper to automate the rehydration flow.
//   - Schema evolution: Automatic version-checking that discards outdated snapshots and falls back to full replay.
//   - Pluggable serialization: Customize JSON, Protobuf, or encryption behavior via callbacks.
//
// The postgres package provides the PostgreSQL implementation of the Store interface.
//
// Example usage:
//
//	res, err := repo.Load(ctx, tx, userID)
//	if err != nil {
//		return err
//	}
//	// ... run domain logic and append events ...
//	appendRes, err := eventStore.Append(ctx, tx, store.Exact(res.StreamVersion), events)
//	if err != nil {
//		return err
//	}
//	if res.ShouldSnapshot(snapshot.EveryNEvents(100), int64(len(events))) {
//		_, err = repo.SaveAppended(ctx, tx, userID, res.State, appendRes)
//		if err != nil {
//			return err
//		}
//	}
package snapshot
