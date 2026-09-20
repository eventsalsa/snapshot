package snapshot

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
)

// Snapshot represents a persisted stream state snapshot at a specific version.
type Snapshot struct {
	CreatedAt     time.Time
	StreamType    string
	StreamID      string
	Payload       []byte
	StreamVersion int64
	SchemaVersion int
}

// Result contains the rehydrated stream state along with operational load data.
type Result[T any] struct {
	// State is the rehydrated stream state up to StreamVersion.
	State T

	// StreamVersion is the final version of the stream after delta replay.
	StreamVersion int64

	// SnapshotVersion is the version of the snapshot that was loaded (0 if no snapshot was used).
	SnapshotVersion int64

	// EventsReplayed is the number of delta events replayed from the event store.
	EventsReplayed int

	// SnapshotSchemaVersion is the raw schema version of the snapshot row loaded from storage (0 if miss).
	SnapshotSchemaVersion int

	// SnapshotHit is true if a valid snapshot (either matching or upcasted) was loaded and used.
	SnapshotHit bool

	// Upcasted is true if the snapshot was migrated from an older schema version using configured upcasters.
	Upcasted bool
}

// Data returns the rehydrated stream state (alias for State).
func (r Result[T]) Data() T {
	return r.State
}

// LoadResult is an alias for Result for callers who prefer explicit naming.
type LoadResult[T any] = Result[T]

// Upcaster transforms raw snapshot payload bytes from one schema version to the next schema version.
// For example, an upcaster registered for version 1 transforms a schema version 1 payload into a schema version 2 payload.
type Upcaster func(fromSchemaVersion int, payload []byte) ([]byte, error)

// Store defines the low-level persistence interface for snapshots.
type Store interface {
	// Get retrieves the snapshot for the given stream with the highest schema version <= maxSchemaVersion.
	// maxSchemaVersion must be greater than 0.
	// Returns a zero Snapshot and nil if no snapshot exists.
	Get(ctx context.Context, tx pgx.Tx, streamType, streamID string, maxSchemaVersion int) (Snapshot, error)

	// Put saves (inserts or updates) a snapshot for the stream and schema version.
	// Preserves monotonicity for the (stream_type, stream_id, schema_version) tuple.
	Put(ctx context.Context, tx pgx.Tx, snapshot *Snapshot) error
}
