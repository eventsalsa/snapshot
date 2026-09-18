package snapshot

import (
	"context"
	"fmt"
	"time"

	"github.com/eventsalsa/store"
	"github.com/google/uuid"
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

	// SchemaVersion is the schema version of the snapshot that was loaded (0 if none was used).
	SchemaVersion int

	// SnapshotHit is true if a valid snapshot matching the configured schema version was loaded and used.
	SnapshotHit bool
}

// Data returns the rehydrated stream state (alias for State).
func (r Result[T]) Data() T {
	return r.State
}

// LoadResult is an alias for Result for callers who prefer explicit naming.
type LoadResult[T any] = Result[T]

// Policy evaluates whether a snapshot should be persisted following an append operation.
// It receives the stream version at load time, the snapshot version at load time (0 if miss),
// and the number of events appended to the stream.
type Policy func(streamVersion, snapshotVersion int64, appendedCount int64) bool

// EveryNEvents returns a policy that triggers a snapshot whenever the number of events
// since the last snapshot (including newly appended events) is greater than or equal to n.
func EveryNEvents(n int64) Policy {
	return func(streamVersion, snapshotVersion int64, appendedCount int64) bool {
		if n <= 0 {
			return false
		}
		return (streamVersion-snapshotVersion)+appendedCount >= n
	}
}

// Never returns a policy that never triggers a snapshot.
func Never() Policy {
	return func(_, _, _ int64) bool { return false }
}

// ShouldSnapshot evaluates whether the given policy triggers a snapshot after appending appendedCount events.
func (r Result[T]) ShouldSnapshot(policy Policy, appendedCount int64) bool {
	if policy == nil {
		return false
	}
	return policy(r.StreamVersion, r.SnapshotVersion, appendedCount)
}

// Store defines the low-level persistence interface for snapshots.
type Store interface {
	// Get retrieves the latest snapshot for the given stream.
	// Returns a zero Snapshot and nil if no snapshot exists.
	Get(ctx context.Context, tx pgx.Tx, streamType, streamID string) (Snapshot, error)

	// Put saves (inserts or updates) a snapshot for the stream.
	// Overwrites the existing snapshot if it already exists for the (type, id) pair.
	Put(ctx context.Context, tx pgx.Tx, snapshot *Snapshot) error
}

// RepositoryConfig contains configuration for the generic Repository.
type RepositoryConfig[T any] struct {
	// Logger is an optional logger for observability.
	// If nil, logging is disabled (zero overhead).
	Logger store.Logger

	// Initializer instantiates a new, empty stream state for a given ID.
	Initializer func(id string) T

	// Apply applies a single persisted event to update the stream state.
	// It must return the updated state (which can be the mutated input state).
	Apply func(state T, event store.PersistedEvent) (T, error)

	// Marshal serializes the stream state into raw bytes for snapshot storage.
	Marshal func(state T) ([]byte, error)

	// Unmarshal deserializes raw snapshot bytes back into the stream state.
	Unmarshal func(data []byte) (T, error)

	// StreamType is the string identifier for the stream type (e.g. "User").
	StreamType string

	// SchemaVersion is the current version of the stream struct/logic schema.
	// Must be a positive integer (>= 1); version 0 is reserved for uninitialized configuration.
	// If a stored snapshot has a different SchemaVersion, it is ignored
	// and the stream is rehydrated from version 1 of the event log.
	SchemaVersion int

	// FailOnCorruptSnapshot determines whether an unmarshaling error on a snapshot
	// payload immediately fails Load (true), or safely discards the snapshot and
	// falls back to replaying the full stream of events from version 1 (false).
	// Defaults to false (resilient fallback).
	FailOnCorruptSnapshot bool
}

// Repository orchestrates the loading and saving of stream states
// using snapshots and events.
type Repository[T any] struct {
	reader        store.StreamReader
	snapshotStore Store
	config        RepositoryConfig[T]
}

// NewRepository creates a new generic Repository for a stream type.
func NewRepository[T any](
	reader store.StreamReader,
	snapshotStore Store,
	config RepositoryConfig[T],
) (*Repository[T], error) {
	if reader == nil {
		return nil, fmt.Errorf("event store reader cannot be nil")
	}
	if snapshotStore == nil {
		return nil, fmt.Errorf("snapshot store cannot be nil")
	}
	if config.StreamType == "" {
		return nil, fmt.Errorf("stream type cannot be empty")
	}
	if config.Initializer == nil {
		return nil, fmt.Errorf("initializer cannot be nil")
	}
	if config.Apply == nil {
		return nil, fmt.Errorf("apply cannot be nil")
	}
	if config.Marshal == nil {
		return nil, fmt.Errorf("marshal cannot be nil")
	}
	if config.Unmarshal == nil {
		return nil, fmt.Errorf("unmarshal cannot be nil")
	}
	if config.SchemaVersion <= 0 {
		return nil, fmt.Errorf("schema version must be >= 1 (got %d); version 0 is reserved for uninitialized configuration", config.SchemaVersion)
	}
	return &Repository[T]{
		reader:        reader,
		snapshotStore: snapshotStore,
		config:        config,
	}, nil
}

// Load rehydrates the stream state up to its current version.
// First, it attempts to load the latest snapshot. If a valid snapshot is found
// matching the configured SchemaVersion, it deserializes the state and reads
// newer events starting from version S + 1. Otherwise, it initializes a new state
// and reads all events from the beginning.
func (r *Repository[T]) Load(ctx context.Context, tx pgx.Tx, id string) (res Result[T], err error) {
	var snap Snapshot
	snap, err = r.snapshotStore.Get(ctx, tx, r.config.StreamType, id)
	if err != nil {
		return res, fmt.Errorf("failed to load snapshot: %w", err)
	}

	var fromVersion *int64
	var initialized bool
	var version int64

	// Check if we have a valid snapshot matching the current schema version
	if snap.StreamVersion > 0 && snap.SchemaVersion == r.config.SchemaVersion {
		res.State, err = r.config.Unmarshal(snap.Payload)
		if err != nil {
			if r.config.FailOnCorruptSnapshot {
				return res, fmt.Errorf("failed to unmarshal snapshot: %w", err)
			}
			if r.config.Logger != nil {
				r.config.Logger.Error(ctx, "failed to unmarshal snapshot payload; falling back to full stream replay",
					"stream_type", r.config.StreamType,
					"stream_id", id,
					"stream_version", snap.StreamVersion,
					"schema_version", snap.SchemaVersion,
					"error", err)
			}
		} else {
			version = snap.StreamVersion
			nextVersion := snap.StreamVersion + 1
			fromVersion = &nextVersion
			initialized = true
			res.SnapshotHit = true
			res.SnapshotVersion = snap.StreamVersion
			res.SchemaVersion = snap.SchemaVersion
		}
	}

	if !initialized {
		res.State = r.config.Initializer(id)
		version = 0
		fromVersion = nil
	}

	// Read newer events
	stream, err := r.reader.ReadStream(ctx, tx, r.config.StreamType, id, fromVersion, nil)
	if err != nil {
		return res, fmt.Errorf("failed to read stream: %w", err)
	}

	for i := range stream.Events {
		res.State, err = r.config.Apply(res.State, stream.Events[i])
		if err != nil {
			return res, fmt.Errorf("failed to apply event v%d: %w", stream.Events[i].StreamVersion, err)
		}
		version = stream.Events[i].StreamVersion
	}

	res.StreamVersion = version
	res.EventsReplayed = len(stream.Events)

	return res, nil
}

// Save persists a snapshot of the current stream state at the specified version.
func (r *Repository[T]) Save(ctx context.Context, tx pgx.Tx, id string, version int64, state T) error {
	if version <= 0 {
		return fmt.Errorf("invalid stream version: %d", version)
	}

	payload, err := r.config.Marshal(state)
	if err != nil {
		return fmt.Errorf("failed to marshal snapshot: %w", err)
	}

	snap := Snapshot{
		Payload:       payload,
		StreamType:    r.config.StreamType,
		StreamID:      id,
		StreamVersion: version,
		SchemaVersion: r.config.SchemaVersion,
	}

	err = r.snapshotStore.Put(ctx, tx, &snap)
	if err != nil {
		return fmt.Errorf("failed to put snapshot: %w", err)
	}

	return nil
}

// SaveAppended applies the events from an append result to the provided state
// and persists a snapshot of the updated state at the new stream version.
//
// This helper guarantees that newly appended events are folded into the in-memory
// state before the snapshot is serialized and persisted, preventing the critical
// data loss trap where a snapshot is saved with a new version but stale pre-append state.
func (r *Repository[T]) SaveAppended(
	ctx context.Context,
	tx pgx.Tx,
	id string,
	state T,
	result store.AppendResult,
) (T, error) {
	var err error
	for i := range result.Events {
		state, err = r.config.Apply(state, result.Events[i])
		if err != nil {
			return state, fmt.Errorf("failed to apply appended event v%d: %w", result.Events[i].StreamVersion, err)
		}
	}

	if len(result.Events) == 0 {
		return state, nil
	}

	err = r.Save(ctx, tx, id, result.ToVersion(), state)
	if err != nil {
		return state, err
	}
	return state, nil
}

// HandlerFunc represents a domain command handler that takes the current aggregate state
// and stream version, and returns new events to append to the stream.
type HandlerFunc[T any] func(state T, version int64) ([]store.Event, error)

func (r *Repository[T]) prepareEvents(id string, events []store.Event) error {
	for i := range events {
		if events[i].StreamType == "" {
			events[i].StreamType = r.config.StreamType
		} else if events[i].StreamType != r.config.StreamType {
			return fmt.Errorf("event %d stream type %q does not match repository stream type %q", i, events[i].StreamType, r.config.StreamType)
		}
		if events[i].StreamID == "" {
			events[i].StreamID = id
		} else if events[i].StreamID != id {
			return fmt.Errorf("event %d stream id %q does not match target id %q", i, events[i].StreamID, id)
		}
		if events[i].EventID == uuid.Nil {
			events[i].EventID = uuid.New()
		}
		if events[i].CreatedAt.IsZero() {
			events[i].CreatedAt = time.Now()
		}
	}
	return nil
}

// Execute coordinates the complete load-handle-append-snapshot cycle in a single transaction:
// 1. Loads the latest stream state and metadata via Load.
// 2. Invokes the handler function with the current state and version.
// 3. If handler returns no events and no error, returns early without appending.
// 4. Appends the events to the event store using store.Exact(version) (or store.NoStream for v0).
// 5. Folds newly appended events into the state using Apply.
// 6. Evaluates the snapshot policy and, if triggered, persists the updated state snapshot.
// 7. Returns the final Result[T] and store.AppendResult.
func (r *Repository[T]) Execute(
	ctx context.Context,
	tx pgx.Tx,
	id string,
	eventStore store.EventStore,
	handler HandlerFunc[T],
	policy Policy,
) (Result[T], store.AppendResult, error) {
	if eventStore == nil {
		return Result[T]{}, store.AppendResult{}, fmt.Errorf("event store cannot be nil")
	}
	if handler == nil {
		return Result[T]{}, store.AppendResult{}, fmt.Errorf("handler cannot be nil")
	}

	loadRes, err := r.Load(ctx, tx, id)
	if err != nil {
		return Result[T]{}, store.AppendResult{}, fmt.Errorf("failed to load stream: %w", err)
	}

	events, err := handler(loadRes.State, loadRes.StreamVersion)
	if err != nil {
		return Result[T]{}, store.AppendResult{}, fmt.Errorf("handler error: %w", err)
	}

	if len(events) == 0 {
		return loadRes, store.AppendResult{}, nil
	}

	if prepErr := r.prepareEvents(id, events); prepErr != nil {
		return Result[T]{}, store.AppendResult{}, prepErr
	}

	var expectedVersion store.ExpectedVersion
	if loadRes.StreamVersion == 0 {
		expectedVersion = store.NoStream()
	} else {
		expectedVersion = store.Exact(loadRes.StreamVersion)
	}

	appendRes, err := eventStore.Append(ctx, tx, expectedVersion, events)
	if err != nil {
		return Result[T]{}, store.AppendResult{}, fmt.Errorf("failed to append events: %w", err)
	}

	currentState := loadRes.State
	for i := range appendRes.Events {
		currentState, err = r.config.Apply(currentState, appendRes.Events[i])
		if err != nil {
			return Result[T]{}, appendRes, fmt.Errorf("failed to apply appended event v%d: %w", appendRes.Events[i].StreamVersion, err)
		}
	}

	finalVersion := appendRes.ToVersion()
	res := Result[T]{
		State:           currentState,
		StreamVersion:   finalVersion,
		SnapshotVersion: loadRes.SnapshotVersion,
		SnapshotHit:     loadRes.SnapshotHit,
		SchemaVersion:   r.config.SchemaVersion,
		EventsReplayed:  loadRes.EventsReplayed + len(events),
	}

	if loadRes.ShouldSnapshot(policy, int64(len(events))) {
		err = r.Save(ctx, tx, id, finalVersion, currentState)
		if err != nil {
			return Result[T]{}, appendRes, fmt.Errorf("failed to save snapshot: %w", err)
		}
		res.SnapshotVersion = finalVersion
		res.SnapshotHit = true
		res.EventsReplayed = 0
	}

	return res, appendRes, nil
}
