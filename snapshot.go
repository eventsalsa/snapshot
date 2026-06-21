package snapshot

import (
	"context"
	"fmt"
	"time"

	"github.com/eventsalsa/store"
	"github.com/jackc/pgx/v5"
)

// Snapshot represents a persisted aggregate state snapshot at a specific version.
type Snapshot struct {
	CreatedAt        time.Time
	AggregateType    string
	AggregateID      string
	Payload          []byte
	AggregateVersion int64
	SchemaVersion    int
}

// Store defines the low-level persistence interface for snapshots.
type Store interface {
	// Get retrieves the latest snapshot for the given aggregate.
	// Returns a zero Snapshot and nil if no snapshot exists.
	Get(ctx context.Context, tx pgx.Tx, aggregateType, aggregateID string) (Snapshot, error)

	// Put saves (inserts or updates) a snapshot for the aggregate.
	// Overwrites the existing snapshot if it already exists for the (type, id) pair.
	Put(ctx context.Context, tx pgx.Tx, snapshot *Snapshot) error
}

// RepositoryConfig contains configuration for the generic Repository.
type RepositoryConfig[T any] struct {
	// Initializer instantiates a new, empty aggregate state for a given ID.
	Initializer func(id string) T

	// Apply applies a single persisted event to update the aggregate state.
	// It must return the updated state (which can be the mutated input state).
	Apply func(state T, event store.PersistedEvent) (T, error)

	// Marshal serializes the aggregate state into raw bytes for snapshot storage.
	Marshal func(state T) ([]byte, error)

	// Unmarshal deserializes raw snapshot bytes back into the aggregate state.
	Unmarshal func(data []byte) (T, error)

	// AggregateType is the string identifier for the aggregate type (e.g. "User").
	AggregateType string

	// SchemaVersion is the current version of the aggregate struct/logic schema.
	// If a stored snapshot has a different SchemaVersion, it is ignored
	// and the aggregate is rehydrated from version 1 of the event log.
	SchemaVersion int
}

// Repository orchestrates the loading and saving of aggregate states
// using snapshots and events.
type Repository[T any] struct {
	reader        store.AggregateStreamReader
	snapshotStore Store
	config        RepositoryConfig[T]
}

// NewRepository creates a new generic Repository for an aggregate type.
func NewRepository[T any](
	reader store.AggregateStreamReader,
	snapshotStore Store,
	config RepositoryConfig[T],
) (*Repository[T], error) {
	if reader == nil {
		return nil, fmt.Errorf("event store reader cannot be nil")
	}
	if snapshotStore == nil {
		return nil, fmt.Errorf("snapshot store cannot be nil")
	}
	if config.AggregateType == "" {
		return nil, fmt.Errorf("aggregate type cannot be empty")
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
	return &Repository[T]{
		reader:        reader,
		snapshotStore: snapshotStore,
		config:        config,
	}, nil
}

// Load rehydrates the aggregate state up to its current version.
// First, it attempts to load the latest snapshot. If a valid snapshot is found
// matching the configured SchemaVersion, it deserializes the state and reads
// newer events starting from version S + 1. Otherwise, it initializes a new state
// and reads all events from the beginning.
func (r *Repository[T]) Load(ctx context.Context, tx pgx.Tx, id string) (state T, version int64, err error) {
	var snap Snapshot
	snap, err = r.snapshotStore.Get(ctx, tx, r.config.AggregateType, id)
	if err != nil {
		return state, 0, fmt.Errorf("failed to load snapshot: %w", err)
	}

	var fromVersion *int64
	var initialized bool

	// Check if we have a valid snapshot matching the current schema version
	if snap.AggregateVersion > 0 && snap.SchemaVersion == r.config.SchemaVersion {
		state, err = r.config.Unmarshal(snap.Payload)
		if err != nil {
			// If unmarshaling fails, return the error to avoid corrupt or incomplete states.
			return state, 0, fmt.Errorf("failed to unmarshal snapshot: %w", err)
		}
		version = snap.AggregateVersion
		nextVersion := snap.AggregateVersion + 1
		fromVersion = &nextVersion
		initialized = true
	}

	if !initialized {
		state = r.config.Initializer(id)
		version = 0
		fromVersion = nil
	}

	// Read newer events
	stream, err := r.reader.ReadAggregateStream(ctx, tx, r.config.AggregateType, id, fromVersion, nil)
	if err != nil {
		return state, 0, fmt.Errorf("failed to read aggregate stream: %w", err)
	}

	for i := range stream.Events {
		state, err = r.config.Apply(state, stream.Events[i])
		if err != nil {
			return state, 0, fmt.Errorf("failed to apply event v%d: %w", stream.Events[i].AggregateVersion, err)
		}
		version = stream.Events[i].AggregateVersion
	}

	return state, version, nil
}

// Save persists a snapshot of the current aggregate state at the specified version.
func (r *Repository[T]) Save(ctx context.Context, tx pgx.Tx, id string, version int64, state T) error {
	if version <= 0 {
		return fmt.Errorf("invalid aggregate version: %d", version)
	}

	payload, err := r.config.Marshal(state)
	if err != nil {
		return fmt.Errorf("failed to marshal snapshot: %w", err)
	}

	snap := Snapshot{
		Payload:          payload,
		AggregateType:    r.config.AggregateType,
		AggregateID:      id,
		AggregateVersion: version,
		SchemaVersion:    r.config.SchemaVersion,
	}

	err = r.snapshotStore.Put(ctx, tx, &snap)
	if err != nil {
		return fmt.Errorf("failed to put snapshot: %w", err)
	}

	return nil
}
