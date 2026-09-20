package snapshot

import (
	"context"
	"fmt"
	"time"

	"github.com/eventsalsa/store"
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

// Policy evaluates whether a snapshot should be persisted following an append operation.
// It receives the stream version at load time, the snapshot version at load time (0 if miss),
// and the number of events appended to the stream.
type Policy func(streamVersionAtLoad, snapshotVersionAtLoad int64, appendedCount int64) bool

// EveryNEvents returns a policy that triggers a snapshot whenever the number of events
// since the last snapshot (including newly appended events) is greater than or equal to n.
func EveryNEvents(n int64) Policy {
	return func(streamVersionAtLoad, snapshotVersionAtLoad int64, appendedCount int64) bool {
		if n <= 0 {
			return false
		}
		return (streamVersionAtLoad-snapshotVersionAtLoad)+appendedCount >= n
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

// RepositoryConfig contains configuration for the generic Repository.
type RepositoryConfig[T any] struct {
	// Logger provides structured logging for repository events, warnings, and fallbacks.
	Logger store.Logger

	// Initializer creates a blank state for a new stream with the given id.
	Initializer func(id string) T

	// Apply folds an event from the event store into state T.
	// Tip: To keep domain aggregates decoupled from store.PersistedEvent and auto-generate
	// type-safe event mapping code, use github.com/eventsalsa/store/cmd/eventmap-gen.
	Apply func(state T, event store.PersistedEvent) (T, error)

	// Marshal serializes state T into bytes for snapshot persistence.
	Marshal func(state T) ([]byte, error)

	// Unmarshal deserializes raw snapshot bytes into state T.
	// It receives the target streamID to allow validating that the deserialized state
	// belongs to the requested stream.
	Unmarshal func(streamID string, data []byte) (T, error)

	// Version optionally extracts the internal stream version from state T.
	// When provided, Save verifies that Version(state) matches the target version.
	Version func(state T) int64

	// OnSnapshotRejected is an optional hook called when a snapshot cannot be used
	// (e.g., corrupt payload, missing upcaster, schema mismatch, or version absent in log).
	OnSnapshotRejected func(ctx context.Context, streamID string, reason string, err error)

	// Upcasters provides sequential payload transformations across schema versions (e.g. 1 -> 2).
	Upcasters map[int]Upcaster

	// StreamType identifies the partition/category of the stream in the event store.
	// Note: If StreamType does not match the type used when events were written,
	// Load will find no events or snapshots and will return the initialized state at version 0
	// without returning an error (empty stream semantics).
	StreamType string

	// SchemaVersion is the current snapshot schema version (must be > 0).
	SchemaVersion int

	// FailOnCorruptSnapshot determines whether snapshot unmarshal/upcast errors fail fast
	// (if true) or gracefully fall back to replaying all events from version 1 (if false, default).
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
	for v, u := range config.Upcasters {
		if v <= 0 {
			return nil, fmt.Errorf("upcaster source schema version must be >= 1 (got %d)", v)
		}
		if v >= config.SchemaVersion {
			return nil, fmt.Errorf("upcaster source schema version %d cannot be >= repository schema version %d", v, config.SchemaVersion)
		}
		if u == nil {
			return nil, fmt.Errorf("nil upcaster configured for schema version %d", v)
		}
	}
	return &Repository[T]{
		reader:        reader,
		snapshotStore: snapshotStore,
		config:        config,
	}, nil
}

// Load rehydrates the stream state up to its current version.
// First, it attempts to load the latest snapshot <= current SchemaVersion. If a valid
// snapshot is found matching or upcastable to the configured SchemaVersion, it
// deserializes the state and reads newer delta events starting from version S + 1.
// Otherwise, it initializes a new state and reads all events from the beginning.
//
// Empty stream semantics:
// If the stream does not exist in the event store, or if the configured StreamType
// does not match what events were written under, Load returns Initializer(id) at
// StreamVersion 0 with err == nil. Callers should ensure StreamType matches between
// writers and the repository.
func (r *Repository[T]) Load(ctx context.Context, tx pgx.Tx, id string) (res Result[T], err error) {
	snap, err := r.snapshotStore.Get(ctx, tx, r.config.StreamType, id, r.config.SchemaVersion)
	if err != nil {
		return res, fmt.Errorf("failed to load snapshot: %w", err)
	}

	var fromVersion *int64
	var initialized bool
	var version int64

	if snap.StreamVersion > 0 {
		state, ok, upcasted, snapErr := r.resolveSnapshot(ctx, id, &snap)
		if snapErr != nil {
			return res, snapErr
		}
		if ok {
			res.State = state
			res.SnapshotHit = true
			res.SnapshotVersion = snap.StreamVersion
			res.SnapshotSchemaVersion = snap.SchemaVersion
			res.Upcasted = upcasted
			version = snap.StreamVersion
			nextVersion := snap.StreamVersion + 1
			fromVersion = &nextVersion
			initialized = true
		}
	}

	if !initialized {
		res.State = r.config.Initializer(id)
		res.SnapshotSchemaVersion = 0
		res.Upcasted = false
		version = 0
		fromVersion = nil
	}

	// Read newer events
	stream, err := r.reader.ReadStream(ctx, tx, r.config.StreamType, id, fromVersion, nil)
	if err != nil {
		return res, fmt.Errorf("failed to read stream: %w", err)
	}

	if initialized && len(stream.Events) == 0 {
		fullStream, valid, verifyErr := r.verifySnapshotInStream(ctx, tx, id, &snap)
		if verifyErr != nil {
			return res, verifyErr
		}
		if !valid {
			res.State = r.config.Initializer(id)
			res.SnapshotHit = false
			res.SnapshotVersion = 0
			res.SnapshotSchemaVersion = 0
			res.Upcasted = false
			version = 0
			stream = fullStream
		}
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

func (r *Repository[T]) resolveSnapshot(ctx context.Context, id string, snap *Snapshot) (state T, ok, upcasted bool, err error) {
	var payload []byte
	var zero T

	switch {
	case snap.SchemaVersion == r.config.SchemaVersion:
		payload = snap.Payload
	case snap.SchemaVersion < r.config.SchemaVersion:
		var chainOK bool
		payload, chainOK, err = r.upcastPayload(ctx, id, snap)
		if err != nil {
			return zero, false, false, err
		}
		if !chainOK {
			return zero, false, false, nil
		}
		upcasted = true
	default:
		if r.config.OnSnapshotRejected != nil {
			r.config.OnSnapshotRejected(ctx, id, "snapshot schema version is newer than configured repository schema version", nil)
		}
		return zero, false, false, nil
	}

	state, err = r.config.Unmarshal(id, payload)
	if err != nil {
		if r.config.FailOnCorruptSnapshot {
			return zero, false, false, fmt.Errorf("failed to unmarshal snapshot: %w", err)
		}
		if r.config.Logger != nil {
			r.config.Logger.Error(ctx, "failed to unmarshal snapshot payload; falling back to full stream replay",
				"stream_type", r.config.StreamType,
				"stream_id", id,
				"stream_version", snap.StreamVersion,
				"schema_version", snap.SchemaVersion,
				"error", err,
			)
		}
		if r.config.OnSnapshotRejected != nil {
			r.config.OnSnapshotRejected(ctx, id, "failed to unmarshal snapshot payload", err)
		}
		return zero, false, false, nil
	}

	return state, true, upcasted, nil
}

func (r *Repository[T]) upcastPayload(ctx context.Context, id string, snap *Snapshot) (payload []byte, complete bool, err error) {
	currentPayload := snap.Payload
	currentVer := snap.SchemaVersion

	for currentVer < r.config.SchemaVersion {
		upcaster, ok := r.config.Upcasters[currentVer]
		if !ok {
			if r.config.Logger != nil {
				r.config.Logger.Debug(ctx, "no upcaster found in chain; falling back to full stream replay",
					"stream_type", r.config.StreamType,
					"stream_id", id,
					"current_schema_version", currentVer,
					"target_schema_version", r.config.SchemaVersion,
				)
			}
			if r.config.OnSnapshotRejected != nil {
				r.config.OnSnapshotRejected(ctx, id, fmt.Sprintf("no upcaster found for schema version %d in chain", currentVer), nil)
			}
			return nil, false, nil
		}

		var err error
		currentPayload, err = upcaster(currentVer, currentPayload)
		if err != nil {
			if r.config.FailOnCorruptSnapshot {
				return nil, false, fmt.Errorf("failed to upcast snapshot from schema version %d: %w", currentVer, err)
			}
			if r.config.Logger != nil {
				r.config.Logger.Error(ctx, "failed to upcast snapshot payload; falling back to full stream replay",
					"stream_type", r.config.StreamType,
					"stream_id", id,
					"from_schema_version", currentVer,
					"to_schema_version", currentVer+1,
					"error", err,
				)
			}
			if r.config.OnSnapshotRejected != nil {
				r.config.OnSnapshotRejected(ctx, id, fmt.Sprintf("failed to upcast snapshot from schema version %d", currentVer), err)
			}
			return nil, false, nil
		}
		currentVer++
	}

	return currentPayload, true, nil
}

// verifySnapshotInStream validates that the snapshot's StreamVersion actually exists in the event log.
//
// In an append-only, contiguous event log, if delta events exist (len(stream.Events) > 0),
// the existence of version S is guaranteed without needing an extra read.
//
// When the snapshot is at head (len(stream.Events) == 0), the delta query (from=S+1, to=nil) returns 0 events.
// An empty delta cannot distinguish between a valid snapshot at the stream head versus an orphan or
// phantom snapshot written beyond the head of the log.
//
// Therefore, verifySnapshotInStream performs a single-event read (from=S, to=S) to confirm that
// version S genuinely exists in the event store. While this costs one extra event-log read in the
// snapshot-at-head idle case, it prevents trusting snapshots beyond the head of the log.
//
// Note on physical log truncation: This check assumes the event log is contiguous from the oldest
// snapshot onwards. If event streams are physically truncated or pruned, snapshots older than the
// retention horizon should be pruned concurrently to prevent verification failure.
func (r *Repository[T]) verifySnapshotInStream(ctx context.Context, tx pgx.Tx, id string, snap *Snapshot) (store.Stream, bool, error) {
	headCheck, err := r.reader.ReadStream(ctx, tx, r.config.StreamType, id, &snap.StreamVersion, &snap.StreamVersion)
	if err != nil {
		return store.Stream{}, false, fmt.Errorf("failed to verify snapshot version in stream: %w", err)
	}
	if !headCheck.IsEmpty() {
		return store.Stream{}, true, nil
	}

	if r.config.Logger != nil {
		r.config.Logger.Error(ctx, "snapshot version not found in event log; falling back to full stream replay",
			"stream_type", r.config.StreamType,
			"stream_id", id,
			"snapshot_version", snap.StreamVersion,
			"schema_version", snap.SchemaVersion,
		)
	}
	if r.config.OnSnapshotRejected != nil {
		r.config.OnSnapshotRejected(ctx, id, fmt.Sprintf("snapshot version %d not found in event log", snap.StreamVersion), nil)
	}

	fullStream, err := r.reader.ReadStream(ctx, tx, r.config.StreamType, id, nil, nil)
	if err != nil {
		return store.Stream{}, false, fmt.Errorf("failed to replay full stream: %w", err)
	}
	return fullStream, false, nil
}

// Save persists a snapshot of the current stream state at the specified version.
func (r *Repository[T]) Save(ctx context.Context, tx pgx.Tx, id string, version int64, state T) error {
	if version <= 0 {
		return fmt.Errorf("invalid stream version: %d", version)
	}

	if r.config.Version != nil {
		if stateVer := r.config.Version(state); stateVer != version {
			return fmt.Errorf("state version %d does not match save version %d", stateVer, version)
		}
	}

	check, err := r.reader.ReadStream(ctx, tx, r.config.StreamType, id, &version, &version)
	if err != nil {
		return fmt.Errorf("failed to verify stream version: %w", err)
	}
	if check.IsEmpty() {
		return fmt.Errorf("cannot save snapshot at version %d: stream version does not exist in event log", version)
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
