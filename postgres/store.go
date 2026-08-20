package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/eventsalsa/store"
	"github.com/jackc/pgx/v5"

	"github.com/eventsalsa/snapshot"
)

// StoreConfig contains configuration for the PostgreSQL snapshot store.
// Configuration is immutable after construction.
type StoreConfig struct {
	// Logger is an optional logger for observability.
	// If nil, logging is disabled (zero overhead).
	Logger store.Logger

	// SnapshotsTable is the name of the snapshots table.
	SnapshotsTable string
}

// DefaultStoreConfig returns the default configuration.
func DefaultStoreConfig() *StoreConfig {
	return &StoreConfig{
		SnapshotsTable: "snapshots",
		Logger:         nil, // No logging by default
	}
}

// StoreOption is a functional option for configuring a Store.
type StoreOption func(*StoreConfig)

// WithLogger sets a logger for the store.
func WithLogger(logger store.Logger) StoreOption {
	return func(c *StoreConfig) {
		c.Logger = logger
	}
}

// WithSnapshotsTable sets a custom snapshots table name.
func WithSnapshotsTable(tableName string) StoreOption {
	return func(c *StoreConfig) {
		c.SnapshotsTable = tableName
	}
}

// NewStoreConfig creates a new store configuration with functional options.
// It starts with the default configuration and applies the given options.
func NewStoreConfig(opts ...StoreOption) *StoreConfig {
	config := DefaultStoreConfig()
	for _, opt := range opts {
		opt(config)
	}
	return config
}

// Store is a PostgreSQL-backed snapshot store implementation.
type Store struct {
	config StoreConfig
}

// NewStore creates a new PostgreSQL snapshot store with the given configuration.
func NewStore(config *StoreConfig) *Store {
	return &Store{
		config: *config,
	}
}

// Get implements snapshot.Store.
// It retrieves the latest snapshot for the given stream.
// Returns a zero Snapshot and nil if no snapshot exists.
func (s *Store) Get(ctx context.Context, tx pgx.Tx, streamType, streamID string) (snapshot.Snapshot, error) {
	if s.config.Logger != nil {
		s.config.Logger.Debug(ctx, "fetching snapshot",
			"stream_type", streamType,
			"stream_id", streamID)
	}

	//nolint:gosec // G201: table name from trusted config, not user input
	query := fmt.Sprintf(`
		SELECT stream_version, schema_version, payload, created_at 
		FROM %s 
		WHERE stream_type = $1 AND stream_id = $2
	`, s.config.SnapshotsTable)

	var snap snapshot.Snapshot
	snap.StreamType = streamType
	snap.StreamID = streamID

	err := tx.QueryRow(ctx, query, streamType, streamID).Scan(
		&snap.StreamVersion,
		&snap.SchemaVersion,
		&snap.Payload,
		&snap.CreatedAt,
	)

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			if s.config.Logger != nil {
				s.config.Logger.Debug(ctx, "snapshot not found",
					"stream_type", streamType,
					"stream_id", streamID)
			}
			return snapshot.Snapshot{}, nil
		}
		return snapshot.Snapshot{}, fmt.Errorf("failed to query snapshot: %w", err)
	}

	if s.config.Logger != nil {
		s.config.Logger.Debug(ctx, "snapshot retrieved successfully",
			"stream_type", streamType,
			"stream_id", streamID,
			"stream_version", snap.StreamVersion,
			"schema_version", snap.SchemaVersion)
	}

	return snap, nil
}

// Put implements snapshot.Store.
// It saves (inserts or updates) a snapshot for the stream.
// Overwrites the existing snapshot if it already exists for the (type, id) pair.
func (s *Store) Put(ctx context.Context, tx pgx.Tx, snap *snapshot.Snapshot) error {
	if s.config.Logger != nil {
		s.config.Logger.Debug(ctx, "saving snapshot",
			"stream_type", snap.StreamType,
			"stream_id", snap.StreamID,
			"stream_version", snap.StreamVersion,
			"schema_version", snap.SchemaVersion)
	}

	//nolint:gosec // G201: table name from trusted config, not user input
	query := fmt.Sprintf(`
		INSERT INTO %s (stream_type, stream_id, stream_version, schema_version, payload, created_at)
		VALUES ($1, $2, $3, $4, $5, NOW())
		ON CONFLICT (stream_type, stream_id)
		DO UPDATE SET 
			stream_version = EXCLUDED.stream_version,
			schema_version = EXCLUDED.schema_version,
			payload = EXCLUDED.payload,
			created_at = NOW()
	`, s.config.SnapshotsTable)

	_, err := tx.Exec(ctx, query,
		snap.StreamType,
		snap.StreamID,
		snap.StreamVersion,
		snap.SchemaVersion,
		snap.Payload,
	)
	if err != nil {
		return fmt.Errorf("failed to upsert snapshot: %w", err)
	}

	if s.config.Logger != nil {
		s.config.Logger.Debug(ctx, "snapshot saved successfully",
			"stream_type", snap.StreamType,
			"stream_id", snap.StreamID,
			"stream_version", snap.StreamVersion)
	}

	return nil
}
