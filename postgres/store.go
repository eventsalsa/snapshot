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
		SnapshotsTable: "aggregate_snapshots",
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
// It retrieves the latest snapshot for the given aggregate.
// Returns a zero Snapshot and nil if no snapshot exists.
func (s *Store) Get(ctx context.Context, tx pgx.Tx, aggregateType, aggregateID string) (snapshot.Snapshot, error) {
	if s.config.Logger != nil {
		s.config.Logger.Debug(ctx, "fetching snapshot",
			"aggregate_type", aggregateType,
			"aggregate_id", aggregateID)
	}

	//nolint:gosec // G201: table name from trusted config, not user input
	query := fmt.Sprintf(`
		SELECT aggregate_version, schema_version, payload, created_at 
		FROM %s 
		WHERE aggregate_type = $1 AND aggregate_id = $2
	`, s.config.SnapshotsTable)

	var snap snapshot.Snapshot
	snap.AggregateType = aggregateType
	snap.AggregateID = aggregateID

	err := tx.QueryRow(ctx, query, aggregateType, aggregateID).Scan(
		&snap.AggregateVersion,
		&snap.SchemaVersion,
		&snap.Payload,
		&snap.CreatedAt,
	)

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			if s.config.Logger != nil {
				s.config.Logger.Debug(ctx, "snapshot not found",
					"aggregate_type", aggregateType,
					"aggregate_id", aggregateID)
			}
			return snapshot.Snapshot{}, nil
		}
		return snapshot.Snapshot{}, fmt.Errorf("failed to query snapshot: %w", err)
	}

	if s.config.Logger != nil {
		s.config.Logger.Debug(ctx, "snapshot retrieved successfully",
			"aggregate_type", aggregateType,
			"aggregate_id", aggregateID,
			"aggregate_version", snap.AggregateVersion,
			"schema_version", snap.SchemaVersion)
	}

	return snap, nil
}

// Put implements snapshot.Store.
// It saves (inserts or updates) a snapshot for the aggregate.
// Overwrites the existing snapshot if it already exists for the (type, id) pair.
func (s *Store) Put(ctx context.Context, tx pgx.Tx, snap *snapshot.Snapshot) error {
	if s.config.Logger != nil {
		s.config.Logger.Debug(ctx, "saving snapshot",
			"aggregate_type", snap.AggregateType,
			"aggregate_id", snap.AggregateID,
			"aggregate_version", snap.AggregateVersion,
			"schema_version", snap.SchemaVersion)
	}

	//nolint:gosec // G201: table name from trusted config, not user input
	query := fmt.Sprintf(`
		INSERT INTO %s (aggregate_type, aggregate_id, aggregate_version, schema_version, payload, created_at)
		VALUES ($1, $2, $3, $4, $5, NOW())
		ON CONFLICT (aggregate_type, aggregate_id)
		DO UPDATE SET 
			aggregate_version = EXCLUDED.aggregate_version,
			schema_version = EXCLUDED.schema_version,
			payload = EXCLUDED.payload,
			created_at = NOW()
	`, s.config.SnapshotsTable)

	_, err := tx.Exec(ctx, query,
		snap.AggregateType,
		snap.AggregateID,
		snap.AggregateVersion,
		snap.SchemaVersion,
		snap.Payload,
	)
	if err != nil {
		return fmt.Errorf("failed to upsert snapshot: %w", err)
	}

	if s.config.Logger != nil {
		s.config.Logger.Debug(ctx, "snapshot saved successfully",
			"aggregate_type", snap.AggregateType,
			"aggregate_id", snap.AggregateID,
			"aggregate_version", snap.AggregateVersion)
	}

	return nil
}
