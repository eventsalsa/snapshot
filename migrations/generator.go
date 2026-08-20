// Package migrations provides SQL migration generation for snapshot infrastructure.
package migrations

import (
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// Config configures migration generation.
type Config struct {
	// OutputFolder is the directory where the migration file will be written.
	OutputFolder string

	// OutputFilename is the name of the migration file.
	OutputFilename string

	// SnapshotsTable is the name of the snapshots table.
	SnapshotsTable string
}

// DefaultConfig returns the default configuration.
func DefaultConfig() Config {
	timestamp := time.Now().Format("20060102150405")
	return Config{
		OutputFolder:   "migrations",
		OutputFilename: fmt.Sprintf("%s_init_snapshots.sql", timestamp),
		SnapshotsTable: "snapshots",
	}
}

// GeneratePostgres generates a PostgreSQL migration file.
func GeneratePostgres(config *Config) error {
	// Ensure output folder exists
	if err := os.MkdirAll(config.OutputFolder, 0o755); err != nil {
		return fmt.Errorf("failed to create output folder: %w", err)
	}

	sql := generatePostgresSQL(config)

	outputPath := filepath.Join(config.OutputFolder, config.OutputFilename)
	if err := os.WriteFile(outputPath, []byte(sql), 0o600); err != nil {
		return fmt.Errorf("failed to write migration file: %w", err)
	}

	return nil
}

func generatePostgresSQL(config *Config) string {
	return fmt.Sprintf(`-- Snapshots DDL Migration
-- Generated: %s

-- Table to store stream state snapshots
CREATE TABLE IF NOT EXISTS %s (
    stream_type TEXT NOT NULL,
    stream_id TEXT NOT NULL,
    stream_version BIGINT NOT NULL,
    schema_version INT NOT NULL,
    payload BYTEA NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    PRIMARY KEY (stream_type, stream_id)
);

-- Index for schema version analysis/observability
CREATE INDEX IF NOT EXISTS idx_%s_schema_version 
    ON %s (schema_version);
`,
		time.Now().Format(time.RFC3339),
		config.SnapshotsTable,
		config.SnapshotsTable, config.SnapshotsTable,
	)
}
