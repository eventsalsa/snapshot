-- Aggregate Snapshots DDL Migration
-- Generated: 2026-06-21T08:49:51+02:00

-- Table to store aggregate state snapshots
CREATE TABLE IF NOT EXISTS aggregate_snapshots (
    aggregate_type TEXT NOT NULL,
    aggregate_id TEXT NOT NULL,
    aggregate_version BIGINT NOT NULL,
    schema_version INT NOT NULL,
    payload BYTEA NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    PRIMARY KEY (aggregate_type, aggregate_id)
);

-- Index for schema version analysis/observability
CREATE INDEX IF NOT EXISTS idx_aggregate_snapshots_schema_version 
    ON aggregate_snapshots (schema_version);
