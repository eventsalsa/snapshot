-- Snapshots DDL Migration
-- Generated: 2026-06-21T08:49:51+02:00

-- Table to store stream state snapshots
CREATE TABLE IF NOT EXISTS snapshots (
    stream_type TEXT NOT NULL,
    stream_id TEXT NOT NULL,
    stream_version BIGINT NOT NULL,
    schema_version INT NOT NULL,
    payload BYTEA NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    PRIMARY KEY (stream_type, stream_id)
);

-- Index for schema version analysis/observability
CREATE INDEX IF NOT EXISTS idx_snapshots_schema_version 
    ON snapshots (schema_version);
