-- Snapshots DDL Migration
-- Generated: 2026-06-21T08:49:51+02:00
--
-- Note: Snapshots contain derived, disposable cache data. This table can be
-- safely truncated or dropped at any time without data loss; missing snapshots
-- will be transparently rehydrated on demand by replaying the event store log.

CREATE TABLE IF NOT EXISTS snapshots (
    stream_type TEXT NOT NULL,
    stream_id TEXT NOT NULL,
    stream_version BIGINT NOT NULL,
    schema_version INT NOT NULL,
    payload BYTEA NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    PRIMARY KEY (stream_type, stream_id, schema_version)
);
