-- Snapshots Compound Primary Key Migration
-- Upgrades snapshots table to compound primary key (stream_type, stream_id, schema_version)
-- to support multi-version rolling deployments without row thrashing.

ALTER TABLE snapshots DROP CONSTRAINT IF EXISTS snapshots_pkey;
ALTER TABLE snapshots ADD PRIMARY KEY (stream_type, stream_id, schema_version);
