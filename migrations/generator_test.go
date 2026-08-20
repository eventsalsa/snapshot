package migrations_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/eventsalsa/snapshot/migrations"
)

func TestDefaultConfig(t *testing.T) {
	cfg := migrations.DefaultConfig()
	if cfg.SnapshotsTable != "snapshots" {
		t.Errorf("expected default table 'snapshots', got %s", cfg.SnapshotsTable)
	}
	if cfg.OutputFolder != "migrations" {
		t.Errorf("expected default folder 'migrations', got %s", cfg.OutputFolder)
	}
	if !strings.HasSuffix(cfg.OutputFilename, "_init_snapshots.sql") {
		t.Errorf("expected filename ending in '_init_snapshots.sql', got %s", cfg.OutputFilename)
	}
}

func TestGeneratePostgres_Default(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := migrations.Config{
		OutputFolder:   tmpDir,
		OutputFilename: "init.sql",
		SnapshotsTable: "snapshots",
	}

	err := migrations.GeneratePostgres(&cfg)
	if err != nil {
		t.Fatalf("GeneratePostgres failed: %v", err)
	}

	content, err := os.ReadFile(filepath.Join(tmpDir, "init.sql"))
	if err != nil {
		t.Fatalf("failed to read generated file: %v", err)
	}
	sql := string(content)

	expectedSnippets := []string{
		"-- Snapshots DDL Migration",
		"CREATE TABLE IF NOT EXISTS snapshots (",
		"stream_type TEXT NOT NULL,",
		"stream_id TEXT NOT NULL,",
		"stream_version BIGINT NOT NULL,",
		"schema_version INT NOT NULL,",
		"payload BYTEA NOT NULL,",
		"PRIMARY KEY (stream_type, stream_id)",
		"CREATE INDEX IF NOT EXISTS idx_snapshots_schema_version",
		"ON snapshots (schema_version);",
	}

	for _, snippet := range expectedSnippets {
		if !strings.Contains(sql, snippet) {
			t.Errorf("expected SQL to contain %q, but got:\n%s", snippet, sql)
		}
	}
}

func TestGeneratePostgres_CustomTable(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := migrations.Config{
		OutputFolder:   tmpDir,
		OutputFilename: "custom.sql",
		SnapshotsTable: "custom_snapshots",
	}

	err := migrations.GeneratePostgres(&cfg)
	if err != nil {
		t.Fatalf("GeneratePostgres failed: %v", err)
	}

	content, err := os.ReadFile(filepath.Join(tmpDir, "custom.sql"))
	if err != nil {
		t.Fatalf("failed to read generated file: %v", err)
	}
	sql := string(content)

	if !strings.Contains(sql, "CREATE TABLE IF NOT EXISTS custom_snapshots (") {
		t.Errorf("expected custom table name in DDL")
	}
	if !strings.Contains(sql, "idx_custom_snapshots_schema_version") {
		t.Errorf("expected custom index name in DDL")
	}
}

func TestEmbeddedFS(t *testing.T) {
	entries, err := migrations.FS.ReadDir(".")
	if err != nil {
		t.Fatalf("failed to read embedded FS dir: %v", err)
	}
	if len(entries) == 0 {
		t.Fatal("expected at least one embedded sql file")
	}

	content, err := migrations.FS.ReadFile(entries[0].Name())
	if err != nil {
		t.Fatalf("failed to read embedded file %s: %v", entries[0].Name(), err)
	}

	sql := string(content)
	if !strings.Contains(sql, "CREATE TABLE IF NOT EXISTS snapshots (") {
		t.Errorf("expected embedded migration to create 'snapshots' table")
	}
	if !strings.Contains(sql, "stream_type TEXT NOT NULL") {
		t.Errorf("expected embedded migration to use stream_type")
	}
	if strings.Contains(sql, "aggregate") {
		t.Errorf("embedded migration should not contain 'aggregate', got:\n%s", sql)
	}
}
