package postgres_test

import (
	"context"
	"testing"

	snapshotpostgres "github.com/eventsalsa/snapshot/postgres"
)

type testLogger struct {
	debugCalls int
}

func (l *testLogger) Debug(_ context.Context, _ string, _ ...any) {
	l.debugCalls++
}

func (l *testLogger) Info(_ context.Context, _ string, _ ...any) {}

func (l *testLogger) Warn(_ context.Context, _ string, _ ...any) {}

func (l *testLogger) Error(_ context.Context, _ string, _ ...any) {}

func TestStoreConfig(t *testing.T) {
	t.Run("default configuration", func(t *testing.T) {
		cfg := snapshotpostgres.DefaultStoreConfig()
		if cfg.SnapshotsTable != "snapshots" {
			t.Errorf("expected default table 'snapshots', got %q", cfg.SnapshotsTable)
		}
		if cfg.Logger != nil {
			t.Errorf("expected default logger to be nil, got %v", cfg.Logger)
		}
	})

	t.Run("custom options", func(t *testing.T) {
		logger := &testLogger{}
		cfg := snapshotpostgres.NewStoreConfig(
			snapshotpostgres.WithSnapshotsTable("custom_snapshots"),
			snapshotpostgres.WithLogger(logger),
		)

		if cfg.SnapshotsTable != "custom_snapshots" {
			t.Errorf("expected table 'custom_snapshots', got %q", cfg.SnapshotsTable)
		}
		if cfg.Logger != logger {
			t.Errorf("expected custom logger to be set")
		}

		store := snapshotpostgres.NewStore(cfg)
		if store == nil {
			t.Fatal("expected non-nil Store")
		}
	})
}

func TestStore_Get_Validation(t *testing.T) {
	store := snapshotpostgres.NewStore(snapshotpostgres.DefaultStoreConfig())
	ctx := context.Background()

	t.Run("zero maxSchemaVersion returns error", func(t *testing.T) {
		_, err := store.Get(ctx, nil, "User", "u-1", 0)
		if err == nil || err.Error() != "maxSchemaVersion must be > 0 (got 0)" {
			t.Fatalf("expected maxSchemaVersion error, got: %v", err)
		}
	})

	t.Run("negative maxSchemaVersion returns error", func(t *testing.T) {
		_, err := store.Get(ctx, nil, "User", "u-1", -5)
		if err == nil || err.Error() != "maxSchemaVersion must be > 0 (got -5)" {
			t.Fatalf("expected maxSchemaVersion error, got: %v", err)
		}
	})
}
