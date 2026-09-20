package snapshot_test

import (
	"testing"
	"time"

	"github.com/eventsalsa/snapshot"
)

func TestSnapshot_Fields(t *testing.T) {
	now := time.Now()
	snap := snapshot.Snapshot{
		CreatedAt:     now,
		StreamType:    "User",
		StreamID:      "u-1",
		Payload:       []byte(`{"name":"Alice"}`),
		StreamVersion: 5,
		SchemaVersion: 1,
	}

	if snap.StreamType != "User" || snap.StreamID != "u-1" {
		t.Errorf("unexpected stream identifier: %s/%s", snap.StreamType, snap.StreamID)
	}
	if snap.StreamVersion != 5 || snap.SchemaVersion != 1 {
		t.Errorf("unexpected versions: stream=%d, schema=%d", snap.StreamVersion, snap.SchemaVersion)
	}
	if !snap.CreatedAt.Equal(now) {
		t.Errorf("unexpected CreatedAt: %v", snap.CreatedAt)
	}
}

func TestResult_Data(t *testing.T) {
	res := snapshot.Result[string]{
		State:                 "test-data",
		StreamVersion:         10,
		SnapshotVersion:       8,
		EventsReplayed:        2,
		SnapshotSchemaVersion: 1,
		SnapshotHit:           true,
		Upcasted:              false,
	}

	if res.Data() != "test-data" {
		t.Errorf("expected Data() = 'test-data', got %q", res.Data())
	}

	alias := snapshot.LoadResult[string]{
		State: "test-data",
	}
	if alias.Data() != "test-data" {
		t.Errorf("expected alias Data() = 'test-data', got %q", alias.Data())
	}
}
