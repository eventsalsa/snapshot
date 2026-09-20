package snapshot_test

import (
	"testing"

	"github.com/eventsalsa/snapshot"
)

func TestSnapshotPolicy(t *testing.T) {
	t.Run("EveryNEvents", func(t *testing.T) {
		policy := snapshot.EveryNEvents(100)

		// 1. Not enough events: snapshot at v100, current at v150, 20 new events (total delta 50+20=70 < 100)
		res := snapshot.Result[*userState]{
			StreamVersion:   150,
			SnapshotVersion: 100,
			SnapshotHit:     true,
		}
		if res.ShouldSnapshot(policy, 20) {
			t.Errorf("expected EveryNEvents(100) to return false for 70 events delta")
		}

		// 2. Exactly reaches threshold: delta 50 + 50 appended = 100
		if !res.ShouldSnapshot(policy, 50) {
			t.Errorf("expected EveryNEvents(100) to return true for 100 events delta")
		}

		// 3. Multi-event batch jumps over threshold: delta 50 + 65 appended = 115 >= 100
		if !res.ShouldSnapshot(policy, 65) {
			t.Errorf("expected EveryNEvents(100) to return true for 115 events delta (batch jump)")
		}

		// 4. Direct policy call
		if !policy(150, 100, 65) {
			t.Errorf("expected direct policy call to return true")
		}

		// 5. No snapshot loaded: snapshotVersion=0, streamVersion=80, 25 appended = 105 >= 100
		noSnapRes := snapshot.Result[*userState]{
			StreamVersion:   80,
			SnapshotVersion: 0,
			SnapshotHit:     false,
		}
		if !noSnapRes.ShouldSnapshot(policy, 25) {
			t.Errorf("expected EveryNEvents(100) to return true for 105 events since inception")
		}

		// 6. Invalid n (<= 0) returns false
		zeroPolicy := snapshot.EveryNEvents(0)
		if res.ShouldSnapshot(zeroPolicy, 1000) {
			t.Errorf("expected EveryNEvents(0) to return false")
		}

		// 7. Nil policy returns false
		if res.ShouldSnapshot(nil, 1000) {
			t.Errorf("expected nil policy to return false")
		}
	})

	t.Run("Never", func(t *testing.T) {
		policy := snapshot.Never()
		res := snapshot.Result[*userState]{
			StreamVersion:   1000,
			SnapshotVersion: 0,
		}
		if res.ShouldSnapshot(policy, 1000) {
			t.Errorf("expected Never() to return false")
		}
	})
}
