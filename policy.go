package snapshot

// Policy evaluates whether a snapshot should be persisted following an append operation.
// It receives the stream version at load time, the snapshot version at load time (0 if miss),
// and the number of events appended to the stream.
type Policy func(streamVersionAtLoad, snapshotVersionAtLoad int64, appendedCount int64) bool

// EveryNEvents returns a policy that triggers a snapshot whenever the number of events
// since the last snapshot (including newly appended events) is greater than or equal to n.
func EveryNEvents(n int64) Policy {
	return func(streamVersionAtLoad, snapshotVersionAtLoad int64, appendedCount int64) bool {
		if n <= 0 {
			return false
		}
		return (streamVersionAtLoad-snapshotVersionAtLoad)+appendedCount >= n
	}
}

// Never returns a policy that never triggers a snapshot.
func Never() Policy {
	return func(_, _, _ int64) bool { return false }
}

// ShouldSnapshot evaluates whether the given policy triggers a snapshot after appending appendedCount events.
func (r Result[T]) ShouldSnapshot(policy Policy, appendedCount int64) bool {
	if policy == nil {
		return false
	}
	return policy(r.StreamVersion, r.SnapshotVersion, appendedCount)
}
