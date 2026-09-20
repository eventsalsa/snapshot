package snapshot_test

import (
	"context"
	"encoding/json"

	"github.com/eventsalsa/store"
	"github.com/jackc/pgx/v5"

	"github.com/eventsalsa/snapshot"
)

type userState struct {
	ID    string `json:"id"`
	Name  string `json:"name"`
	Email string `json:"email"`
}

type userEvent struct {
	Name  string `json:"name"`
	Email string `json:"email"`
}

type mockStreamReader struct {
	readStreamFunc func(ctx context.Context, tx pgx.Tx, streamType, streamID string, fromVersion, toVersion *int64) (store.Stream, error)
}

func (m *mockStreamReader) ReadStream(ctx context.Context, tx pgx.Tx, streamType, streamID string, fromVersion, toVersion *int64) (store.Stream, error) {
	if m.readStreamFunc != nil {
		return m.readStreamFunc(ctx, tx, streamType, streamID, fromVersion, toVersion)
	}
	return store.Stream{StreamType: streamType, StreamID: streamID}, nil
}

type mockSnapshotStore struct {
	getFunc func(ctx context.Context, tx pgx.Tx, streamType, streamID string, maxSchemaVersion int) (snapshot.Snapshot, error)
	putFunc func(ctx context.Context, tx pgx.Tx, snap *snapshot.Snapshot) error
}

func (m *mockSnapshotStore) Get(ctx context.Context, tx pgx.Tx, streamType, streamID string, maxSchemaVersion int) (snapshot.Snapshot, error) {
	if m.getFunc != nil {
		return m.getFunc(ctx, tx, streamType, streamID, maxSchemaVersion)
	}
	return snapshot.Snapshot{}, nil
}

func (m *mockSnapshotStore) Put(ctx context.Context, tx pgx.Tx, snap *snapshot.Snapshot) error {
	if m.putFunc != nil {
		return m.putFunc(ctx, tx, snap)
	}
	return nil
}

type mockLogger struct {
	lastErrMsg string
	debugCalls int
	infoCalls  int
	errorCalls int
}

func (m *mockLogger) Debug(_ context.Context, _ string, _ ...any) { m.debugCalls++ }
func (m *mockLogger) Info(_ context.Context, _ string, _ ...any)  { m.infoCalls++ }
func (m *mockLogger) Error(_ context.Context, msg string, _ ...any) {
	m.errorCalls++
	m.lastErrMsg = msg
}

func defaultTestConfig() snapshot.RepositoryConfig[*userState] {
	return snapshot.RepositoryConfig[*userState]{
		StreamType:    "User",
		SchemaVersion: 1,
		Initializer: func(id string) *userState {
			return &userState{ID: id}
		},
		Apply: func(s *userState, e store.PersistedEvent) (*userState, error) {
			var ev userEvent
			if err := json.Unmarshal(e.Payload, &ev); err != nil {
				return nil, err
			}
			if ev.Name != "" {
				s.Name = ev.Name
			}
			if ev.Email != "" {
				s.Email = ev.Email
			}
			return s, nil
		},
		Codec: snapshot.JSON[*userState](),
	}
}
