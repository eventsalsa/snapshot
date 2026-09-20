package snapshot_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/eventsalsa/store"
	"github.com/jackc/pgx/v5"

	"github.com/eventsalsa/snapshot"
)

func TestJSONCodec(t *testing.T) {
	t.Run("pointer type encode and decode", func(t *testing.T) {
		codec := snapshot.JSON[*userState]()
		original := &userState{ID: "u-123", Name: "Alice", Email: "alice@example.com"}

		data, err := codec.Encode(original)
		if err != nil {
			t.Fatalf("Encode failed: %v", err)
		}

		decoded, err := codec.Decode("u-123", data)
		if err != nil {
			t.Fatalf("Decode failed: %v", err)
		}

		if decoded == nil || decoded.ID != "u-123" || decoded.Name != "Alice" || decoded.Email != "alice@example.com" {
			t.Errorf("unexpected decoded state: %+v", decoded)
		}
	})

	t.Run("value type encode and decode", func(t *testing.T) {
		codec := snapshot.JSON[userState]()
		original := userState{ID: "u-456", Name: "Bob", Email: "bob@example.com"}

		data, err := codec.Encode(original)
		if err != nil {
			t.Fatalf("Encode failed: %v", err)
		}

		decoded, err := codec.Decode("u-456", data)
		if err != nil {
			t.Fatalf("Decode failed: %v", err)
		}

		if decoded.ID != "u-456" || decoded.Name != "Bob" || decoded.Email != "bob@example.com" {
			t.Errorf("unexpected decoded state: %+v", decoded)
		}
	})

	t.Run("corrupt payload decode error", func(t *testing.T) {
		codec := snapshot.JSON[*userState]()
		_, err := codec.Decode("u-123", []byte(`{invalid-json`))
		if err == nil {
			t.Error("expected error for corrupt JSON payload")
		}
	})
}

func TestFuncCodec(t *testing.T) {
	t.Run("successful encode and decode", func(t *testing.T) {
		fc := snapshot.FuncCodec(
			func(s *userState) ([]byte, error) {
				return []byte(s.ID + ":" + s.Name), nil
			},
			func(streamID string, payload []byte) (*userState, error) {
				if streamID == "" {
					return nil, errors.New("empty streamID")
				}
				parts := strings.Split(string(payload), ":")
				return &userState{ID: parts[0], Name: parts[1]}, nil
			},
		)

		encoded, err := fc.Encode(&userState{ID: "u-1", Name: "Test"})
		if err != nil {
			t.Fatalf("Encode failed: %v", err)
		}
		if string(encoded) != "u-1:Test" {
			t.Errorf("expected 'u-1:Test', got %q", string(encoded))
		}

		decoded, err := fc.Decode("u-1", encoded)
		if err != nil {
			t.Fatalf("Decode failed: %v", err)
		}
		if decoded.ID != "u-1" || decoded.Name != "Test" {
			t.Errorf("unexpected decoded state: %+v", decoded)
		}
	})

	t.Run("nil encode function returns error", func(t *testing.T) {
		fc := snapshot.FuncCodec[*userState](nil, func(_ string, _ []byte) (*userState, error) {
			return &userState{}, nil
		})

		_, err := fc.Encode(&userState{})
		if err == nil || !strings.Contains(err.Error(), "encode func cannot be nil") {
			t.Errorf("expected nil encode error, got: %v", err)
		}
	})

	t.Run("nil decode function returns error", func(t *testing.T) {
		fc := snapshot.FuncCodec[*userState](func(_ *userState) ([]byte, error) {
			return []byte("data"), nil
		}, nil)

		_, err := fc.Decode("id", []byte("data"))
		if err == nil || !strings.Contains(err.Error(), "decode func cannot be nil") {
			t.Errorf("expected nil decode error, got: %v", err)
		}
	})
}

func TestRepository_Codec_StreamIDValidation(t *testing.T) {
	ctx := context.Background()

	t.Run("decode receives streamID and can reject foreign stream payloads", func(t *testing.T) {
		cfg := defaultTestConfig()
		cfg.Codec = snapshot.FuncCodec(
			func(s *userState) ([]byte, error) {
				return json.Marshal(s)
			},
			func(streamID string, data []byte) (*userState, error) {
				var s userState
				if err := json.Unmarshal(data, &s); err != nil {
					return nil, err
				}
				if s.ID != streamID {
					return nil, fmt.Errorf("stream ID mismatch: expected %q, got %q", streamID, s.ID)
				}
				return &s, nil
			},
		)

		foreignPayload, err := json.Marshal(&userState{ID: "other-user", Name: "Foreign"})
		if err != nil {
			t.Fatalf("Marshal foreignPayload: %v", err)
		}
		snapStore := &mockSnapshotStore{
			getFunc: func(_ context.Context, _ pgx.Tx, streamType, streamID string, _ int) (snapshot.Snapshot, error) {
				return snapshot.Snapshot{
					StreamType:    streamType,
					StreamID:      streamID,
					StreamVersion: 5,
					SchemaVersion: 1,
					Payload:       foreignPayload,
				}, nil
			},
		}

		reader := &mockStreamReader{
			readStreamFunc: func(_ context.Context, _ pgx.Tx, streamType, streamID string, fromVersion, _ *int64) (store.Stream, error) {
				// Fallback full replay from beginning
				if fromVersion == nil {
					return store.Stream{
						StreamType: streamType,
						StreamID:   streamID,
						Events: []store.PersistedEvent{
							{StreamVersion: 1, EventType: "UserCreated", Payload: []byte(`{"name":"Local"}`)},
						},
					}, nil
				}
				return store.Stream{StreamType: streamType, StreamID: streamID}, nil
			},
		}

		repo, err := snapshot.NewRepository(reader, snapStore, cfg)
		if err != nil {
			t.Fatalf("NewRepository failed: %v", err)
		}

		res, err := repo.Load(ctx, nil, "expected-user")
		if err != nil {
			t.Fatalf("Load failed: %v", err)
		}

		// Snapshot should be discarded because Unmarshal rejected the foreign payload, falling back to full replay
		if res.SnapshotHit {
			t.Errorf("expected SnapshotHit = false due to stream ID mismatch")
		}
		if res.StreamVersion != 1 {
			t.Errorf("expected StreamVersion = 1 from replay, got %d", res.StreamVersion)
		}
		if res.State.Name != "Local" {
			t.Errorf("expected state Name = 'Local', got %q", res.State.Name)
		}
	})
}
