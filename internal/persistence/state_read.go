package persistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/rappidAI-Research/rappid-replay/internal/id"
	"github.com/rappidAI-Research/rappid-replay/internal/store"
)

var ErrStateNotFound = errors.New("Replay state not found")

// StateRecord is the immutable metadata needed to verify or materialize one
// published workspace state.
type StateRecord struct {
	ID         id.StateID
	SessionID  id.SessionID
	EventSeq   uint64
	RootTreeID store.ObjectID
	CreatedAt  time.Time
}

// GetState resolves one published state without changing session history.
func (db *DB) GetState(ctx context.Context, stateID id.StateID) (StateRecord, error) {
	if db == nil || db.sql == nil {
		return StateRecord{}, fmt.Errorf("Replay database is required")
	}
	if _, err := id.ParseState(stateID.String()); err != nil {
		return StateRecord{}, fmt.Errorf("invalid state id: %w", err)
	}

	var rawID, rawSessionID, rawRootID, rawCreatedAt string
	var eventSeq sql.NullInt64
	err := db.sql.QueryRowContext(ctx, `
SELECT id, session_id, event_seq, root_object_id, created_at
FROM states
WHERE id = ?`, stateID.String()).Scan(
		&rawID,
		&rawSessionID,
		&eventSeq,
		&rawRootID,
		&rawCreatedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return StateRecord{}, fmt.Errorf("%w: %s", ErrStateNotFound, stateID)
	}
	if err != nil {
		return StateRecord{}, fmt.Errorf("read state %s: %w", stateID, err)
	}

	parsedID, err := id.ParseState(rawID)
	if err != nil {
		return StateRecord{}, fmt.Errorf("database contains invalid state id %q: %w", rawID, err)
	}
	parsedSessionID, err := id.ParseSession(rawSessionID)
	if err != nil {
		return StateRecord{}, fmt.Errorf("state %s contains invalid session id %q: %w", stateID, rawSessionID, err)
	}
	rootID, err := store.ParseObjectID(rawRootID)
	if err != nil {
		return StateRecord{}, fmt.Errorf("state %s contains invalid root object id %q: %w", stateID, rawRootID, err)
	}
	if !eventSeq.Valid || eventSeq.Int64 <= 0 {
		return StateRecord{}, fmt.Errorf("state %s has invalid event sequence", stateID)
	}
	createdAt, err := time.Parse(time.RFC3339Nano, rawCreatedAt)
	if err != nil {
		return StateRecord{}, fmt.Errorf("state %s contains invalid created_at %q: %w", stateID, rawCreatedAt, err)
	}

	return StateRecord{
		ID:         parsedID,
		SessionID:  parsedSessionID,
		EventSeq:   uint64(eventSeq.Int64),
		RootTreeID: rootID,
		CreatedAt:  createdAt,
	}, nil
}
