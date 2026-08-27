package persistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/rappidAI-Research/rappid-replay/internal/id"
	"github.com/rappidAI-Research/rappid-replay/internal/state"
	"github.com/rappidAI-Research/rappid-replay/internal/store"
)

const maxSQLiteInteger = uint64(1<<63 - 1)

// SnapshotRole describes how a published state is attached to its session.
type SnapshotRole string

const (
	SnapshotInitial    SnapshotRole = "initial"
	SnapshotCheckpoint SnapshotRole = "checkpoint"
	SnapshotFinal      SnapshotRole = "final"
)

// PublishSnapshotRequest describes one immutable state publication and its
// corresponding state.snapshot event.
type PublishSnapshotRequest struct {
	StateID     id.StateID
	SessionID   id.SessionID
	RootTreeID  store.ObjectID
	Role        SnapshotRole
	EventSeq    uint64
	WallTimeUTC time.Time
	MonotonicNS uint64
	StateBefore id.StateID
	Source      string
}

// PublishSnapshot first verifies the complete reachable CAS graph, then writes
// object catalog rows, the state row, reachability edges, the snapshot event,
// and session pointers in one SQLite transaction. A state is therefore never
// visible in metadata unless every referenced object was present and verified.
func (db *DB) PublishSnapshot(
	ctx context.Context,
	cas state.InspectableObjectStore,
	req PublishSnapshotRequest,
) (state.Inspection, error) {
	if err := validatePublishSnapshotRequest(req); err != nil {
		return state.Inspection{}, err
	}

	inspection, err := state.InspectSnapshot(cas, req.RootTreeID)
	if err != nil {
		return state.Inspection{}, fmt.Errorf("verify snapshot before publication: %w", err)
	}

	tx, err := db.sql.BeginTx(ctx, nil)
	if err != nil {
		return state.Inspection{}, fmt.Errorf("begin snapshot publication: %w", err)
	}
	rollback := true
	defer func() {
		if rollback {
			_ = tx.Rollback()
		}
	}()

	for _, metadata := range inspection.Objects {
		if err := catalogObject(ctx, tx, metadata); err != nil {
			return state.Inspection{}, err
		}
	}

	if _, err := tx.ExecContext(ctx, `
INSERT INTO states(id, session_id, event_seq, root_object_id)
VALUES(?, ?, ?, ?)`,
		req.StateID.String(), req.SessionID.String(), int64(req.EventSeq), req.RootTreeID.String(),
	); err != nil {
		return state.Inspection{}, fmt.Errorf("insert state %s: %w", req.StateID, err)
	}

	for _, metadata := range inspection.Objects {
		if _, err := tx.ExecContext(ctx,
			"INSERT INTO state_objects(state_id, object_id) VALUES(?, ?)",
			req.StateID.String(), metadata.ID.String(),
		); err != nil {
			return state.Inspection{}, fmt.Errorf("link state %s to object %s: %w", req.StateID, metadata.ID, err)
		}
	}

	payloadJSON, err := json.Marshal(struct {
		StateID       string       `json:"state_id"`
		RootTreeID    string       `json:"root_tree_id"`
		Role          SnapshotRole `json:"role"`
		Trees         int          `json:"trees"`
		Files         int          `json:"files"`
		Directories   int          `json:"directories"`
		Symlinks      int          `json:"symlinks"`
		FileBytes     int64        `json:"file_bytes"`
		ReachableObjs int          `json:"reachable_objects"`
	}{
		StateID:       req.StateID.String(),
		RootTreeID:    req.RootTreeID.String(),
		Role:          req.Role,
		Trees:         inspection.Verification.Trees,
		Files:         inspection.Verification.Files,
		Directories:   inspection.Verification.Directories,
		Symlinks:      inspection.Verification.Symlinks,
		FileBytes:     inspection.Verification.FileBytes,
		ReachableObjs: len(inspection.Objects),
	})
	if err != nil {
		return state.Inspection{}, fmt.Errorf("encode snapshot event payload: %w", err)
	}
	privacyJSON := []byte(`{"classification":"technical"}`)
	stateBefore := req.StateBefore.String()
	if _, err := tx.ExecContext(ctx, `
INSERT INTO events(
    session_id, seq, wall_time_utc, monotonic_ns, type, source,
    state_before, state_after, payload_json, privacy_json
) VALUES(?, ?, ?, ?, 'state.snapshot', ?, NULLIF(?, ''), ?, ?, ?)`,
		req.SessionID.String(), int64(req.EventSeq), req.WallTimeUTC.UTC().Format(time.RFC3339Nano),
		int64(req.MonotonicNS), req.Source, stateBefore, req.StateID.String(), payloadJSON, privacyJSON,
	); err != nil {
		return state.Inspection{}, fmt.Errorf("insert state.snapshot event: %w", err)
	}

	if err := attachPublishedState(ctx, tx, req); err != nil {
		return state.Inspection{}, err
	}

	if err := tx.Commit(); err != nil {
		return state.Inspection{}, fmt.Errorf("commit snapshot publication: %w", err)
	}
	rollback = false
	return inspection, nil
}

func validatePublishSnapshotRequest(req PublishSnapshotRequest) error {
	if _, err := id.ParseState(req.StateID.String()); err != nil {
		return fmt.Errorf("invalid state id: %w", err)
	}
	if _, err := id.ParseSession(req.SessionID.String()); err != nil {
		return fmt.Errorf("invalid session id: %w", err)
	}
	if _, err := store.ParseObjectID(req.RootTreeID.String()); err != nil {
		return fmt.Errorf("invalid root tree id: %w", err)
	}
	if req.EventSeq == 0 || req.EventSeq > maxSQLiteInteger {
		return fmt.Errorf("event sequence must be between 1 and %d", maxSQLiteInteger)
	}
	if req.MonotonicNS > maxSQLiteInteger {
		return fmt.Errorf("monotonic timestamp exceeds SQLite INTEGER range")
	}
	if req.WallTimeUTC.IsZero() {
		return fmt.Errorf("snapshot wall time is required")
	}
	if req.Source == "" {
		return fmt.Errorf("snapshot event source is required")
	}
	if req.StateBefore != "" {
		if _, err := id.ParseState(req.StateBefore.String()); err != nil {
			return fmt.Errorf("invalid state_before id: %w", err)
		}
	}
	switch req.Role {
	case SnapshotInitial:
		if req.StateBefore != "" {
			return fmt.Errorf("initial snapshot cannot declare state_before")
		}
	case SnapshotCheckpoint, SnapshotFinal:
	default:
		return fmt.Errorf("invalid snapshot role %q", req.Role)
	}
	return nil
}

func catalogObject(ctx context.Context, tx *sql.Tx, metadata store.ObjectMetadata) error {
	if metadata.PlaintextSize < 0 || metadata.StoredSize < 0 {
		return fmt.Errorf("object %s has invalid negative size metadata", metadata.ID)
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO objects(id, kind, plaintext_size, stored_size)
VALUES(?, ?, ?, ?)
ON CONFLICT(id) DO NOTHING`,
		metadata.ID.String(), string(metadata.Kind), metadata.PlaintextSize, metadata.StoredSize,
	); err != nil {
		return fmt.Errorf("catalog object %s: %w", metadata.ID, err)
	}

	var kind string
	var plaintextSize, storedSize int64
	if err := tx.QueryRowContext(ctx,
		"SELECT kind, plaintext_size, stored_size FROM objects WHERE id = ?", metadata.ID.String(),
	).Scan(&kind, &plaintextSize, &storedSize); err != nil {
		return fmt.Errorf("read cataloged object %s: %w", metadata.ID, err)
	}
	if kind != string(metadata.Kind) || plaintextSize != metadata.PlaintextSize || storedSize != metadata.StoredSize {
		return fmt.Errorf(
			"object %s metadata mismatch: database=(%s,%d,%d) verified=(%s,%d,%d)",
			metadata.ID, kind, plaintextSize, storedSize,
			metadata.Kind, metadata.PlaintextSize, metadata.StoredSize,
		)
	}
	return nil
}

func attachPublishedState(ctx context.Context, tx *sql.Tx, req PublishSnapshotRequest) error {
	var result sql.Result
	var err error
	switch req.Role {
	case SnapshotInitial:
		result, err = tx.ExecContext(ctx, `
UPDATE sessions
SET initial_state_id = ?,
    reproducibility_level = CASE WHEN reproducibility_level = 'R0' THEN 'R1' ELSE reproducibility_level END
WHERE id = ? AND initial_state_id IS NULL`, req.StateID.String(), req.SessionID.String())
	case SnapshotFinal:
		result, err = tx.ExecContext(ctx,
			"UPDATE sessions SET final_state_id = ? WHERE id = ? AND final_state_id IS NULL",
			req.StateID.String(), req.SessionID.String(),
		)
	case SnapshotCheckpoint:
		return nil
	default:
		return fmt.Errorf("invalid snapshot role %q", req.Role)
	}
	if err != nil {
		return fmt.Errorf("attach %s state to session: %w", req.Role, err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("read session state update result: %w", err)
	}
	if rows != 1 {
		return fmt.Errorf("session %s cannot accept another %s state", req.SessionID, req.Role)
	}
	return nil
}
