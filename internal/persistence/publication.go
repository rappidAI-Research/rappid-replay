package persistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/rappidAI-Research/rappid-replay/internal/event"
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
// corresponding state.snapshot event. Event sequencing is owned by persistence;
// callers cannot supply or skip a session-local sequence number.
type PublishSnapshotRequest struct {
	StateID     id.StateID
	SessionID   id.SessionID
	RootTreeID  store.ObjectID
	Role        SnapshotRole
	WallTimeUTC time.Time
	MonotonicNS uint64
	StateBefore id.StateID
	Source      string
}

// PublishedSnapshot is the durable result of atomically publishing one state.
type PublishedSnapshot struct {
	Inspection state.Inspection
	Event      event.Event
}

// PublishSnapshot first verifies the complete reachable CAS graph, then writes
// object catalog rows, the state row, reachability edges, the snapshot event,
// and session pointers in one SQLite transaction. A state is therefore never
// visible in metadata unless every referenced object was present and verified.
func (db *DB) PublishSnapshot(
	ctx context.Context,
	cas state.InspectableObjectStore,
	req PublishSnapshotRequest,
) (PublishedSnapshot, error) {
	if err := validatePublishSnapshotRequest(req); err != nil {
		return PublishedSnapshot{}, err
	}

	inspection, err := state.InspectSnapshot(cas, req.RootTreeID)
	if err != nil {
		wrapped := fmt.Errorf("verify snapshot before publication: %w", err)
		if errors.Is(err, store.ErrCorruptObject) {
			if degradeErr := db.MarkSessionDegraded(ctx, req.SessionID, wrapped.Error()); degradeErr != nil {
				return PublishedSnapshot{}, fmt.Errorf("%w; additionally failed to mark session degraded: %v", wrapped, degradeErr)
			}
		}
		return PublishedSnapshot{}, wrapped
	}

	tx, err := db.sql.BeginTx(ctx, nil)
	if err != nil {
		return PublishedSnapshot{}, fmt.Errorf("begin snapshot publication: %w", err)
	}
	rollback := true
	defer func() {
		if rollback {
			_ = tx.Rollback()
		}
	}()

	if err := validateSnapshotLineageTx(ctx, tx, req); err != nil {
		return PublishedSnapshot{}, err
	}
	seq, err := claimEventSequence(ctx, tx, req.SessionID.String())
	if err != nil {
		return PublishedSnapshot{}, err
	}
	if err := validateMonotonicOrder(ctx, tx, req.SessionID.String(), req.MonotonicNS); err != nil {
		return PublishedSnapshot{}, err
	}

	for _, metadata := range inspection.Objects {
		if err := catalogObject(ctx, tx, metadata); err != nil {
			return PublishedSnapshot{}, err
		}
	}

	if _, err := tx.ExecContext(ctx, `
INSERT INTO states(id, session_id, event_seq, root_object_id)
VALUES(?, ?, ?, ?)`,
		req.StateID.String(), req.SessionID.String(), int64(seq), req.RootTreeID.String(),
	); err != nil {
		return PublishedSnapshot{}, fmt.Errorf("insert state %s: %w", req.StateID, err)
	}

	for _, metadata := range inspection.Objects {
		if _, err := tx.ExecContext(ctx,
			"INSERT INTO state_objects(state_id, object_id) VALUES(?, ?)",
			req.StateID.String(), metadata.ID.String(),
		); err != nil {
			return PublishedSnapshot{}, fmt.Errorf("link state %s to object %s: %w", req.StateID, metadata.ID, err)
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
		return PublishedSnapshot{}, fmt.Errorf("encode snapshot event payload: %w", err)
	}

	draft := event.NewDraft(
		req.SessionID.String(),
		"state.snapshot",
		req.Source,
		req.WallTimeUTC,
		event.Privacy{Classification: "technical"},
		payloadJSON,
	)
	draft.StateBefore = req.StateBefore.String()
	draft.StateAfter = req.StateID.String()
	persistedEvent, err := insertEventTx(ctx, tx, draft, seq, req.MonotonicNS)
	if err != nil {
		return PublishedSnapshot{}, err
	}

	if err := attachPublishedState(ctx, tx, req); err != nil {
		return PublishedSnapshot{}, err
	}

	if err := tx.Commit(); err != nil {
		return PublishedSnapshot{}, fmt.Errorf("commit snapshot publication: %w", err)
	}
	rollback = false
	return PublishedSnapshot{Inspection: inspection, Event: persistedEvent}, nil
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
	if req.MonotonicNS > maxSQLiteInteger {
		return fmt.Errorf("monotonic timestamp exceeds SQLite INTEGER range")
	}
	if req.WallTimeUTC.IsZero() {
		return fmt.Errorf("snapshot wall time is required")
	}
	if strings.TrimSpace(req.Source) == "" {
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
		if req.StateBefore == "" {
			return fmt.Errorf("%s snapshot requires state_before", req.Role)
		}
	default:
		return fmt.Errorf("invalid snapshot role %q", req.Role)
	}
	return nil
}

func validateSnapshotLineageTx(ctx context.Context, tx *sql.Tx, req PublishSnapshotRequest) error {
	var status string
	var initialState, finalState sql.NullString
	if err := tx.QueryRowContext(ctx,
		"SELECT status, initial_state_id, final_state_id FROM sessions WHERE id = ?", req.SessionID.String(),
	).Scan(&status, &initialState, &finalState); err != nil {
		if err == sql.ErrNoRows {
			return fmt.Errorf("session %s does not exist", req.SessionID)
		}
		return fmt.Errorf("read session snapshot lineage: %w", err)
	}
	if status != "recording" {
		return fmt.Errorf("session %s is %q and cannot accept snapshots", req.SessionID, status)
	}
	if finalState.Valid {
		return fmt.Errorf("session %s already has final state %s", req.SessionID, finalState.String)
	}

	var latest string
	err := tx.QueryRowContext(ctx, `
SELECT id
FROM states
WHERE session_id = ?
ORDER BY event_seq DESC
LIMIT 1`, req.SessionID.String()).Scan(&latest)
	hasState := true
	if err == sql.ErrNoRows {
		hasState = false
		latest = ""
	} else if err != nil {
		return fmt.Errorf("read latest session state: %w", err)
	}

	switch req.Role {
	case SnapshotInitial:
		if initialState.Valid || hasState {
			return fmt.Errorf("session %s already has a published state", req.SessionID)
		}
	case SnapshotCheckpoint, SnapshotFinal:
		if !initialState.Valid || !hasState {
			return fmt.Errorf("session %s cannot publish %s state before an initial state", req.SessionID, req.Role)
		}
		if latest != req.StateBefore.String() {
			return fmt.Errorf("snapshot state_before %s is stale; latest session state is %s", req.StateBefore, latest)
		}
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
WHERE id = ? AND status = 'recording' AND initial_state_id IS NULL AND final_state_id IS NULL`,
			req.StateID.String(), req.SessionID.String())
	case SnapshotFinal:
		result, err = tx.ExecContext(ctx, `
UPDATE sessions
SET final_state_id = ?
WHERE id = ? AND status = 'recording' AND final_state_id IS NULL`,
			req.StateID.String(), req.SessionID.String())
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
