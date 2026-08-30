package persistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/rappidAI-Research/rappid-replay/internal/event"
	"github.com/rappidAI-Research/rappid-replay/internal/id"
	"github.com/rappidAI-Research/rappid-replay/internal/store"
)

// ImportedState is a portable state together with every authenticated CAS
// object reachable from its root. Object rows are catalogued before reachability
// links are inserted.
type ImportedState struct {
	Record  StateRecord
	Objects []store.ObjectID
}

// ImportedSession is sealed immutable evidence ready for transactional import.
type ImportedSession struct {
	Record      SessionRecord
	Events      []event.Event
	States      []ImportedState
	Environment json.RawMessage
	Artifacts   []ArtifactRecord
}

// ImportEvidence atomically imports session/event/state/environment/artifact
// metadata after callers have authenticated and persisted every required CAS
// object. Existing session IDs are rejected rather than merged or rewritten.
func (db *DB) ImportEvidence(ctx context.Context, objects []store.ObjectMetadata, sessions []ImportedSession) error {
	if db == nil || db.sql == nil {
		return fmt.Errorf("Replay database is required")
	}
	if len(sessions) == 0 {
		return fmt.Errorf("portable import contains no sessions")
	}
	if err := validateImportedEvidence(objects, sessions); err != nil {
		return err
	}

	tx, err := db.sql.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin portable import: %w", err)
	}
	rollback := true
	defer func() {
		if rollback {
			_ = tx.Rollback()
		}
	}()

	for _, metadata := range objects {
		if err := importObjectCatalogRow(ctx, tx, metadata); err != nil {
			return err
		}
	}

	ordered, err := orderImportedSessions(ctx, tx, sessions)
	if err != nil {
		return err
	}
	for _, imported := range ordered {
		if err := importSessionTx(ctx, tx, imported); err != nil {
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit portable import: %w", err)
	}
	rollback = false
	return nil
}

func validateImportedEvidence(objects []store.ObjectMetadata, sessions []ImportedSession) error {
	objectSet := make(map[store.ObjectID]store.ObjectMetadata, len(objects))
	for _, metadata := range objects {
		if _, err := store.ParseObjectID(metadata.ID.String()); err != nil {
			return fmt.Errorf("invalid imported object id: %w", err)
		}
		switch metadata.Kind {
		case store.ObjectBlob, store.ObjectTree, store.ObjectChunkList, store.ObjectLink:
		default:
			return fmt.Errorf("object %s has unsupported kind %q", metadata.ID, metadata.Kind)
		}
		if metadata.PlaintextSize <= 0 || metadata.StoredSize <= 0 {
			return fmt.Errorf("object %s has invalid size metadata", metadata.ID)
		}
		if _, exists := objectSet[metadata.ID]; exists {
			return fmt.Errorf("duplicate imported object %s", metadata.ID)
		}
		objectSet[metadata.ID] = metadata
	}

	sessionSet := make(map[id.SessionID]struct{}, len(sessions))
	stateSet := make(map[id.StateID]id.SessionID)
	for _, imported := range sessions {
		record := imported.Record
		if _, err := id.ParseSession(record.ID.String()); err != nil {
			return fmt.Errorf("invalid imported session id: %w", err)
		}
		if _, exists := sessionSet[record.ID]; exists {
			return fmt.Errorf("duplicate imported session %s", record.ID)
		}
		sessionSet[record.ID] = struct{}{}
		if record.Status == "" || record.Status == "recording" {
			return fmt.Errorf("session %s is not sealed", record.ID)
		}
		switch record.Status {
		case "completed", "aborted", "recovered", "degraded":
		default:
			return fmt.Errorf("session %s has unsupported status %q", record.ID, record.Status)
		}
		if len(record.Command) == 0 || record.Command[0] == "" || record.CWD == "" || record.StartedAt.IsZero() {
			return fmt.Errorf("session %s has incomplete metadata", record.ID)
		}
		if record.ParentSessionID == "" && record.ForkEventSeq != 0 {
			return fmt.Errorf("session %s has fork sequence without parent", record.ID)
		}
		if record.ParentSessionID != "" && record.ForkEventSeq == 0 {
			return fmt.Errorf("session %s has parent without fork sequence", record.ID)
		}
		var previousMonotonic uint64
		for index, persisted := range imported.Events {
			wantSeq := uint64(index + 1)
			if persisted.Schema != event.SchemaV1 || persisted.SessionID != record.ID.String() || persisted.Seq != wantSeq {
				return fmt.Errorf("session %s event %d has invalid envelope identity", record.ID, wantSeq)
			}
			if index > 0 && persisted.MonotonicNS < previousMonotonic {
				return fmt.Errorf("session %s event %d monotonic timestamp regresses", record.ID, persisted.Seq)
			}
			draft := event.NewDraft(persisted.SessionID, persisted.Type, persisted.Source, persisted.WallTimeUTC, persisted.Privacy, persisted.Payload)
			draft.StateBefore = persisted.StateBefore
			draft.StateAfter = persisted.StateAfter
			draft.ParentEvent = persisted.ParentEvent
			draft.SpanID = persisted.SpanID
			if persisted.Type == "state.snapshot" {
				// Snapshot events are imported through the exact evidence path below;
				// validateEventDraft intentionally rejects them for normal AppendEvent.
				if persisted.Privacy.Classification == "" || persisted.Source == "" || persisted.WallTimeUTC.IsZero() || !json.Valid(persisted.Payload) {
					return fmt.Errorf("session %s snapshot event %d is invalid", record.ID, persisted.Seq)
				}
			} else if err := validateEventDraft(draft, persisted.MonotonicNS); err != nil {
				return fmt.Errorf("session %s event %d: %w", record.ID, persisted.Seq, err)
			}
			previousMonotonic = persisted.MonotonicNS
		}
		for _, importedState := range imported.States {
			stateRecord := importedState.Record
			if stateRecord.SessionID != record.ID || stateRecord.EventSeq == 0 || stateRecord.CreatedAt.IsZero() {
				return fmt.Errorf("session %s contains invalid state %s", record.ID, stateRecord.ID)
			}
			if int(stateRecord.EventSeq) > len(imported.Events) || imported.Events[stateRecord.EventSeq-1].Type != "state.snapshot" || imported.Events[stateRecord.EventSeq-1].StateAfter != stateRecord.ID.String() {
				return fmt.Errorf("state %s is not bound to its state.snapshot event", stateRecord.ID)
			}
			if _, exists := stateSet[stateRecord.ID]; exists {
				return fmt.Errorf("duplicate imported state %s", stateRecord.ID)
			}
			stateSet[stateRecord.ID] = record.ID
			if len(importedState.Objects) == 0 {
				return fmt.Errorf("state %s has empty object reachability", stateRecord.ID)
			}
			rootSeen := false
			seenReachable := make(map[store.ObjectID]struct{}, len(importedState.Objects))
			for _, objectID := range importedState.Objects {
				if _, ok := objectSet[objectID]; !ok {
					return fmt.Errorf("state %s references uncatalogued object %s", stateRecord.ID, objectID)
				}
				if _, duplicate := seenReachable[objectID]; duplicate {
					return fmt.Errorf("state %s repeats reachable object %s", stateRecord.ID, objectID)
				}
				seenReachable[objectID] = struct{}{}
				if objectID == stateRecord.RootTreeID {
					rootSeen = true
				}
			}
			if !rootSeen {
				return fmt.Errorf("state %s reachability omits root object %s", stateRecord.ID, stateRecord.RootTreeID)
			}
		}
		if record.InitialStateID != "" {
			owner, ok := stateSet[record.InitialStateID]
			if !ok || owner != record.ID {
				return fmt.Errorf("session %s initial state %s is not imported with the session", record.ID, record.InitialStateID)
			}
		}
		if record.FinalStateID != "" {
			owner, ok := stateSet[record.FinalStateID]
			if !ok || owner != record.ID {
				return fmt.Errorf("session %s final state %s is not imported with the session", record.ID, record.FinalStateID)
			}
		}
		if len(imported.Environment) != 0 && !json.Valid(imported.Environment) {
			return fmt.Errorf("session %s environment is invalid JSON", record.ID)
		}
		for _, artifact := range imported.Artifacts {
			if artifact.SessionID != record.ID || artifact.EventSeq == 0 || int(artifact.EventSeq) > len(imported.Events) {
				return fmt.Errorf("session %s contains invalid artifact %s", record.ID, artifact.ID)
			}
			if imported.Events[artifact.EventSeq-1].Type != ArtifactEventType {
				return fmt.Errorf("artifact %s does not reference an artifact event", artifact.ID)
			}
			if err := validateArtifactPath(artifact.Path); err != nil {
				return fmt.Errorf("artifact %s path: %w", artifact.ID, err)
			}
			if _, ok := objectSet[artifact.ObjectID]; !ok {
				return fmt.Errorf("artifact %s references uncatalogued object %s", artifact.ID, artifact.ObjectID)
			}
			if artifact.PreviousObjectID != "" {
				if _, ok := objectSet[artifact.PreviousObjectID]; !ok {
					return fmt.Errorf("artifact %s references uncatalogued previous object %s", artifact.ID, artifact.PreviousObjectID)
				}
			}
		}
	}
	return nil
}

func orderImportedSessions(ctx context.Context, tx *sql.Tx, sessions []ImportedSession) ([]ImportedSession, error) {
	pending := make(map[id.SessionID]ImportedSession, len(sessions))
	for _, session := range sessions {
		pending[session.Record.ID] = session
	}
	ordered := make([]ImportedSession, 0, len(sessions))
	imported := make(map[id.SessionID]struct{}, len(sessions))
	for len(pending) != 0 {
		progress := false
		ids := make([]string, 0, len(pending))
		for sessionID := range pending {
			ids = append(ids, sessionID.String())
		}
		sort.Strings(ids)
		for _, rawID := range ids {
			sessionID, _ := id.ParseSession(rawID)
			candidate := pending[sessionID]
			parent := candidate.Record.ParentSessionID
			if parent != "" {
				if _, ok := imported[parent]; !ok {
					if _, waiting := pending[parent]; waiting {
						continue
					}
					var exists int
					if err := tx.QueryRowContext(ctx, "SELECT COUNT(1) FROM sessions WHERE id = ?", parent.String()).Scan(&exists); err != nil {
						return nil, fmt.Errorf("check imported parent %s: %w", parent, err)
					}
					if exists != 1 {
						return nil, fmt.Errorf("session %s requires missing parent session %s", sessionID, parent)
					}
				}
			}
			ordered = append(ordered, candidate)
			imported[sessionID] = struct{}{}
			delete(pending, sessionID)
			progress = true
		}
		if !progress {
			return nil, fmt.Errorf("portable import contains cyclic session lineage")
		}
	}
	return ordered, nil
}

func importObjectCatalogRow(ctx context.Context, tx *sql.Tx, metadata store.ObjectMetadata) error {
	var kind string
	var plaintext, stored int64
	err := tx.QueryRowContext(ctx, "SELECT kind, plaintext_size, stored_size FROM objects WHERE id = ?", metadata.ID.String()).Scan(&kind, &plaintext, &stored)
	if err == nil {
		if store.ObjectKind(kind) != metadata.Kind || plaintext != metadata.PlaintextSize {
			return fmt.Errorf("existing object catalog row %s conflicts with imported object", metadata.ID)
		}
		return nil
	}
	if err != sql.ErrNoRows {
		return fmt.Errorf("read object catalog row %s: %w", metadata.ID, err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO objects(id, kind, plaintext_size, stored_size) VALUES(?, ?, ?, ?)`,
		metadata.ID.String(), string(metadata.Kind), metadata.PlaintextSize, metadata.StoredSize); err != nil {
		return fmt.Errorf("insert imported object %s: %w", metadata.ID, err)
	}
	return nil
}

func importSessionTx(ctx context.Context, tx *sql.Tx, imported ImportedSession) error {
	record := imported.Record
	var exists int
	if err := tx.QueryRowContext(ctx, "SELECT COUNT(1) FROM sessions WHERE id = ?", record.ID.String()).Scan(&exists); err != nil {
		return fmt.Errorf("check existing session %s: %w", record.ID, err)
	}
	if exists != 0 {
		return fmt.Errorf("session %s already exists; portable import never merges immutable session IDs", record.ID)
	}
	commandJSON, err := json.Marshal(record.Command)
	if err != nil {
		return fmt.Errorf("encode command for session %s: %w", record.ID, err)
	}
	var parent any
	var fork any
	if record.ParentSessionID != "" {
		parent = record.ParentSessionID.String()
		fork = int64(record.ForkEventSeq)
	}
	var ended any
	if !record.EndedAt.IsZero() {
		ended = record.EndedAt.UTC().Format(time.RFC3339Nano)
	}
	var initial any
	if record.InitialStateID != "" {
		initial = record.InitialStateID.String()
	}
	var final any
	if record.FinalStateID != "" {
		final = record.FinalStateID.String()
	}
	nextSeq := int64(len(imported.Events) + 1)
	if _, err := tx.ExecContext(ctx, `
INSERT INTO sessions(
    id, parent_session_id, fork_event_seq, status, command_json, cwd, started_at,
    ended_at, initial_state_id, final_state_id, reproducibility_level,
    adapter_id, adapter_version, next_event_seq
) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULLIF(?, ''), NULLIF(?, ''), ?)`,
		record.ID.String(), parent, fork, record.Status, commandJSON, record.CWD,
		record.StartedAt.UTC().Format(time.RFC3339Nano), ended, initial, final,
		record.ReproducibilityLevel, record.AdapterID, record.AdapterVersion, nextSeq,
	); err != nil {
		return fmt.Errorf("insert imported session %s: %w", record.ID, err)
	}

	for _, importedState := range imported.States {
		stateRecord := importedState.Record
		if _, err := tx.ExecContext(ctx, `
INSERT INTO states(id, session_id, event_seq, root_object_id, created_at)
VALUES(?, ?, ?, ?, ?)`, stateRecord.ID.String(), record.ID.String(), int64(stateRecord.EventSeq), stateRecord.RootTreeID.String(), stateRecord.CreatedAt.UTC().Format(time.RFC3339Nano)); err != nil {
			return fmt.Errorf("insert imported state %s: %w", stateRecord.ID, err)
		}
		for _, objectID := range importedState.Objects {
			if _, err := tx.ExecContext(ctx, "INSERT INTO state_objects(state_id, object_id) VALUES(?, ?)", stateRecord.ID.String(), objectID.String()); err != nil {
				return fmt.Errorf("insert reachability for state %s object %s: %w", stateRecord.ID, objectID, err)
			}
		}
	}

	for _, persisted := range imported.Events {
		privacyJSON, err := json.Marshal(persisted.Privacy)
		if err != nil {
			return fmt.Errorf("encode privacy for session %s event %d: %w", record.ID, persisted.Seq, err)
		}
		if _, err := tx.ExecContext(ctx, `
INSERT INTO events(
    session_id, seq, wall_time_utc, monotonic_ns, type, source,
    state_before, state_after, payload_json, privacy_json, parent_event_id, span_id
) VALUES(?, ?, ?, ?, ?, ?, NULLIF(?, ''), NULLIF(?, ''), ?, ?, NULLIF(?, ''), NULLIF(?, ''))`,
		record.ID.String(), int64(persisted.Seq), persisted.WallTimeUTC.UTC().Format(time.RFC3339Nano), int64(persisted.MonotonicNS),
		persisted.Type, persisted.Source, persisted.StateBefore, persisted.StateAfter, []byte(persisted.Payload), privacyJSON, persisted.ParentEvent, persisted.SpanID,
		); err != nil {
			return fmt.Errorf("insert imported event %s/%d: %w", record.ID, persisted.Seq, err)
		}
	}

	if len(imported.Environment) != 0 {
		if _, err := tx.ExecContext(ctx, "INSERT INTO environments(session_id, fingerprint_json) VALUES(?, ?)", record.ID.String(), []byte(imported.Environment)); err != nil {
			return fmt.Errorf("insert imported environment for session %s: %w", record.ID, err)
		}
	}
	for _, artifact := range imported.Artifacts {
		if _, err := tx.ExecContext(ctx, `
INSERT INTO artifacts(
    id, session_id, event_seq, from_state_id, state_id, path_bytes, path_display,
    change_kind, discovery, object_id, previous_object_id, mode, size
) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULLIF(?, ''), ?, ?)`,
			artifact.ID.String(), record.ID.String(), int64(artifact.EventSeq), artifact.FromStateID.String(), artifact.StateID.String(),
			artifact.Path, artifact.PathDisplay, string(artifact.ChangeKind), artifact.Discovery, artifact.ObjectID.String(), artifact.PreviousObjectID.String(), int64(artifact.Mode), artifact.Size,
		); err != nil {
			return fmt.Errorf("insert imported artifact %s: %w", artifact.ID, err)
		}
	}
	return nil
}
