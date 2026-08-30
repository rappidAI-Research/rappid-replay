package persistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/rappidAI-Research/rappid-replay/internal/id"
	"github.com/rappidAI-Research/rappid-replay/internal/store"
)

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

func validateImportedLineageTx(ctx context.Context, tx *sql.Tx, imported ImportedSession) error {
	record := imported.Record
	if record.ParentSessionID == "" {
		return nil
	}
	var parentRootText string
	err := tx.QueryRowContext(ctx, `
SELECT root_object_id
FROM states
WHERE session_id = ? AND event_seq = ?`, record.ParentSessionID.String(), int64(record.ForkEventSeq)).Scan(&parentRootText)
	if err == sql.ErrNoRows {
		return fmt.Errorf("session %s parent %s has no state at fork event %d", record.ID, record.ParentSessionID, record.ForkEventSeq)
	}
	if err != nil {
		return fmt.Errorf("read parent fork state for session %s: %w", record.ID, err)
	}
	parentRoot, err := store.ParseObjectID(parentRootText)
	if err != nil {
		return fmt.Errorf("session %s parent fork state has invalid root object: %w", record.ID, err)
	}
	if record.InitialStateID == "" {
		return fmt.Errorf("branched session %s has no initial state", record.ID)
	}
	for _, importedState := range imported.States {
		if importedState.Record.ID != record.InitialStateID {
			continue
		}
		if importedState.Record.RootTreeID != parentRoot {
			return fmt.Errorf(
				"session %s initial root %s differs from parent fork root %s",
				record.ID,
				importedState.Record.RootTreeID,
				parentRoot,
			)
		}
		return nil
	}
	return fmt.Errorf("branched session %s initial state %s is missing", record.ID, record.InitialStateID)
}

func importObjectCatalogRow(ctx context.Context, tx *sql.Tx, metadata store.ObjectMetadata) error {
	var kind string
	var plaintext, stored int64
	err := tx.QueryRowContext(
		ctx,
		"SELECT kind, plaintext_size, stored_size FROM objects WHERE id = ?",
		metadata.ID.String(),
	).Scan(&kind, &plaintext, &stored)
	if err == nil {
		if store.ObjectKind(kind) != metadata.Kind || plaintext != metadata.PlaintextSize {
			return fmt.Errorf("existing object catalog row %s conflicts with imported object", metadata.ID)
		}
		return nil
	}
	if err != sql.ErrNoRows {
		return fmt.Errorf("read object catalog row %s: %w", metadata.ID, err)
	}
	if _, err := tx.ExecContext(
		ctx,
		`INSERT INTO objects(id, kind, plaintext_size, stored_size) VALUES(?, ?, ?, ?)`,
		metadata.ID.String(),
		string(metadata.Kind),
		metadata.PlaintextSize,
		metadata.StoredSize,
	); err != nil {
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
		record.ID.String(),
		parent,
		fork,
		record.Status,
		commandJSON,
		record.CWD,
		record.StartedAt.UTC().Format(time.RFC3339Nano),
		ended,
		initial,
		final,
		record.ReproducibilityLevel,
		record.AdapterID,
		record.AdapterVersion,
		nextSeq,
	); err != nil {
		return fmt.Errorf("insert imported session %s: %w", record.ID, err)
	}

	for _, importedState := range imported.States {
		stateRecord := importedState.Record
		if _, err := tx.ExecContext(ctx, `
INSERT INTO states(id, session_id, event_seq, root_object_id, created_at)
VALUES(?, ?, ?, ?, ?)`,
			stateRecord.ID.String(),
			record.ID.String(),
			int64(stateRecord.EventSeq),
			stateRecord.RootTreeID.String(),
			stateRecord.CreatedAt.UTC().Format(time.RFC3339Nano),
		); err != nil {
			return fmt.Errorf("insert imported state %s: %w", stateRecord.ID, err)
		}
		for _, objectID := range importedState.Objects {
			if _, err := tx.ExecContext(
				ctx,
				"INSERT INTO state_objects(state_id, object_id) VALUES(?, ?)",
				stateRecord.ID.String(),
				objectID.String(),
			); err != nil {
				return fmt.Errorf("insert reachability for state %s object %s: %w", stateRecord.ID, objectID, err)
			}
		}
	}

	for _, persisted := range imported.Events {
		privacyJSON, err := json.Marshal(persisted.Privacy)
		if err != nil {
			return fmt.Errorf("encode privacy for session %s event %d: %w", record.ID, persisted.Seq, err)
		}
		payload := persisted.Payload
		if len(payload) == 0 {
			payload = json.RawMessage("null")
		}
		if _, err := tx.ExecContext(ctx, `
INSERT INTO events(
    session_id, seq, wall_time_utc, monotonic_ns, type, source,
    state_before, state_after, payload_json, privacy_json, parent_event_id, span_id
) VALUES(?, ?, ?, ?, ?, ?, NULLIF(?, ''), NULLIF(?, ''), ?, ?, NULLIF(?, ''), NULLIF(?, ''))`,
			record.ID.String(),
			int64(persisted.Seq),
			persisted.WallTimeUTC.UTC().Format(time.RFC3339Nano),
			int64(persisted.MonotonicNS),
			persisted.Type,
			persisted.Source,
			persisted.StateBefore,
			persisted.StateAfter,
			[]byte(payload),
			privacyJSON,
			persisted.ParentEvent,
			persisted.SpanID,
		); err != nil {
			return fmt.Errorf("insert imported event %s/%d: %w", record.ID, persisted.Seq, err)
		}
	}

	if len(imported.Environment) != 0 {
		if _, err := tx.ExecContext(
			ctx,
			"INSERT INTO environments(session_id, fingerprint_json) VALUES(?, ?)",
			record.ID.String(),
			[]byte(imported.Environment),
		); err != nil {
			return fmt.Errorf("insert imported environment for session %s: %w", record.ID, err)
		}
	}
	for _, artifact := range imported.Artifacts {
		if _, err := tx.ExecContext(ctx, `
INSERT INTO artifacts(
    id, session_id, event_seq, from_state_id, state_id, path_bytes, path_display,
    change_kind, discovery, object_id, previous_object_id, mode, size
) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULLIF(?, ''), ?, ?)`,
			artifact.ID.String(),
			record.ID.String(),
			int64(artifact.EventSeq),
			artifact.FromStateID.String(),
			artifact.StateID.String(),
			artifact.Path,
			artifact.PathDisplay,
			string(artifact.ChangeKind),
			artifact.Discovery,
			artifact.ObjectID.String(),
			artifact.PreviousObjectID.String(),
			int64(artifact.Mode),
			artifact.Size,
		); err != nil {
			return fmt.Errorf("insert imported artifact %s: %w", artifact.ID, err)
		}
	}
	return nil
}
