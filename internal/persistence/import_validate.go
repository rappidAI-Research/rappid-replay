package persistence

import (
	"encoding/json"
	"fmt"

	"github.com/rappidAI-Research/rappid-replay/internal/event"
	"github.com/rappidAI-Research/rappid-replay/internal/id"
	"github.com/rappidAI-Research/rappid-replay/internal/store"
)

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
	artifactSet := make(map[id.ArtifactID]struct{})
	for _, imported := range sessions {
		record := imported.Record
		if _, err := id.ParseSession(record.ID.String()); err != nil {
			return fmt.Errorf("invalid imported session id: %w", err)
		}
		if _, exists := sessionSet[record.ID]; exists {
			return fmt.Errorf("duplicate imported session %s", record.ID)
		}
		sessionSet[record.ID] = struct{}{}
		if err := validateImportedSessionRecord(record); err != nil {
			return err
		}
		if uint64(len(imported.Events)) >= maxSQLiteInteger {
			return fmt.Errorf("session %s has too many events", record.ID)
		}

		var previousMonotonic uint64
		for index, persisted := range imported.Events {
			wantSeq := uint64(index + 1)
			if persisted.Schema != event.SchemaV1 || persisted.SessionID != record.ID.String() || persisted.Seq != wantSeq {
				return fmt.Errorf("session %s event %d has invalid envelope identity", record.ID, wantSeq)
			}
			if persisted.MonotonicNS > maxSQLiteInteger {
				return fmt.Errorf("session %s event %d monotonic timestamp exceeds SQLite INTEGER range", record.ID, persisted.Seq)
			}
			if index > 0 && persisted.MonotonicNS < previousMonotonic {
				return fmt.Errorf("session %s event %d monotonic timestamp regresses", record.ID, persisted.Seq)
			}
			if err := validateImportedEvent(persisted); err != nil {
				return fmt.Errorf("session %s event %d: %w", record.ID, persisted.Seq, err)
			}
			previousMonotonic = persisted.MonotonicNS
		}

		localStates := make(map[id.StateID]struct{}, len(imported.States))
		for _, importedState := range imported.States {
			stateRecord := importedState.Record
			if _, err := id.ParseState(stateRecord.ID.String()); err != nil {
				return fmt.Errorf("session %s has invalid state id: %w", record.ID, err)
			}
			if stateRecord.SessionID != record.ID || stateRecord.EventSeq == 0 || stateRecord.EventSeq > maxSQLiteInteger || stateRecord.CreatedAt.IsZero() {
				return fmt.Errorf("session %s contains invalid state %s", record.ID, stateRecord.ID)
			}
			if stateRecord.EventSeq > uint64(len(imported.Events)) {
				return fmt.Errorf("state %s references missing event sequence %d", stateRecord.ID, stateRecord.EventSeq)
			}
			snapshotEvent := imported.Events[stateRecord.EventSeq-1]
			if snapshotEvent.Type != "state.snapshot" || snapshotEvent.StateAfter != stateRecord.ID.String() {
				return fmt.Errorf("state %s is not bound to its state.snapshot event", stateRecord.ID)
			}
			if _, exists := stateSet[stateRecord.ID]; exists {
				return fmt.Errorf("duplicate imported state %s", stateRecord.ID)
			}
			stateSet[stateRecord.ID] = record.ID
			localStates[stateRecord.ID] = struct{}{}
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

		if err := validateImportedEventStateOwnership(imported.Events, localStates, record.ID); err != nil {
			return err
		}
		if record.InitialStateID != "" {
			if _, ok := localStates[record.InitialStateID]; !ok {
				return fmt.Errorf("session %s initial state %s is not imported with the session", record.ID, record.InitialStateID)
			}
		}
		if record.FinalStateID != "" {
			if _, ok := localStates[record.FinalStateID]; !ok {
				return fmt.Errorf("session %s final state %s is not imported with the session", record.ID, record.FinalStateID)
			}
		}
		if len(imported.Environment) != 0 && !json.Valid(imported.Environment) {
			return fmt.Errorf("session %s environment is invalid JSON", record.ID)
		}

		for _, artifact := range imported.Artifacts {
			if _, err := id.ParseArtifact(artifact.ID.String()); err != nil {
				return fmt.Errorf("session %s has invalid artifact id: %w", record.ID, err)
			}
			if _, duplicate := artifactSet[artifact.ID]; duplicate {
				return fmt.Errorf("duplicate imported artifact %s", artifact.ID)
			}
			artifactSet[artifact.ID] = struct{}{}
			if artifact.SessionID != record.ID || artifact.EventSeq == 0 || artifact.EventSeq > uint64(len(imported.Events)) {
				return fmt.Errorf("session %s contains invalid artifact %s", record.ID, artifact.ID)
			}
			if imported.Events[artifact.EventSeq-1].Type != ArtifactEventType {
				return fmt.Errorf("artifact %s does not reference an artifact event", artifact.ID)
			}
			if _, ok := localStates[artifact.FromStateID]; !ok {
				return fmt.Errorf("artifact %s references foreign from-state %s", artifact.ID, artifact.FromStateID)
			}
			if _, ok := localStates[artifact.StateID]; !ok {
				return fmt.Errorf("artifact %s references foreign state %s", artifact.ID, artifact.StateID)
			}
			if err := validateArtifactPath(artifact.Path); err != nil {
				return fmt.Errorf("artifact %s path: %w", artifact.ID, err)
			}
			switch artifact.ChangeKind {
			case ArtifactCreated, ArtifactModified, ArtifactReplaced:
			default:
				return fmt.Errorf("artifact %s has invalid change kind %q", artifact.ID, artifact.ChangeKind)
			}
			if artifact.Discovery != ArtifactDiscoveryWorkspaceDelta {
				return fmt.Errorf("artifact %s has unsupported discovery %q", artifact.ID, artifact.Discovery)
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

func validateImportedEventStateOwnership(events []event.Event, localStates map[id.StateID]struct{}, sessionID id.SessionID) error {
	for _, persisted := range events {
		for label, rawStateID := range map[string]string{
			"state_before": persisted.StateBefore,
			"state_after":  persisted.StateAfter,
		} {
			if rawStateID == "" {
				continue
			}
			stateID, err := id.ParseState(rawStateID)
			if err != nil {
				return fmt.Errorf("session %s event %d has invalid %s: %w", sessionID, persisted.Seq, label, err)
			}
			if _, ok := localStates[stateID]; !ok {
				return fmt.Errorf("session %s event %d %s %s does not belong to the session", sessionID, persisted.Seq, label, stateID)
			}
		}
	}
	return nil
}

func validateImportedSessionRecord(record SessionRecord) error {
	if record.Status == "" || record.Status == "recording" {
		return fmt.Errorf("session %s is not sealed", record.ID)
	}
	switch record.Status {
	case "completed", "aborted", "recovered", "degraded":
	default:
		return fmt.Errorf("session %s has unsupported status %q", record.ID, record.Status)
	}
	if len(record.Command) == 0 || record.Command[0] == "" || record.CWD == "" || record.StartedAt.IsZero() || record.EndedAt.IsZero() {
		return fmt.Errorf("session %s has incomplete metadata", record.ID)
	}
	if !validReproducibilityLevel(record.ReproducibilityLevel) {
		return fmt.Errorf("session %s has invalid reproducibility level %q", record.ID, record.ReproducibilityLevel)
	}
	if record.ParentSessionID == "" && record.ForkEventSeq != 0 {
		return fmt.Errorf("session %s has fork sequence without parent", record.ID)
	}
	if record.ParentSessionID != "" {
		if _, err := id.ParseSession(record.ParentSessionID.String()); err != nil {
			return fmt.Errorf("session %s has invalid parent id: %w", record.ID, err)
		}
		if record.ForkEventSeq == 0 || record.ForkEventSeq > maxSQLiteInteger {
			return fmt.Errorf("session %s has invalid fork sequence %d", record.ID, record.ForkEventSeq)
		}
	}
	return nil
}

func validateImportedEvent(persisted event.Event) error {
	draft := event.NewDraft(
		persisted.SessionID,
		persisted.Type,
		persisted.Source,
		persisted.WallTimeUTC,
		persisted.Privacy,
		persisted.Payload,
	)
	draft.StateBefore = persisted.StateBefore
	draft.StateAfter = persisted.StateAfter
	draft.ParentEvent = persisted.ParentEvent
	draft.SpanID = persisted.SpanID

	if persisted.Type == "state.snapshot" {
		if persisted.WallTimeUTC.IsZero() || persisted.Source == "" || persisted.Privacy.Classification == "" {
			return fmt.Errorf("snapshot event metadata is incomplete")
		}
		if len(persisted.Payload) != 0 && !json.Valid(persisted.Payload) {
			return fmt.Errorf("snapshot event payload is invalid JSON")
		}
		if persisted.StateAfter == "" {
			return fmt.Errorf("snapshot event requires state_after")
		}
		if _, err := id.ParseState(persisted.StateAfter); err != nil {
			return fmt.Errorf("snapshot event has invalid state_after: %w", err)
		}
		return nil
	}
	if persisted.Type == ArtifactEventType {
		return validateEventDraft(draft, persisted.MonotonicNS)
	}
	return validateEventDraft(draft, persisted.MonotonicNS)
}
