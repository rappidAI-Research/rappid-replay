package replayformat

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"

	"github.com/rappidAI-Research/rappid-replay/internal/event"
	"github.com/rappidAI-Research/rappid-replay/internal/id"
	"github.com/rappidAI-Research/rappid-replay/internal/store"
)

const maxPortableInteger = uint64(1<<63 - 1)

var portableEventTypePattern = regexp.MustCompile(`^[a-z][a-z0-9_-]*\.[a-z][a-z0-9_.-]*$`)

// validatePortableEvidence checks the immutable metadata/timeline relationships
// that make a .rplay archive more than a bag of authenticated objects. Object
// graph structure is validated separately by ValidateObjectGraphs.
func validatePortableEvidence(bundle Bundle) error {
	sessions := make(map[string]SessionData, len(bundle.Sessions))
	statesBySession := make(map[string]map[string]State, len(bundle.Sessions))
	statesByEvent := make(map[string]map[uint64]State, len(bundle.Sessions))

	for _, session := range bundle.Sessions {
		metadata := session.Metadata
		if _, err := id.ParseSession(metadata.ID); err != nil {
			return fmt.Errorf("invalid portable session id %q: %w", metadata.ID, err)
		}
		if _, duplicate := sessions[metadata.ID]; duplicate {
			return fmt.Errorf("duplicate portable session %s", metadata.ID)
		}
		if err := validatePortableSessionMetadata(metadata); err != nil {
			return err
		}
		sessions[metadata.ID] = session

		localStates := make(map[string]State, len(session.States))
		byEvent := make(map[uint64]State, len(session.States))
		var previousStateSeq uint64
		for index, stateRecord := range session.States {
			if _, err := id.ParseState(stateRecord.ID); err != nil {
				return fmt.Errorf("session %s has invalid state id %q: %w", metadata.ID, stateRecord.ID, err)
			}
			if stateRecord.SessionID != metadata.ID {
				return fmt.Errorf("state %s belongs to session %s, want %s", stateRecord.ID, stateRecord.SessionID, metadata.ID)
			}
			if stateRecord.EventSeq == 0 || stateRecord.EventSeq > maxPortableInteger {
				return fmt.Errorf("state %s has invalid event sequence %d", stateRecord.ID, stateRecord.EventSeq)
			}
			if index > 0 && stateRecord.EventSeq <= previousStateSeq {
				return fmt.Errorf("session %s states are not in strictly increasing event order", metadata.ID)
			}
			previousStateSeq = stateRecord.EventSeq
			if stateRecord.CreatedAt.IsZero() {
				return fmt.Errorf("state %s has no created_at", stateRecord.ID)
			}
			if _, err := store.ParseObjectID(stateRecord.RootTreeID); err != nil {
				return fmt.Errorf("state %s has invalid root object id: %w", stateRecord.ID, err)
			}
			if _, duplicate := localStates[stateRecord.ID]; duplicate {
				return fmt.Errorf("session %s repeats state id %s", metadata.ID, stateRecord.ID)
			}
			if _, duplicate := byEvent[stateRecord.EventSeq]; duplicate {
				return fmt.Errorf("session %s has multiple states at event sequence %d", metadata.ID, stateRecord.EventSeq)
			}
			localStates[stateRecord.ID] = stateRecord
			byEvent[stateRecord.EventSeq] = stateRecord
		}
		statesBySession[metadata.ID] = localStates
		statesByEvent[metadata.ID] = byEvent

		if err := validatePortableEvents(metadata.ID, session.Events, localStates); err != nil {
			return err
		}
		for _, stateRecord := range session.States {
			if stateRecord.EventSeq > uint64(len(session.Events)) {
				return fmt.Errorf("state %s references missing event sequence %d", stateRecord.ID, stateRecord.EventSeq)
			}
			var persisted event.Event
			if err := json.Unmarshal(session.Events[stateRecord.EventSeq-1], &persisted); err != nil {
				return fmt.Errorf("decode state event %s/%d: %w", metadata.ID, stateRecord.EventSeq, err)
			}
			if persisted.Type != "state.snapshot" || persisted.StateAfter != stateRecord.ID {
				return fmt.Errorf("state %s is not bound to its state.snapshot event", stateRecord.ID)
			}
		}
		if metadata.InitialStateID != "" {
			if _, ok := localStates[metadata.InitialStateID]; !ok {
				return fmt.Errorf("session %s initial state %s is missing or foreign", metadata.ID, metadata.InitialStateID)
			}
		}
		if metadata.FinalStateID != "" {
			if _, ok := localStates[metadata.FinalStateID]; !ok {
				return fmt.Errorf("session %s final state %s is missing or foreign", metadata.ID, metadata.FinalStateID)
			}
		}
		if err := validatePortableArtifacts(metadata.ID, session.Artifacts, session.Events, localStates, bundle.Objects); err != nil {
			return err
		}
	}

	for _, session := range bundle.Sessions {
		metadata := session.Metadata
		if metadata.ParentSessionID == "" {
			continue
		}
		if metadata.ParentSessionID == metadata.ID {
			return fmt.Errorf("session %s cannot be its own parent", metadata.ID)
		}
		parent, ok := sessions[metadata.ParentSessionID]
		if !ok {
			return fmt.Errorf("session %s requires missing parent session %s", metadata.ID, metadata.ParentSessionID)
		}
		forkState, ok := statesByEvent[parent.Metadata.ID][metadata.ForkEventSeq]
		if !ok {
			return fmt.Errorf("session %s parent %s has no state at fork event %d", metadata.ID, metadata.ParentSessionID, metadata.ForkEventSeq)
		}
		if metadata.InitialStateID == "" {
			return fmt.Errorf("branched session %s has no initial state", metadata.ID)
		}
		initial := statesBySession[metadata.ID][metadata.InitialStateID]
		if initial.RootTreeID != forkState.RootTreeID {
			return fmt.Errorf("session %s initial root %s differs from parent fork root %s", metadata.ID, initial.RootTreeID, forkState.RootTreeID)
		}
	}

	for sessionID := range sessions {
		seen := make(map[string]struct{})
		current := sessionID
		for current != "" {
			if _, duplicate := seen[current]; duplicate {
				return fmt.Errorf("portable session lineage contains cycle at %s", current)
			}
			seen[current] = struct{}{}
			current = sessions[current].Metadata.ParentSessionID
		}
	}
	return nil
}

func validatePortableSessionMetadata(metadata Session) error {
	if len(metadata.Command) == 0 || metadata.Command[0] == "" {
		return fmt.Errorf("session %s has empty command", metadata.ID)
	}
	if metadata.CWD == "" || metadata.StartedAt.IsZero() || metadata.EndedAt.IsZero() {
		return fmt.Errorf("session %s has incomplete metadata", metadata.ID)
	}
	switch metadata.Status {
	case "completed", "aborted", "recovered", "degraded":
	default:
		return fmt.Errorf("session %s has unsupported status %q", metadata.ID, metadata.Status)
	}
	switch metadata.ReproducibilityLevel {
	case "R0", "R1", "R2", "R3", "R4":
	default:
		return fmt.Errorf("session %s has invalid reproducibility level %q", metadata.ID, metadata.ReproducibilityLevel)
	}
	if metadata.ParentSessionID == "" {
		if metadata.ForkEventSeq != 0 {
			return fmt.Errorf("session %s has fork sequence without parent", metadata.ID)
		}
		return nil
	}
	if _, err := id.ParseSession(metadata.ParentSessionID); err != nil {
		return fmt.Errorf("session %s has invalid parent id: %w", metadata.ID, err)
	}
	if metadata.ForkEventSeq == 0 || metadata.ForkEventSeq > maxPortableInteger {
		return fmt.Errorf("session %s has invalid fork sequence %d", metadata.ID, metadata.ForkEventSeq)
	}
	return nil
}

func validatePortableEvents(sessionID string, lines []json.RawMessage, localStates map[string]State) error {
	var previousMonotonic uint64
	for index, raw := range lines {
		if len(raw) == 0 || !json.Valid(raw) {
			return fmt.Errorf("session %s event line %d is invalid JSON", sessionID, index+1)
		}
		var persisted event.Event
		if err := json.Unmarshal(raw, &persisted); err != nil {
			return fmt.Errorf("decode session %s event %d: %w", sessionID, index+1, err)
		}
		wantSeq := uint64(index + 1)
		if persisted.Schema != event.SchemaV1 || persisted.SessionID != sessionID || persisted.Seq != wantSeq {
			return fmt.Errorf("session %s event %d has invalid envelope identity", sessionID, wantSeq)
		}
		if persisted.WallTimeUTC.IsZero() || !portableEventTypePattern.MatchString(persisted.Type) || strings.TrimSpace(persisted.Source) == "" {
			return fmt.Errorf("session %s event %d has incomplete envelope metadata", sessionID, wantSeq)
		}
		if strings.TrimSpace(persisted.Privacy.Classification) == "" {
			return fmt.Errorf("session %s event %d has no privacy classification", sessionID, wantSeq)
		}
		if persisted.MonotonicNS > maxPortableInteger {
			return fmt.Errorf("session %s event %d monotonic timestamp exceeds supported range", sessionID, wantSeq)
		}
		if index > 0 && persisted.MonotonicNS < previousMonotonic {
			return fmt.Errorf("session %s event %d monotonic timestamp regresses", sessionID, wantSeq)
		}
		if len(persisted.Payload) != 0 && !json.Valid(persisted.Payload) {
			return fmt.Errorf("session %s event %d payload is invalid JSON", sessionID, wantSeq)
		}
		for label, rawStateID := range map[string]string{
			"state_before": persisted.StateBefore,
			"state_after":  persisted.StateAfter,
		} {
			if rawStateID == "" {
				continue
			}
			if _, err := id.ParseState(rawStateID); err != nil {
				return fmt.Errorf("session %s event %d has invalid %s: %w", sessionID, wantSeq, label, err)
			}
			if _, ok := localStates[rawStateID]; !ok {
				return fmt.Errorf("session %s event %d %s %s does not belong to the session", sessionID, wantSeq, label, rawStateID)
			}
		}
		previousMonotonic = persisted.MonotonicNS
	}
	return nil
}

func validatePortableArtifacts(
	sessionID string,
	artifacts []Artifact,
	events []json.RawMessage,
	localStates map[string]State,
	objects map[string][]byte,
) error {
	seen := make(map[string]struct{}, len(artifacts))
	for _, artifact := range artifacts {
		if _, err := id.ParseArtifact(artifact.ID); err != nil {
			return fmt.Errorf("session %s has invalid artifact id %q: %w", sessionID, artifact.ID, err)
		}
		if _, duplicate := seen[artifact.ID]; duplicate {
			return fmt.Errorf("session %s repeats artifact id %s", sessionID, artifact.ID)
		}
		seen[artifact.ID] = struct{}{}
		if artifact.SessionID != sessionID || artifact.EventSeq == 0 || artifact.EventSeq > uint64(len(events)) {
			return fmt.Errorf("session %s contains invalid artifact %s", sessionID, artifact.ID)
		}
		var persisted event.Event
		if err := json.Unmarshal(events[artifact.EventSeq-1], &persisted); err != nil {
			return fmt.Errorf("decode artifact event %s/%d: %w", sessionID, artifact.EventSeq, err)
		}
		if persisted.Type != "fs.artifact.discovered" {
			return fmt.Errorf("artifact %s does not reference an artifact event", artifact.ID)
		}
		if _, ok := localStates[artifact.FromStateID]; !ok {
			return fmt.Errorf("artifact %s references missing or foreign from-state %s", artifact.ID, artifact.FromStateID)
		}
		if _, ok := localStates[artifact.StateID]; !ok {
			return fmt.Errorf("artifact %s references missing or foreign state %s", artifact.ID, artifact.StateID)
		}
		if artifact.FromStateID == artifact.StateID {
			return fmt.Errorf("artifact %s has no state transition", artifact.ID)
		}
		pathBytes, err := base64.StdEncoding.DecodeString(artifact.PathB64)
		if err != nil {
			return fmt.Errorf("artifact %s has invalid path encoding: %w", artifact.ID, err)
		}
		if err := validatePortableArtifactPath(pathBytes); err != nil {
			return fmt.Errorf("artifact %s path: %w", artifact.ID, err)
		}
		if artifact.Size < 0 {
			return fmt.Errorf("artifact %s has negative size", artifact.ID)
		}
		if _, ok := objects[artifact.ObjectID]; !ok {
			return fmt.Errorf("artifact %s references missing object %s", artifact.ID, artifact.ObjectID)
		}
		switch artifact.ChangeKind {
		case "created":
			if artifact.PreviousObjectID != "" {
				return fmt.Errorf("created artifact %s must not have a previous object", artifact.ID)
			}
		case "modified", "replaced":
			if artifact.PreviousObjectID == "" {
				return fmt.Errorf("%s artifact %s requires a previous object", artifact.ChangeKind, artifact.ID)
			}
		default:
			return fmt.Errorf("artifact %s has unsupported change kind %q", artifact.ID, artifact.ChangeKind)
		}
		if artifact.PreviousObjectID != "" {
			if artifact.PreviousObjectID == artifact.ObjectID {
				return fmt.Errorf("artifact %s previous and current objects are identical", artifact.ID)
			}
			if _, ok := objects[artifact.PreviousObjectID]; !ok {
				return fmt.Errorf("artifact %s references missing previous object %s", artifact.ID, artifact.PreviousObjectID)
			}
		}
		if artifact.Discovery != "workspace-delta" {
			return fmt.Errorf("artifact %s has unsupported discovery %q", artifact.ID, artifact.Discovery)
		}
	}
	return nil
}

func validatePortableArtifactPath(path []byte) error {
	if len(path) == 0 {
		return fmt.Errorf("path is required")
	}
	if path[0] == '/' || path[len(path)-1] == '/' {
		return fmt.Errorf("path must be workspace-relative")
	}
	if bytes.IndexByte(path, 0) >= 0 {
		return fmt.Errorf("path contains NUL")
	}
	for _, component := range bytes.Split(path, []byte{'/'}) {
		if len(component) == 0 || bytes.Equal(component, []byte(".")) || bytes.Equal(component, []byte("..")) {
			return fmt.Errorf("path contains invalid component %q", component)
		}
	}
	return nil
}
