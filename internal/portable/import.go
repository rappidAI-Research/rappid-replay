package portable

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"sort"

	"github.com/rappidAI-Research/rappid-replay/internal/event"
	"github.com/rappidAI-Research/rappid-replay/internal/id"
	"github.com/rappidAI-Research/rappid-replay/internal/persistence"
	"github.com/rappidAI-Research/rappid-replay/internal/state"
	"github.com/rappidAI-Research/rappid-replay/internal/store"
	"github.com/rappidAI-Research/rappid-replay/pkg/replayformat"
)

type VerifyArchiveResult struct {
	Sessions  int `json:"sessions"`
	States    int `json:"states"`
	Events    int `json:"events"`
	Artifacts int `json:"artifacts"`
	Objects   int `json:"objects"`
}

type ImportResult struct {
	Sessions []id.SessionID
	States   int
	Objects  int
}

// VerifyFile authenticates a portable archive and its complete state-object
// graphs without opening Replay's local runtime or writing any workspace data.
func VerifyFile(path string) (VerifyArchiveResult, error) {
	bundle, err := readBundleFile(path)
	if err != nil {
		return VerifyArchiveResult{}, err
	}
	if err := replayformat.ValidateObjectGraphs(bundle); err != nil {
		return VerifyArchiveResult{}, err
	}
	result := VerifyArchiveResult{Sessions: len(bundle.Sessions), Objects: len(bundle.Objects)}
	for _, session := range bundle.Sessions {
		result.States += len(session.States)
		result.Events += len(session.Events)
		result.Artifacts += len(session.Artifacts)
	}
	return result, nil
}

// ImportFile validates the complete archive before mutating local metadata. CAS
// objects are content-addressed and may be written before the SQLite transaction;
// a later metadata failure can leave only harmless unreferenced objects for GC.
func ImportFile(ctx context.Context, deps Dependencies, path string) (ImportResult, error) {
	if err := validateDependencies(deps); err != nil {
		return ImportResult{}, err
	}
	bundle, err := readBundleFile(path)
	if err != nil {
		return ImportResult{}, err
	}
	if err := replayformat.ValidateObjectGraphs(bundle); err != nil {
		return ImportResult{}, fmt.Errorf("validate archive object graph: %w", err)
	}

	objectIDs := make([]string, 0, len(bundle.Objects))
	for rawID := range bundle.Objects {
		objectIDs = append(objectIDs, rawID)
	}
	sort.Strings(objectIDs)
	metadata := make([]store.ObjectMetadata, 0, len(objectIDs))
	for _, rawID := range objectIDs {
		expected, err := store.ParseObjectID(rawID)
		if err != nil {
			return ImportResult{}, err
		}
		actual, err := deps.CAS.Put(bundle.Objects[rawID])
		if err != nil {
			return ImportResult{}, fmt.Errorf("store imported object %s: %w", expected, err)
		}
		if actual != expected {
			return ImportResult{}, fmt.Errorf("imported object %s stored as unexpected id %s", expected, actual)
		}
		item, err := deps.CAS.InspectObject(expected)
		if err != nil {
			return ImportResult{}, fmt.Errorf("inspect imported object %s: %w", expected, err)
		}
		metadata = append(metadata, item)
	}

	importedSessions := make([]persistence.ImportedSession, 0, len(bundle.Sessions))
	stateCount := 0
	for _, portableSession := range bundle.Sessions {
		converted, err := convertImportedSession(deps.CAS, portableSession)
		if err != nil {
			return ImportResult{}, err
		}
		stateCount += len(converted.States)
		importedSessions = append(importedSessions, converted)
	}
	if err := deps.DB.ImportEvidence(ctx, metadata, importedSessions); err != nil {
		return ImportResult{}, err
	}
	result := ImportResult{States: stateCount, Objects: len(metadata)}
	for _, imported := range importedSessions {
		result.Sessions = append(result.Sessions, imported.Record.ID)
	}
	return result, nil
}

func readBundleFile(name string) (replayformat.Bundle, error) {
	file, err := os.Open(name)
	if err != nil {
		return replayformat.Bundle{}, fmt.Errorf("open .rplay archive: %w", err)
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return replayformat.Bundle{}, fmt.Errorf("stat .rplay archive: %w", err)
	}
	if !info.Mode().IsRegular() {
		return replayformat.Bundle{}, fmt.Errorf(".rplay archive must be a regular file")
	}
	bundle, err := replayformat.Read(file, info.Size())
	if err != nil {
		return replayformat.Bundle{}, err
	}
	return bundle, nil
}

func convertImportedSession(cas *store.LocalStore, portableSession replayformat.SessionData) (persistence.ImportedSession, error) {
	metadata := portableSession.Metadata
	sessionID, err := id.ParseSession(metadata.ID)
	if err != nil {
		return persistence.ImportedSession{}, fmt.Errorf("invalid portable session id %q: %w", metadata.ID, err)
	}
	var parent id.SessionID
	if metadata.ParentSessionID != "" {
		parent, err = id.ParseSession(metadata.ParentSessionID)
		if err != nil {
			return persistence.ImportedSession{}, fmt.Errorf("session %s parent: %w", sessionID, err)
		}
	}
	var initial, final id.StateID
	if metadata.InitialStateID != "" {
		initial, err = id.ParseState(metadata.InitialStateID)
		if err != nil {
			return persistence.ImportedSession{}, fmt.Errorf("session %s initial state: %w", sessionID, err)
		}
	}
	if metadata.FinalStateID != "" {
		final, err = id.ParseState(metadata.FinalStateID)
		if err != nil {
			return persistence.ImportedSession{}, fmt.Errorf("session %s final state: %w", sessionID, err)
		}
	}
	result := persistence.ImportedSession{Record: persistence.SessionRecord{
		ID:                   sessionID,
		ParentSessionID:      parent,
		ForkEventSeq:         metadata.ForkEventSeq,
		Status:               metadata.Status,
		Command:              append([]string(nil), metadata.Command...),
		CWD:                  metadata.CWD,
		StartedAt:            metadata.StartedAt.UTC(),
		EndedAt:              metadata.EndedAt.UTC(),
		InitialStateID:       initial,
		FinalStateID:         final,
		ReproducibilityLevel: metadata.ReproducibilityLevel,
		AdapterID:            metadata.AdapterID,
		AdapterVersion:       metadata.AdapterVersion,
	}}
	for index, raw := range portableSession.Events {
		var persisted event.Event
		if err := json.Unmarshal(raw, &persisted); err != nil {
			return persistence.ImportedSession{}, fmt.Errorf("decode session %s event %d: %w", sessionID, index+1, err)
		}
		result.Events = append(result.Events, persisted)
	}
	for _, portableState := range portableSession.States {
		stateID, err := id.ParseState(portableState.ID)
		if err != nil {
			return persistence.ImportedSession{}, fmt.Errorf("session %s state id: %w", sessionID, err)
		}
		stateSessionID, err := id.ParseSession(portableState.SessionID)
		if err != nil {
			return persistence.ImportedSession{}, fmt.Errorf("state %s session id: %w", stateID, err)
		}
		rootID, err := store.ParseObjectID(portableState.RootTreeID)
		if err != nil {
			return persistence.ImportedSession{}, fmt.Errorf("state %s root: %w", stateID, err)
		}
		inspection, err := state.InspectSnapshot(cas, rootID)
		if err != nil {
			return persistence.ImportedSession{}, fmt.Errorf("verify imported state %s: %w", stateID, err)
		}
		reachable := make([]store.ObjectID, 0, len(inspection.Objects))
		for _, object := range inspection.Objects {
			reachable = append(reachable, object.ID)
		}
		result.States = append(result.States, persistence.ImportedState{
			Record: persistence.StateRecord{
				ID:         stateID,
				SessionID:  stateSessionID,
				EventSeq:   portableState.EventSeq,
				RootTreeID: rootID,
				CreatedAt:  portableState.CreatedAt.UTC(),
			},
			Objects: reachable,
		})
	}
	if len(portableSession.Environment) != 0 {
		result.Environment = append(json.RawMessage(nil), portableSession.Environment...)
	}
	for _, portableArtifact := range portableSession.Artifacts {
		artifactID, err := id.ParseArtifact(portableArtifact.ID)
		if err != nil {
			return persistence.ImportedSession{}, fmt.Errorf("invalid artifact id %q: %w", portableArtifact.ID, err)
		}
		artifactSession, err := id.ParseSession(portableArtifact.SessionID)
		if err != nil {
			return persistence.ImportedSession{}, fmt.Errorf("artifact %s session: %w", artifactID, err)
		}
		fromState, err := id.ParseState(portableArtifact.FromStateID)
		if err != nil {
			return persistence.ImportedSession{}, fmt.Errorf("artifact %s from state: %w", artifactID, err)
		}
		toState, err := id.ParseState(portableArtifact.StateID)
		if err != nil {
			return persistence.ImportedSession{}, fmt.Errorf("artifact %s state: %w", artifactID, err)
		}
		objectID, err := store.ParseObjectID(portableArtifact.ObjectID)
		if err != nil {
			return persistence.ImportedSession{}, fmt.Errorf("artifact %s object: %w", artifactID, err)
		}
		var previous store.ObjectID
		if portableArtifact.PreviousObjectID != "" {
			previous, err = store.ParseObjectID(portableArtifact.PreviousObjectID)
			if err != nil {
				return persistence.ImportedSession{}, fmt.Errorf("artifact %s previous object: %w", artifactID, err)
			}
		}
		pathBytes, err := base64.StdEncoding.DecodeString(portableArtifact.PathB64)
		if err != nil {
			return persistence.ImportedSession{}, fmt.Errorf("artifact %s path: %w", artifactID, err)
		}
		result.Artifacts = append(result.Artifacts, persistence.ArtifactRecord{
			ID:               artifactID,
			SessionID:        artifactSession,
			EventSeq:         portableArtifact.EventSeq,
			FromStateID:      fromState,
			StateID:          toState,
			Path:             pathBytes,
			PathDisplay:      portableArtifact.PathDisplay,
			ChangeKind:       persistence.ArtifactChangeKind(portableArtifact.ChangeKind),
			Discovery:        portableArtifact.Discovery,
			ObjectID:         objectID,
			PreviousObjectID: previous,
			Mode:             portableArtifact.Mode,
			Size:             portableArtifact.Size,
		})
	}
	return result, nil
}
