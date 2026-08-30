// Package portable implements .rplay export/import orchestration over Replay's
// immutable database and authenticated CAS. It never executes recorded code.
package portable

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"github.com/rappidAI-Research/rappid-replay/internal/event"
	"github.com/rappidAI-Research/rappid-replay/internal/id"
	"github.com/rappidAI-Research/rappid-replay/internal/persistence"
	"github.com/rappidAI-Research/rappid-replay/internal/privacy"
	"github.com/rappidAI-Research/rappid-replay/internal/state"
	"github.com/rappidAI-Research/rappid-replay/internal/store"
	"github.com/rappidAI-Research/rappid-replay/pkg/replayformat"
)

const createdBy = "rappidAI Replay"

type Dependencies struct {
	DB  *persistence.DB
	CAS *store.LocalStore
}

type ExportOptions struct {
	SessionID id.SessionID
	Path      string
	Force     bool
	SecretScan string
}

type ExportResult struct {
	Path      string
	Sessions  []id.SessionID
	States    int
	Objects   int
	Findings  []privacy.SecretFinding
}

// ExportFile exports the requested sealed session plus its complete ancestor
// lineage so imported parent/fork relationships remain self-contained.
func ExportFile(ctx context.Context, deps Dependencies, opts ExportOptions) (ExportResult, error) {
	if err := validateDependencies(deps); err != nil {
		return ExportResult{}, err
	}
	if _, err := id.ParseSession(opts.SessionID.String()); err != nil {
		return ExportResult{}, fmt.Errorf("invalid export session id: %w", err)
	}
	if opts.Path == "" {
		return ExportResult{}, fmt.Errorf("export path is required")
	}
	if opts.SecretScan != "block" && opts.SecretScan != "warn" && opts.SecretScan != "off" {
		return ExportResult{}, fmt.Errorf("invalid export secret scan mode %q", opts.SecretScan)
	}

	records, err := lineageClosure(ctx, deps.DB, opts.SessionID)
	if err != nil {
		return ExportResult{}, err
	}
	bundle := replayformat.Bundle{Objects: make(map[string][]byte)}
	descriptors := make([]replayformat.SessionDescriptor, 0, len(records))
	findings := make([]privacy.SecretFinding, 0)
	stateCount := 0

	for _, record := range records {
		if record.Status == "recording" {
			return ExportResult{}, fmt.Errorf("session %s is still recording and cannot be exported", record.ID)
		}
		descriptors = append(descriptors, replayformat.SessionDescriptor{
			ID: record.ID.String(), ParentSessionID: record.ParentSessionID.String(), ForkEventSeq: record.ForkEventSeq,
		})
		data, objectData, sessionFindings, err := buildSessionData(ctx, deps, record, opts.SecretScan != "off")
		if err != nil {
			return ExportResult{}, err
		}
		stateCount += len(data.States)
		for objectID, framed := range objectData {
			bundle.Objects[objectID] = framed
		}
		findings = privacy.MergeSecretFindings(findings, sessionFindings)
		bundle.Sessions = append(bundle.Sessions, data)
	}

	bundle.Manifest = replayformat.NewManifest(createdBy, opts.SecretScan, descriptors)
	if err := replayformat.ValidateObjectGraphs(bundle); err != nil {
		return ExportResult{}, fmt.Errorf("validate export object graph: %w", err)
	}
	if opts.SecretScan == "block" && len(findings) != 0 {
		return ExportResult{}, &privacy.SecretScanError{Findings: findings}
	}
	if err := writeBundleAtomically(opts.Path, opts.Force, bundle); err != nil {
		return ExportResult{}, err
	}
	result := ExportResult{Path: opts.Path, States: stateCount, Objects: len(bundle.Objects), Findings: findings}
	for _, record := range records {
		result.Sessions = append(result.Sessions, record.ID)
	}
	return result, nil
}

func buildSessionData(ctx context.Context, deps Dependencies, record persistence.SessionRecord, scan bool) (replayformat.SessionData, map[string][]byte, []privacy.SecretFinding, error) {
	events, err := deps.DB.ListEvents(ctx, record.ID)
	if err != nil {
		return replayformat.SessionData{}, nil, nil, err
	}
	states, err := deps.DB.ListStates(ctx, record.ID)
	if err != nil {
		return replayformat.SessionData{}, nil, nil, err
	}
	environment, hasEnvironment, err := deps.DB.GetEnvironment(ctx, record.ID)
	if err != nil {
		return replayformat.SessionData{}, nil, nil, err
	}
	artifacts, err := deps.DB.ListArtifacts(ctx, record.ID)
	if err != nil {
		return replayformat.SessionData{}, nil, nil, err
	}

	data := replayformat.SessionData{Metadata: replayformat.Session{
		ID: record.ID.String(), ParentSessionID: record.ParentSessionID.String(), ForkEventSeq: record.ForkEventSeq,
		Status: record.Status, Command: append([]string(nil), record.Command...), CWD: record.CWD,
		StartedAt: record.StartedAt.UTC(), EndedAt: record.EndedAt.UTC(),
		InitialStateID: record.InitialStateID.String(), FinalStateID: record.FinalStateID.String(),
		ReproducibilityLevel: record.ReproducibilityLevel, AdapterID: record.AdapterID, AdapterVersion: record.AdapterVersion,
	}}
	findings := make([]privacy.SecretFinding, 0)
	for _, persisted := range events {
		encoded, err := json.Marshal(persisted)
		if err != nil {
			return replayformat.SessionData{}, nil, nil, fmt.Errorf("encode session %s event %d: %w", record.ID, persisted.Seq, err)
		}
		data.Events = append(data.Events, encoded)
		if scan {
			findings = privacy.MergeSecretFindings(findings, privacy.ScanExportBytes(fmt.Sprintf("session %s event %d", record.ID, persisted.Seq), encoded))
		}
	}
	objectData := make(map[string][]byte)
	for _, stateRecord := range states {
		inspection, err := state.InspectSnapshot(deps.CAS, stateRecord.RootTreeID)
		if err != nil {
			return replayformat.SessionData{}, nil, nil, fmt.Errorf("inspect state %s before export: %w", stateRecord.ID, err)
		}
		data.States = append(data.States, replayformat.State{
			ID: stateRecord.ID.String(), SessionID: stateRecord.SessionID.String(), EventSeq: stateRecord.EventSeq,
			RootTreeID: stateRecord.RootTreeID.String(), CreatedAt: stateRecord.CreatedAt.UTC(),
		})
		for _, metadata := range inspection.Objects {
			if _, exists := objectData[metadata.ID.String()]; exists {
				continue
			}
			framed, err := deps.CAS.Get(metadata.ID)
			if err != nil {
				return replayformat.SessionData{}, nil, nil, fmt.Errorf("read object %s for export: %w", metadata.ID, err)
			}
			if store.SumObject(framed) != metadata.ID {
				return replayformat.SessionData{}, nil, nil, fmt.Errorf("object %s changed during export", metadata.ID)
			}
			objectData[metadata.ID.String()] = framed
			if scan {
				object, decodeErr := store.DecodeObject(framed)
				if decodeErr != nil {
					return replayformat.SessionData{}, nil, nil, fmt.Errorf("decode object %s for secret scan: %w", metadata.ID, decodeErr)
				}
				findings = privacy.MergeSecretFindings(findings, privacy.ScanExportBytes("object "+metadata.ID.String(), object.Payload))
			}
		}
	}
	if hasEnvironment {
		data.Environment = append(json.RawMessage(nil), environment...)
		if scan {
			findings = privacy.MergeSecretFindings(findings, privacy.ScanExportBytes("session "+record.ID.String()+" environment", environment))
		}
	}
	for _, artifact := range artifacts {
		data.Artifacts = append(data.Artifacts, replayformat.Artifact{
			ID: artifact.ID.String(), SessionID: artifact.SessionID.String(), EventSeq: artifact.EventSeq,
			FromStateID: artifact.FromStateID.String(), StateID: artifact.StateID.String(),
			PathB64: base64.StdEncoding.EncodeToString(artifact.Path), PathDisplay: artifact.PathDisplay,
			ChangeKind: string(artifact.ChangeKind), Discovery: artifact.Discovery,
			ObjectID: artifact.ObjectID.String(), PreviousObjectID: artifact.PreviousObjectID.String(), Mode: artifact.Mode, Size: artifact.Size,
		})
	}
	return data, objectData, findings, nil
}

func lineageClosure(ctx context.Context, db *persistence.DB, leaf id.SessionID) ([]persistence.SessionRecord, error) {
	var reverse []persistence.SessionRecord
	seen := make(map[id.SessionID]struct{})
	current := leaf
	for current != "" {
		if _, exists := seen[current]; exists {
			return nil, fmt.Errorf("session lineage contains cycle at %s", current)
		}
		seen[current] = struct{}{}
		record, err := db.GetSession(ctx, current)
		if err != nil {
			return nil, err
		}
		reverse = append(reverse, record)
		current = record.ParentSessionID
	}
	result := make([]persistence.SessionRecord, len(reverse))
	for i := range reverse {
		result[len(reverse)-1-i] = reverse[i]
	}
	return result, nil
}

func writeBundleAtomically(target string, force bool, bundle replayformat.Bundle) error {
	absolute, err := filepath.Abs(target)
	if err != nil {
		return fmt.Errorf("resolve export path: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(absolute), 0o755); err != nil {
		return fmt.Errorf("create export parent: %w", err)
	}
	_, statErr := os.Lstat(absolute)
	exists := statErr == nil
	if statErr != nil && !errors.Is(statErr, os.ErrNotExist) {
		return fmt.Errorf("inspect export target: %w", statErr)
	}
	if exists && !force {
		return fmt.Errorf("export target %q already exists; use --force to replace it", absolute)
	}
	temp, err := os.CreateTemp(filepath.Dir(absolute), "."+filepath.Base(absolute)+".rappid-export-")
	if err != nil {
		return fmt.Errorf("create export staging file: %w", err)
	}
	tempName := temp.Name()
	committed := false
	defer func() {
		_ = temp.Close()
		if !committed {
			_ = os.Remove(tempName)
		}
	}()
	if err := temp.Chmod(0o600); err != nil {
		return fmt.Errorf("protect export staging file: %w", err)
	}
	if err := replayformat.Write(temp, bundle); err != nil {
		return err
	}
	if err := temp.Sync(); err != nil {
		return fmt.Errorf("sync export staging file: %w", err)
	}
	if err := temp.Close(); err != nil {
		return fmt.Errorf("close export staging file: %w", err)
	}
	if !exists {
		if err := os.Rename(tempName, absolute); err != nil {
			return fmt.Errorf("commit export: %w", err)
		}
		committed = true
		return nil
	}
	backup := tempName + ".previous"
	if err := os.Rename(absolute, backup); err != nil {
		return fmt.Errorf("move previous export aside: %w", err)
	}
	if err := os.Rename(tempName, absolute); err != nil {
		rollbackErr := os.Rename(backup, absolute)
		if rollbackErr != nil {
			return fmt.Errorf("commit export: %w; rollback failed: %v", err, rollbackErr)
		}
		return fmt.Errorf("commit export: %w", err)
	}
	committed = true
	if err := os.Remove(backup); err != nil {
		return fmt.Errorf("export committed but previous archive cleanup failed: %w", err)
	}
	return nil
}

func validateDependencies(deps Dependencies) error {
	if deps.DB == nil || deps.CAS == nil {
		return fmt.Errorf("Replay database and CAS are required")
	}
	return nil
}

func marshalEventForScan(persisted event.Event) []byte {
	encoded, _ := json.Marshal(persisted)
	return encoded
}

func sortedObjectIDs(objects map[string][]byte) []string {
	ids := make([]string, 0, len(objects))
	for objectID := range objects {
		ids = append(ids, objectID)
	}
	sort.Strings(ids)
	return ids
}
