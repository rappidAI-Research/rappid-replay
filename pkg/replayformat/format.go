// Package replayformat defines the portable, versioned .rplay interchange
// format. The package contains no execution capability: reading or validating an
// archive never starts an agent, command, tool, or restored workspace content.
package replayformat

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"
)

const (
	FormatName    = "rappid-replay"
	Version       = "1.0.0"
	ManifestPath  = "manifest.json"
	ChecksumsPath = "checksums.b3"
)

// Manifest is the canonical archive entry point.
type Manifest struct {
	Format           string              `json:"format"`
	Version          string              `json:"version"`
	CreatedAt        time.Time           `json:"created_at"`
	CreatedBy        string              `json:"created_by"`
	RequiredFeatures []string            `json:"required_features,omitempty"`
	Sessions         []SessionDescriptor `json:"sessions"`
	Privacy          PrivacyDescriptor   `json:"privacy"`
	Integrity        IntegrityDescriptor `json:"integrity"`
}

type PrivacyDescriptor struct {
	SecretScan string `json:"secret_scan"`
	Encrypted  bool   `json:"encrypted"`
}

type IntegrityDescriptor struct {
	Algorithm     string `json:"algorithm"`
	ChecksumsPath string `json:"checksums_path"`
	SignaturePath string `json:"signature_path,omitempty"`
}

type SessionDescriptor struct {
	ID              string `json:"id"`
	ParentSessionID string `json:"parent_session_id,omitempty"`
	ForkEventSeq    uint64 `json:"fork_event_seq,omitempty"`
}

// Session is portable session metadata. Command is already the privacy-filtered
// persisted argv; import never reconstructs missing secret material.
type Session struct {
	ID                   string    `json:"id"`
	ParentSessionID      string    `json:"parent_session_id,omitempty"`
	ForkEventSeq         uint64    `json:"fork_event_seq,omitempty"`
	Status               string    `json:"status"`
	Command              []string  `json:"command"`
	CWD                  string    `json:"cwd"`
	StartedAt            time.Time `json:"started_at"`
	EndedAt              time.Time `json:"ended_at,omitempty"`
	InitialStateID       string    `json:"initial_state_id,omitempty"`
	FinalStateID         string    `json:"final_state_id,omitempty"`
	ReproducibilityLevel string    `json:"reproducibility_level"`
	AdapterID            string    `json:"adapter_id,omitempty"`
	AdapterVersion       string    `json:"adapter_version,omitempty"`
}

type State struct {
	ID         string    `json:"id"`
	SessionID  string    `json:"session_id"`
	EventSeq   uint64    `json:"event_seq"`
	RootTreeID string    `json:"root_tree_id"`
	CreatedAt  time.Time `json:"created_at"`
}

type Artifact struct {
	ID               string `json:"id"`
	SessionID        string `json:"session_id"`
	EventSeq         uint64 `json:"event_seq"`
	FromStateID      string `json:"from_state_id"`
	StateID          string `json:"state_id"`
	PathB64          string `json:"path_b64"`
	PathDisplay      string `json:"path_display"`
	ChangeKind       string `json:"change_kind"`
	Discovery        string `json:"discovery"`
	ObjectID         string `json:"object_id"`
	PreviousObjectID string `json:"previous_object_id,omitempty"`
	Mode             uint32 `json:"mode"`
	Size             int64  `json:"size"`
}

// SessionData contains the archive entries for one immutable session.
type SessionData struct {
	Metadata    Session
	Events      []json.RawMessage
	States      []State
	Environment json.RawMessage
	Artifacts   []Artifact
}

// Bundle is the validated logical archive representation. Objects contains
// canonical typed plaintext object frames keyed by canonical b3: IDs.
type Bundle struct {
	Manifest Manifest
	Sessions []SessionData
	Objects  map[string][]byte
}

func NewManifest(createdBy, secretScan string, sessions []SessionDescriptor) Manifest {
	copySessions := append([]SessionDescriptor(nil), sessions...)
	sort.Slice(copySessions, func(i, j int) bool { return copySessions[i].ID < copySessions[j].ID })
	return Manifest{
		Format:    FormatName,
		Version:   Version,
		CreatedAt: time.Now().UTC(),
		CreatedBy: createdBy,
		Sessions:  copySessions,
		Privacy: PrivacyDescriptor{
			SecretScan: secretScan,
			Encrypted:  false,
		},
		Integrity: IntegrityDescriptor{
			Algorithm:     "blake3-256",
			ChecksumsPath: ChecksumsPath,
		},
	}
}

// ValidateManifest rejects unsupported required features rather than silently
// weakening an archive's guarantees. Version 1 currently has no optional
// required feature flags; unknown optional JSON fields remain forward-compatible.
func ValidateManifest(manifest Manifest) error {
	if manifest.Format != FormatName {
		return fmt.Errorf("archive format = %q, want %q", manifest.Format, FormatName)
	}
	if manifest.Version != Version {
		return fmt.Errorf("archive version = %q, want %q", manifest.Version, Version)
	}
	if manifest.CreatedAt.IsZero() {
		return fmt.Errorf("manifest created_at is required")
	}
	if strings.TrimSpace(manifest.CreatedBy) == "" {
		return fmt.Errorf("manifest created_by is required")
	}
	if manifest.Privacy.Encrypted {
		return fmt.Errorf("encrypted .rplay archives are not supported by format version %s", Version)
	}
	if manifest.Privacy.SecretScan != "block" && manifest.Privacy.SecretScan != "warn" && manifest.Privacy.SecretScan != "off" {
		return fmt.Errorf("manifest secret_scan = %q", manifest.Privacy.SecretScan)
	}
	if manifest.Integrity.Algorithm != "blake3-256" || manifest.Integrity.ChecksumsPath != ChecksumsPath {
		return fmt.Errorf("unsupported archive integrity descriptor")
	}
	if manifest.Integrity.SignaturePath != "" {
		return fmt.Errorf("signed .rplay archives are not supported by format version %s", Version)
	}
	if len(manifest.RequiredFeatures) != 0 {
		return fmt.Errorf("unsupported required_features: %v", manifest.RequiredFeatures)
	}
	if len(manifest.Sessions) == 0 {
		return fmt.Errorf("manifest contains no sessions")
	}
	seen := make(map[string]struct{}, len(manifest.Sessions))
	for _, session := range manifest.Sessions {
		if strings.TrimSpace(session.ID) == "" {
			return fmt.Errorf("manifest session id is required")
		}
		if _, ok := seen[session.ID]; ok {
			return fmt.Errorf("manifest contains duplicate session %s", session.ID)
		}
		seen[session.ID] = struct{}{}
		if session.ParentSessionID == "" && session.ForkEventSeq != 0 {
			return fmt.Errorf("session %s has fork sequence without parent", session.ID)
		}
		if session.ParentSessionID != "" && session.ForkEventSeq == 0 {
			return fmt.Errorf("session %s has parent without fork sequence", session.ID)
		}
	}
	return nil
}
