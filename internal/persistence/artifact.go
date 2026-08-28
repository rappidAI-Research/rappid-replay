package persistence

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/rappidAI-Research/rappid-replay/internal/event"
	"github.com/rappidAI-Research/rappid-replay/internal/id"
	"github.com/rappidAI-Research/rappid-replay/internal/store"
)

// ArtifactChangeKind describes how a regular workspace file became observable
// at a published state boundary.
type ArtifactChangeKind string

const (
	ArtifactCreated  ArtifactChangeKind = "created"
	ArtifactModified ArtifactChangeKind = "modified"
	ArtifactReplaced ArtifactChangeKind = "replaced"

	ArtifactDiscoveryWorkspaceDelta = "workspace-delta"
)

// PublishArtifactRequest binds one discovered file object to the exact state
// transition that made it observable. Path contains workspace-relative raw
// bytes joined with '/'; it is not required to be valid UTF-8.
type PublishArtifactRequest struct {
	SessionID        id.SessionID
	FromStateID      id.StateID
	StateID          id.StateID
	Path             []byte
	ChangeKind       ArtifactChangeKind
	ObjectID         store.ObjectID
	PreviousObjectID store.ObjectID
	Mode             uint32
	Size             int64
	Discovery        string
	Source           string
	WallTimeUTC      time.Time
	MonotonicNS      uint64
}

// PublishedArtifact is the immutable artifact provenance row and its canonical
// timeline event.
type PublishedArtifact struct {
	ID    id.ArtifactID
	Event event.Event
}

type artifactEventPayload struct {
	ArtifactID        string `json:"artifact_id"`
	Discovery         string `json:"discovery"`
	ChangeKind        string `json:"change_kind"`
	PathB64           string `json:"path_b64"`
	PathDisplay       string `json:"path_display"`
	ObjectID          string `json:"object_id"`
	PreviousObjectID  string `json:"previous_object_id,omitempty"`
	Mode              uint32 `json:"mode"`
	Size              int64  `json:"size"`
	FromStateID       string `json:"from_state_id"`
	StateID           string `json:"state_id"`
}

// PublishArtifact atomically reserves an event sequence, writes the
// artifact.discovered event, and publishes the matching provenance row. The
// referenced file object must already be reachable from StateID, so an artifact
// can never point at unauthenticated or unrelated CAS content.
func (db *DB) PublishArtifact(ctx context.Context, req PublishArtifactRequest) (PublishedArtifact, error) {
	if err := validateArtifactRequest(req); err != nil {
		return PublishedArtifact{}, err
	}
	artifactID, err := id.NewArtifact()
	if err != nil {
		return PublishedArtifact{}, err
	}
	pathDisplay := strings.ToValidUTF8(string(req.Path), "\uFFFD")

	payload, err := json.Marshal(artifactEventPayload{
		ArtifactID:       artifactID.String(),
		Discovery:        req.Discovery,
		ChangeKind:       string(req.ChangeKind),
		PathB64:          base64.StdEncoding.EncodeToString(req.Path),
		PathDisplay:      pathDisplay,
		ObjectID:         req.ObjectID.String(),
		PreviousObjectID: req.PreviousObjectID.String(),
		Mode:             req.Mode,
		Size:             req.Size,
		FromStateID:      req.FromStateID.String(),
		StateID:          req.StateID.String(),
	})
	if err != nil {
		return PublishedArtifact{}, fmt.Errorf("encode artifact discovery payload: %w", err)
	}
	draft := event.NewDraft(
		req.SessionID.String(),
		"artifact.discovered",
		req.Source,
		req.WallTimeUTC,
		event.Privacy{Classification: "technical"},
		payload,
	)
	draft.StateBefore = req.FromStateID.String()
	draft.StateAfter = req.StateID.String()
	if err := validateEventDraft(draft, req.MonotonicNS); err != nil {
		return PublishedArtifact{}, err
	}

	tx, err := db.sql.BeginTx(ctx, nil)
	if err != nil {
		return PublishedArtifact{}, fmt.Errorf("begin artifact publication: %w", err)
	}
	rollback := true
	defer func() {
		if rollback {
			_ = tx.Rollback()
		}
	}()

	if err := validateEventStateReferences(ctx, tx, draft); err != nil {
		return PublishedArtifact{}, err
	}
	currentKind, err := artifactObjectKind(ctx, tx, req.SessionID, req.StateID, req.ObjectID)
	if err != nil {
		return PublishedArtifact{}, fmt.Errorf("validate artifact object: %w", err)
	}
	if currentKind != store.ObjectBlob && currentKind != store.ObjectChunkList {
		return PublishedArtifact{}, fmt.Errorf("artifact object %s kind = %q, want file object", req.ObjectID, currentKind)
	}
	if req.PreviousObjectID != "" {
		previousKind, err := artifactObjectKind(ctx, tx, req.SessionID, req.FromStateID, req.PreviousObjectID)
		if err != nil {
			return PublishedArtifact{}, fmt.Errorf("validate previous artifact object: %w", err)
		}
		if req.ChangeKind == ArtifactModified && previousKind != store.ObjectBlob && previousKind != store.ObjectChunkList {
			return PublishedArtifact{}, fmt.Errorf("modified artifact previous object %s kind = %q, want file object", req.PreviousObjectID, previousKind)
		}
	}

	seq, err := claimEventSequence(ctx, tx, req.SessionID.String())
	if err != nil {
		return PublishedArtifact{}, err
	}
	if err := validateMonotonicOrder(ctx, tx, req.SessionID.String(), req.MonotonicNS); err != nil {
		return PublishedArtifact{}, err
	}
	persistedEvent, err := insertEventTx(ctx, tx, draft, seq, req.MonotonicNS)
	if err != nil {
		return PublishedArtifact{}, err
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO artifacts(
    id, session_id, event_seq, from_state_id, state_id,
    path_bytes, path_display, change_kind, discovery,
    object_id, previous_object_id, mode, size
) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULLIF(?, ''), ?, ?)`,
		artifactID.String(),
		req.SessionID.String(),
		int64(seq),
		req.FromStateID.String(),
		req.StateID.String(),
		append([]byte(nil), req.Path...),
		pathDisplay,
		string(req.ChangeKind),
		req.Discovery,
		req.ObjectID.String(),
		req.PreviousObjectID.String(),
		int64(req.Mode),
		req.Size,
	); err != nil {
		return PublishedArtifact{}, fmt.Errorf("insert artifact %s: %w", artifactID, err)
	}
	if err := tx.Commit(); err != nil {
		return PublishedArtifact{}, fmt.Errorf("commit artifact publication: %w", err)
	}
	rollback = false
	return PublishedArtifact{ID: artifactID, Event: persistedEvent}, nil
}

func validateArtifactRequest(req PublishArtifactRequest) error {
	if _, err := id.ParseSession(req.SessionID.String()); err != nil {
		return fmt.Errorf("invalid artifact session id: %w", err)
	}
	if _, err := id.ParseState(req.FromStateID.String()); err != nil {
		return fmt.Errorf("invalid artifact from state id: %w", err)
	}
	if _, err := id.ParseState(req.StateID.String()); err != nil {
		return fmt.Errorf("invalid artifact state id: %w", err)
	}
	if req.FromStateID == req.StateID {
		return fmt.Errorf("artifact state transition must contain distinct states")
	}
	if err := validateArtifactPath(req.Path); err != nil {
		return err
	}
	if _, err := store.ParseObjectID(req.ObjectID.String()); err != nil {
		return fmt.Errorf("invalid artifact object id: %w", err)
	}
	if req.PreviousObjectID != "" {
		if _, err := store.ParseObjectID(req.PreviousObjectID.String()); err != nil {
			return fmt.Errorf("invalid previous artifact object id: %w", err)
		}
		if req.PreviousObjectID == req.ObjectID {
			return fmt.Errorf("artifact previous and current object ids must differ")
		}
	}
	switch req.ChangeKind {
	case ArtifactCreated:
		if req.PreviousObjectID != "" {
			return fmt.Errorf("created artifact must not have previous object id")
		}
	case ArtifactModified, ArtifactReplaced:
		if req.PreviousObjectID == "" {
			return fmt.Errorf("%s artifact requires previous object id", req.ChangeKind)
		}
	default:
		return fmt.Errorf("unsupported artifact change kind %q", req.ChangeKind)
	}
	if req.Size < 0 {
		return fmt.Errorf("artifact size must be non-negative")
	}
	if req.Discovery == "" {
		req.Discovery = ArtifactDiscoveryWorkspaceDelta
	}
	if req.Discovery != ArtifactDiscoveryWorkspaceDelta {
		return fmt.Errorf("unsupported artifact discovery %q", req.Discovery)
	}
	if strings.TrimSpace(req.Source) == "" {
		return fmt.Errorf("artifact source is required")
	}
	if req.WallTimeUTC.IsZero() {
		return fmt.Errorf("artifact wall time is required")
	}
	if req.MonotonicNS > maxSQLiteInteger {
		return fmt.Errorf("artifact monotonic timestamp exceeds SQLite INTEGER range")
	}
	return nil
}

func validateArtifactPath(path []byte) error {
	if len(path) == 0 {
		return fmt.Errorf("artifact path is required")
	}
	if path[0] == '/' || path[len(path)-1] == '/' {
		return fmt.Errorf("artifact path must be workspace-relative")
	}
	if bytes.IndexByte(path, 0) >= 0 {
		return fmt.Errorf("artifact path contains NUL")
	}
	for _, component := range bytes.Split(path, []byte{'/'}) {
		if len(component) == 0 || bytes.Equal(component, []byte(".")) || bytes.Equal(component, []byte("..")) {
			return fmt.Errorf("artifact path contains invalid component %q", component)
		}
	}
	return nil
}

func artifactObjectKind(
	ctx context.Context,
	tx *sql.Tx,
	sessionID id.SessionID,
	stateID id.StateID,
	objectID store.ObjectID,
) (store.ObjectKind, error) {
	var kind string
	err := tx.QueryRowContext(ctx, `
SELECT objects.kind
FROM state_objects
JOIN states ON states.id = state_objects.state_id
JOIN objects ON objects.id = state_objects.object_id
WHERE state_objects.state_id = ?
  AND state_objects.object_id = ?
  AND states.session_id = ?`,
		stateID.String(), objectID.String(), sessionID.String(),
	).Scan(&kind)
	if err == sql.ErrNoRows {
		return "", fmt.Errorf("object %s is not reachable from state %s in session %s", objectID, stateID, sessionID)
	}
	if err != nil {
		return "", fmt.Errorf("read artifact object reachability: %w", err)
	}
	return store.ObjectKind(kind), nil
}
