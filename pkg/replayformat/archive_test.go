package replayformat

import (
	"archive/zip"
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/rappidAI-Research/rappid-replay/internal/id"
	"github.com/rappidAI-Research/rappid-replay/internal/state"
	"github.com/rappidAI-Research/rappid-replay/internal/store"
)

func TestArchiveRoundTripValidatesObjectGraph(t *testing.T) {
	bundle := validTestBundle(t)
	rootID := bundle.Sessions[0].States[0].RootTreeID

	var encoded bytes.Buffer
	if err := Write(&encoded, bundle); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	decoded, err := Read(bytes.NewReader(encoded.Bytes()), int64(encoded.Len()))
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	if err := ValidateObjectGraphs(decoded); err != nil {
		t.Fatalf("decoded graph error = %v", err)
	}
	if len(decoded.Sessions) != 1 || len(decoded.Objects) != 1 {
		t.Fatalf("decoded sizes = sessions %d objects %d", len(decoded.Sessions), len(decoded.Objects))
	}
	if !bytes.Equal(decoded.Objects[rootID], bundle.Objects[rootID]) {
		t.Fatal("object frame changed across archive roundtrip")
	}
}

func TestWriteIsDeterministicForFixedManifest(t *testing.T) {
	bundle := validTestBundle(t)
	var first, second bytes.Buffer
	if err := Write(&first, bundle); err != nil {
		t.Fatal(err)
	}
	if err := Write(&second, bundle); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(first.Bytes(), second.Bytes()) {
		t.Fatal("Write() produced different bytes for identical bundle")
	}
}

func TestReadRejectsPathTraversalBeforeParsingArchiveData(t *testing.T) {
	var encoded bytes.Buffer
	zw := zip.NewWriter(&encoded)
	entry, err := zw.Create("../escape")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := entry.Write([]byte("x")); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	_, err = Read(bytes.NewReader(encoded.Bytes()), int64(encoded.Len()))
	if err == nil || !strings.Contains(err.Error(), "unsafe archive path") {
		t.Fatalf("Read() error = %v, want unsafe path rejection", err)
	}
}

func TestValidateArchiveEntrySetRejectsUnexpectedPayload(t *testing.T) {
	bundle := validTestBundle(t)
	manifest := bundle.Manifest
	files := make(map[string]*zip.File)
	files[ManifestPath] = &zip.File{}
	files[ChecksumsPath] = &zip.File{}
	files["sessions/"+manifest.Sessions[0].ID+"/session.json"] = &zip.File{}
	files["sessions/"+manifest.Sessions[0].ID+"/events.ndjson.zst"] = &zip.File{}
	files["sessions/"+manifest.Sessions[0].ID+"/states.ndjson.zst"] = &zip.File{}
	files["sessions/"+manifest.Sessions[0].ID+"/artifacts.ndjson.zst"] = &zip.File{}
	files["unexpected.bin"] = &zip.File{}
	if err := validateArchiveEntrySet(files, manifest); err == nil || !strings.Contains(err.Error(), "unexpected entry") {
		t.Fatalf("validateArchiveEntrySet() error = %v", err)
	}
}

func TestValidateObjectGraphsRejectsMissingReachableChild(t *testing.T) {
	missing := store.SumObject([]byte("missing"))
	treePayload, err := state.CanonicalBytes(state.NewTree([]state.Entry{{
		Name:     []byte("file"),
		Kind:     state.EntryFile,
		Mode:     0o644,
		Size:     7,
		ObjectID: missing,
	}}))
	if err != nil {
		t.Fatal(err)
	}
	framed, err := store.EncodeObject(store.ObjectTree, treePayload)
	if err != nil {
		t.Fatal(err)
	}
	rootID := store.SumObject(framed)
	bundle := Bundle{
		Sessions: []SessionData{{States: []State{{ID: "st_x", RootTreeID: rootID.String()}}}},
		Objects:  map[string][]byte{rootID.String(): framed},
	}
	if err := ValidateObjectGraphs(bundle); err == nil || !strings.Contains(err.Error(), "omits child object") {
		t.Fatalf("ValidateObjectGraphs() error = %v", err)
	}
}

func validTestBundle(t *testing.T) Bundle {
	t.Helper()
	treePayload, err := state.CanonicalBytes(state.NewTree(nil))
	if err != nil {
		t.Fatal(err)
	}
	framed, err := store.EncodeObject(store.ObjectTree, treePayload)
	if err != nil {
		t.Fatal(err)
	}
	rootID := store.SumObject(framed)
	sessionID, err := id.NewSession()
	if err != nil {
		t.Fatal(err)
	}
	stateID, err := id.NewState()
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 8, 30, 8, 0, 0, 0, time.UTC)
	descriptor := SessionDescriptor{ID: sessionID.String()}
	manifest := NewManifest("test", "off", []SessionDescriptor{descriptor})
	manifest.CreatedAt = now
	eventLine, err := json.Marshal(map[string]any{
		"schema":        "rappid.replay.event/1",
		"session_id":    sessionID.String(),
		"seq":           1,
		"wall_time_utc": now,
		"monotonic_ns":  1,
		"type":          "state.snapshot",
		"source":        "test",
		"state_after":   stateID.String(),
		"payload":       map[string]any{},
		"privacy":       map[string]any{"classification": "technical"},
	})
	if err != nil {
		t.Fatal(err)
	}
	return Bundle{
		Manifest: manifest,
		Sessions: []SessionData{{
			Metadata: Session{
				ID:                   sessionID.String(),
				Status:               "completed",
				Command:              []string{"echo"},
				CWD:                  "/tmp",
				StartedAt:            now,
				EndedAt:              now,
				InitialStateID:       stateID.String(),
				FinalStateID:         stateID.String(),
				ReproducibilityLevel: "R1",
			},
			Events: []json.RawMessage{eventLine},
			States: []State{{
				ID:         stateID.String(),
				SessionID:  sessionID.String(),
				EventSeq:   1,
				RootTreeID: rootID.String(),
				CreatedAt:  now,
			}},
		}},
		Objects: map[string][]byte{rootID.String(): framed},
	}
}
