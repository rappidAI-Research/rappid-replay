package replayformat

import (
	"archive/zip"
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/rappidAI-Research/rappid-replay/internal/state"
	"github.com/rappidAI-Research/rappid-replay/internal/store"
)

func TestArchiveRoundTripValidatesObjectGraph(t *testing.T) {
	treePayload, err := state.CanonicalBytes(state.NewTree(nil))
	if err != nil {
		t.Fatal(err)
	}
	framed, err := store.EncodeObject(store.ObjectTree, treePayload)
	if err != nil {
		t.Fatal(err)
	}
	rootID := store.SumObject(framed)
	now := time.Date(2026, 8, 30, 8, 0, 0, 0, time.UTC)
	sessionID := "se_0198f3c2-6d77-7b12-8d50-f9f4433c1201"
	stateID := "st_0198f3c2-6d77-7b12-8d50-f9f4433c1202"
	descriptor := SessionDescriptor{ID: sessionID}
	manifest := NewManifest("test", "off", []SessionDescriptor{descriptor})
	manifest.CreatedAt = now
	eventLine, _ := json.Marshal(map[string]any{
		"schema": "rappid.replay.event/1", "session_id": sessionID, "seq": 1,
		"wall_time_utc": now, "monotonic_ns": 1, "type": "state.snapshot", "source": "test",
		"state_after": stateID, "payload": map[string]any{}, "privacy": map[string]any{"classification": "technical"},
	})
	bundle := Bundle{
		Manifest: manifest,
		Sessions: []SessionData{{
			Metadata: Session{ID: sessionID, Status: "completed", Command: []string{"echo"}, CWD: "/tmp", StartedAt: now, EndedAt: now, InitialStateID: stateID, FinalStateID: stateID, ReproducibilityLevel: "R1"},
			Events: []json.RawMessage{eventLine},
			States: []State{{ID: stateID, SessionID: sessionID, EventSeq: 1, RootTreeID: rootID.String(), CreatedAt: now}},
		}},
		Objects: map[string][]byte{rootID.String(): framed},
	}
	if err := ValidateObjectGraphs(bundle); err != nil {
		t.Fatalf("ValidateObjectGraphs() error = %v", err)
	}
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
	if !bytes.Equal(decoded.Objects[rootID.String()], framed) {
		t.Fatal("object frame changed across archive roundtrip")
	}
}

func TestReadRejectsPathTraversalBeforeParsingArchiveData(t *testing.T) {
	var encoded bytes.Buffer
	zw := zip.NewWriter(&encoded)
	entry, err := zw.Create("../escape")
	if err != nil {
		t.Fatal(err)
	}
	_, _ = entry.Write([]byte("x"))
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	_, err = Read(bytes.NewReader(encoded.Bytes()), int64(encoded.Len()))
	if err == nil || !strings.Contains(err.Error(), "unsafe archive path") {
		t.Fatalf("Read() error = %v, want unsafe path rejection", err)
	}
}

func TestValidateObjectGraphsRejectsMissingReachableChild(t *testing.T) {
	missing := store.SumObject([]byte("missing"))
	treePayload, err := state.CanonicalBytes(state.NewTree([]state.Entry{{Name: []byte("file"), Kind: state.EntryFile, Mode: 0o644, Size: 7, ObjectID: missing}}))
	if err != nil {
		t.Fatal(err)
	}
	framed, err := store.EncodeObject(store.ObjectTree, treePayload)
	if err != nil {
		t.Fatal(err)
	}
	rootID := store.SumObject(framed)
	bundle := Bundle{Sessions: []SessionData{{States: []State{{ID: "st_x", RootTreeID: rootID.String()}}}}, Objects: map[string][]byte{rootID.String(): framed}}
	if err := ValidateObjectGraphs(bundle); err == nil || !strings.Contains(err.Error(), "omits child object") {
		t.Fatalf("ValidateObjectGraphs() error = %v", err)
	}
}
