package replayformat

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/rappidAI-Research/rappid-replay/internal/id"
	"github.com/rappidAI-Research/rappid-replay/internal/state"
	"github.com/rappidAI-Research/rappid-replay/internal/store"
)

func TestValidateObjectGraphsRejectsMalformedEventEnvelope(t *testing.T) {
	bundle := validTestBundle(t)
	var raw map[string]any
	if err := json.Unmarshal(bundle.Sessions[0].Events[0], &raw); err != nil {
		t.Fatal(err)
	}
	raw["seq"] = 2
	encoded, err := json.Marshal(raw)
	if err != nil {
		t.Fatal(err)
	}
	bundle.Sessions[0].Events[0] = encoded

	if err := ValidateObjectGraphs(bundle); err == nil || !strings.Contains(err.Error(), "invalid envelope identity") {
		t.Fatalf("ValidateObjectGraphs() error = %v, want event identity rejection", err)
	}
}

func TestValidateObjectGraphsRejectsBrokenBranchRoot(t *testing.T) {
	bundle := validTestBundle(t)
	parentData := bundle.Sessions[0]
	parentDescriptor := bundle.Manifest.Sessions[0]

	childSessionID, err := id.NewSession()
	if err != nil {
		t.Fatal(err)
	}
	childStateID, err := id.NewState()
	if err != nil {
		t.Fatal(err)
	}
	childData := parentData
	childData.Metadata.ID = childSessionID.String()
	childData.Metadata.ParentSessionID = parentData.Metadata.ID
	childData.Metadata.ForkEventSeq = parentData.States[0].EventSeq
	childData.Metadata.InitialStateID = childStateID.String()
	childData.Metadata.FinalStateID = childStateID.String()
	childData.States = append([]State(nil), parentData.States...)
	childData.States[0].ID = childStateID.String()
	childData.States[0].SessionID = childSessionID.String()

	var raw map[string]any
	if err := json.Unmarshal(parentData.Events[0], &raw); err != nil {
		t.Fatal(err)
	}
	raw["session_id"] = childSessionID.String()
	raw["state_after"] = childStateID.String()
	encoded, err := json.Marshal(raw)
	if err != nil {
		t.Fatal(err)
	}
	childData.Events = []json.RawMessage{encoded}

	linkFrame, err := store.EncodeObject(store.ObjectLink, []byte("target"))
	if err != nil {
		t.Fatal(err)
	}
	linkID := store.SumObject(linkFrame)
	treePayload, err := state.CanonicalBytes(state.NewTree([]state.Entry{{
		Name:     []byte("link"),
		Kind:     state.EntrySymlink,
		Mode:     0o777,
		Size:     int64(len("target")),
		ObjectID: linkID,
	}}))
	if err != nil {
		t.Fatal(err)
	}
	treeFrame, err := store.EncodeObject(store.ObjectTree, treePayload)
	if err != nil {
		t.Fatal(err)
	}
	childRoot := store.SumObject(treeFrame)
	childData.States[0].RootTreeID = childRoot.String()
	bundle.Objects[linkID.String()] = linkFrame
	bundle.Objects[childRoot.String()] = treeFrame

	bundle.Sessions = append(bundle.Sessions, childData)
	bundle.Manifest.Sessions = append(bundle.Manifest.Sessions, SessionDescriptor{
		ID:              childSessionID.String(),
		ParentSessionID: parentDescriptor.ID,
		ForkEventSeq:    parentData.States[0].EventSeq,
	})

	if err := ValidateObjectGraphs(bundle); err == nil || !strings.Contains(err.Error(), "differs from parent fork root") {
		t.Fatalf("ValidateObjectGraphs() error = %v, want branch root mismatch", err)
	}
}
