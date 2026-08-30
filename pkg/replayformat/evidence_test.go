package replayformat

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/rappidAI-Research/rappid-replay/internal/id"
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
	parent := validTestBundle(t)
	parentData := parent.Sessions[0]
	parentDescriptor := parent.Manifest.Sessions[0]

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

	other := validTestBundle(t)
	otherRoot := other.Sessions[0].States[0].RootTreeID
	if otherRoot == parentData.States[0].RootTreeID {
		t.Fatal("test fixture unexpectedly reused root object")
	}
	childData.States[0].RootTreeID = otherRoot
	for objectID, framed := range other.Objects {
		parent.Objects[objectID] = framed
	}

	parent.Sessions = append(parent.Sessions, childData)
	parent.Manifest.Sessions = append(parent.Manifest.Sessions, SessionDescriptor{
		ID:              childSessionID.String(),
		ParentSessionID: parentDescriptor.ID,
		ForkEventSeq:    parentData.States[0].EventSeq,
	})

	if err := ValidateObjectGraphs(parent); err == nil || !strings.Contains(err.Error(), "differs from parent fork root") {
		t.Fatalf("ValidateObjectGraphs() error = %v, want branch root mismatch", err)
	}
}
