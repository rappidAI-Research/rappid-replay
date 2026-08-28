package persistence

import (
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/rappidAI-Research/rappid-replay/internal/event"
	"github.com/rappidAI-Research/rappid-replay/internal/id"
)

func TestAppendEventRejectsStandaloneArtifactDiscovery(t *testing.T) {
	db, err := Open(context.Background(), filepath.Join(t.TempDir(), "replay.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	sessionID, err := id.NewSession()
	if err != nil {
		t.Fatal(err)
	}
	draft := event.NewDraft(
		sessionID.String(),
		ArtifactEventType,
		"recorder.generic",
		time.Now().UTC(),
		event.Privacy{Classification: "technical"},
		json.RawMessage("{}"),
	)
	if _, err := db.AppendEvent(context.Background(), draft, 1); err == nil || !strings.Contains(err.Error(), "PublishArtifact") {
		t.Fatalf("AppendEvent() error = %v, want PublishArtifact requirement", err)
	}
}
