package persistence

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/rappidAI-Research/rappid-replay/internal/event"
	"github.com/rappidAI-Research/rappid-replay/internal/id"
)

func TestListEventsReturnsValidatedSequenceOrder(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, filepath.Join(t.TempDir(), "replay.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	sessionID, _ := id.NewSession()
	now := time.Now().UTC()
	if err := db.CreateSession(ctx, SessionStart{ID: sessionID, Command: []string{"agent"}, CWD: t.TempDir(), StartedAt: now}); err != nil {
		t.Fatal(err)
	}
	for i, eventType := range []string{"process.started", "process.exited"} {
		draft := event.NewDraft(sessionID.String(), eventType, "test", now.Add(time.Duration(i)*time.Millisecond), event.Privacy{Classification: "technical"}, json.RawMessage(`{"ok":true}`))
		if _, err := db.AppendEvent(ctx, draft, uint64(i+1)); err != nil {
			t.Fatal(err)
		}
	}

	events, err := db.ListEvents(ctx, sessionID)
	if err != nil {
		t.Fatalf("ListEvents() error = %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("len(events) = %d, want 2", len(events))
	}
	if events[0].Seq != 1 || events[0].Type != "process.started" {
		t.Fatalf("first event = %+v", events[0])
	}
	if events[1].Seq != 2 || events[1].Type != "process.exited" {
		t.Fatalf("second event = %+v", events[1])
	}
	if events[0].SessionID != sessionID.String() || events[0].Schema != event.SchemaV1 {
		t.Fatalf("event envelope not reconstructed canonically: %+v", events[0])
	}
}

func TestListEventsDistinguishesMissingSession(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, filepath.Join(t.TempDir(), "replay.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	missing, _ := id.NewSession()
	if _, err := db.ListEvents(ctx, missing); err == nil || !IsSessionNotFound(err) {
		t.Fatalf("ListEvents(missing) error = %v", err)
	}
}
