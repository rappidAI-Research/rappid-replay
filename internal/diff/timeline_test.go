package diff

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/rappidAI-Research/rappid-replay/internal/event"
)

func TestNormalizeEventsIgnoresProcessRuntimeIdentity(t *testing.T) {
	left := []event.Event{testEvent(1, "process.started", `{"pid":101,"path":"/usr/bin/tool","command":["tool","run"],"cwd":"/tmp/left","pty":false}`)}
	right := []event.Event{testEvent(9, "process.started", `{"pid":999,"path":"/usr/bin/tool","command":["tool","run"],"cwd":"/tmp/right","pty":false}`)}

	leftNormalized, err := normalizeEvents(context.Background(), nil, left)
	if err != nil {
		t.Fatal(err)
	}
	rightNormalized, err := normalizeEvents(context.Background(), nil, right)
	if err != nil {
		t.Fatal(err)
	}
	result := compareTimeline(leftNormalized, rightNormalized)
	if !result.Equal || result.CommonPrefixEvents != 1 {
		t.Fatalf("timeline result = %+v", result)
	}
}

func TestNormalizeEventsRetainsTechnicalCommandDifference(t *testing.T) {
	left := []event.Event{testEvent(1, "process.started", `{"pid":101,"path":"/usr/bin/tool","command":["tool","one"],"cwd":"/tmp/left","pty":false}`)}
	right := []event.Event{testEvent(1, "process.started", `{"pid":102,"path":"/usr/bin/tool","command":["tool","two"],"cwd":"/tmp/right","pty":false}`)}

	leftNormalized, err := normalizeEvents(context.Background(), nil, left)
	if err != nil {
		t.Fatal(err)
	}
	rightNormalized, err := normalizeEvents(context.Background(), nil, right)
	if err != nil {
		t.Fatal(err)
	}
	result := compareTimeline(leftNormalized, rightNormalized)
	if result.Equal || result.CommonPrefixEvents != 0 || result.FirstLeft == nil || result.FirstRight == nil {
		t.Fatalf("timeline result = %+v", result)
	}
}

func TestNormalizeEventsDoesNotStripProviderPayloadFields(t *testing.T) {
	left := []event.Event{testEvent(1, "agent.message", `{"session_id":"provider-a","cwd":"remote-a","pid":1}`)}
	right := []event.Event{testEvent(1, "agent.message", `{"session_id":"provider-b","cwd":"remote-b","pid":2}`)}

	leftNormalized, err := normalizeEvents(context.Background(), nil, left)
	if err != nil {
		t.Fatal(err)
	}
	rightNormalized, err := normalizeEvents(context.Background(), nil, right)
	if err != nil {
		t.Fatal(err)
	}
	if compareTimeline(leftNormalized, rightNormalized).Equal {
		t.Fatal("provider-owned agent payload differences were incorrectly normalized away")
	}
}

func testEvent(seq uint64, eventType, payload string) event.Event {
	return event.Event{
		Schema:      event.SchemaV1,
		SessionID:   "se_test",
		Seq:         seq,
		WallTimeUTC: time.Unix(100, 0).UTC(),
		MonotonicNS: seq,
		Type:        eventType,
		Source:      "test",
		Payload:     json.RawMessage(payload),
		Privacy:     event.Privacy{Classification: "technical"},
	}
}
