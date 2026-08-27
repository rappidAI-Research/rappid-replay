package event

import (
	"encoding/json"
	"testing"
	"time"
)

func TestNewUsesCanonicalSchemaAndUTC(t *testing.T) {
	when := time.Date(2026, time.August, 27, 18, 0, 0, 0, time.FixedZone("CEST", 2*60*60))
	e := New("rp_test", "session.started", "core", when, Privacy{Classification: "metadata"}, json.RawMessage(`{"ok":true}`))

	if e.Schema != SchemaV1 {
		t.Fatalf("schema = %q, want %q", e.Schema, SchemaV1)
	}
	if e.WallTimeUTC.Location() != time.UTC {
		t.Fatalf("wall time location = %v, want UTC", e.WallTimeUTC.Location())
	}
}
