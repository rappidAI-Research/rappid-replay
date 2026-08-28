package event

import (
	"encoding/json"
	"testing"
	"time"
)

func TestDraftRequiresExplicitStampingForCanonicalEnvelope(t *testing.T) {
	when := time.Date(2026, time.August, 27, 18, 0, 0, 0, time.FixedZone("CEST", 2*60*60))
	draft := NewDraft("rp_test", "session.started", "core", when, Privacy{Classification: "metadata"}, json.RawMessage(`{"ok":true}`))

	if draft.WallTimeUTC.Location() != time.UTC {
		t.Fatalf("draft wall time location = %v, want UTC", draft.WallTimeUTC.Location())
	}
	e := draft.Stamp(7, 1234)
	if e.Schema != SchemaV1 {
		t.Fatalf("schema = %q, want %q", e.Schema, SchemaV1)
	}
	if e.Seq != 7 || e.MonotonicNS != 1234 {
		t.Fatalf("stamp = seq %d monotonic %d", e.Seq, e.MonotonicNS)
	}
	if e.WallTimeUTC.Location() != time.UTC {
		t.Fatalf("event wall time location = %v, want UTC", e.WallTimeUTC.Location())
	}
}
