package persistence

import (
	"context"
	"encoding/json"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rappidAI-Research/rappid-replay/internal/event"
	"github.com/rappidAI-Research/rappid-replay/internal/id"
)

func TestAppendEventOwnsSequenceAndRejectsMonotonicRegression(t *testing.T) {
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

	firstDraft := event.NewDraft(sessionID.String(), "session.started", "replay.core", now, event.Privacy{Classification: "technical"}, json.RawMessage(`{"ok":true}`))
	first, err := db.AppendEvent(ctx, firstDraft, 100)
	if err != nil {
		t.Fatalf("AppendEvent(first) error = %v", err)
	}
	if first.Seq != 1 {
		t.Fatalf("first seq = %d, want 1", first.Seq)
	}

	secondDraft := event.NewDraft(sessionID.String(), "process.started", "replay.core", now.Add(time.Millisecond), event.Privacy{Classification: "technical"}, nil)
	if _, err := db.AppendEvent(ctx, secondDraft, 99); err == nil || !strings.Contains(err.Error(), "precedes previous") {
		t.Fatalf("AppendEvent(monotonic regression) error = %v", err)
	}
	second, err := db.AppendEvent(ctx, secondDraft, 101)
	if err != nil {
		t.Fatalf("AppendEvent(second) error = %v", err)
	}
	if second.Seq != 2 {
		t.Fatalf("second seq = %d, want 2 after rolled-back failure", second.Seq)
	}
}

func TestAppendEventRejectsStateSnapshotAndForeignStateReference(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, filepath.Join(t.TempDir(), "replay.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	firstSession, _ := id.NewSession()
	secondSession, _ := id.NewSession()
	now := time.Now().UTC()
	for _, sessionID := range []id.SessionID{firstSession, secondSession} {
		if err := db.CreateSession(ctx, SessionStart{ID: sessionID, Command: []string{"agent"}, CWD: t.TempDir(), StartedAt: now}); err != nil {
			t.Fatal(err)
		}
	}

	snapshotDraft := event.NewDraft(firstSession.String(), "state.snapshot", "replay.core", now, event.Privacy{Classification: "technical"}, nil)
	if _, err := db.AppendEvent(ctx, snapshotDraft, 1); err == nil {
		t.Fatal("AppendEvent() accepted state.snapshot")
	}

	foreignState, _ := id.NewState()
	if _, err := db.sql.ExecContext(ctx,
		"INSERT INTO states(id, session_id, event_seq, root_object_id) VALUES(?, ?, NULL, ?)",
		foreignState.String(), secondSession.String(), "b3:0000000000000000000000000000000000000000000000000000000000000000",
	); err == nil {
		// The FK to objects intentionally prevents this shortcut; insert a
		// syntactically valid state reference is not necessary for the behavior
		// under test. A random valid StateID absent from firstSession is enough.
	}

	draft := event.NewDraft(firstSession.String(), "process.started", "replay.core", now, event.Privacy{Classification: "technical"}, nil)
	draft.StateBefore = foreignState.String()
	if _, err := db.AppendEvent(ctx, draft, 1); err == nil || !strings.Contains(err.Error(), "does not belong") {
		t.Fatalf("AppendEvent(foreign state) error = %v", err)
	}
}

func TestAppendEventConcurrentSequencesAreUniqueAndContiguous(t *testing.T) {
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

	const count = 12
	seqs := make(chan uint64, count)
	errs := make(chan error, count)
	var wg sync.WaitGroup
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			draft := event.NewDraft(sessionID.String(), "process.output", "test", now.Add(time.Duration(i)*time.Millisecond), event.Privacy{Classification: "technical"}, json.RawMessage(`{"stream":"stdout"}`))
			persisted, err := db.AppendEvent(ctx, draft, uint64(100+i))
			if err != nil {
				errs <- err
				return
			}
			seqs <- persisted.Seq
		}(i)
	}
	wg.Wait()
	close(seqs)
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent AppendEvent() error = %v", err)
	}

	got := make([]int, 0, count)
	for seq := range seqs {
		got = append(got, int(seq))
	}
	sort.Ints(got)
	if len(got) != count {
		t.Fatalf("sequence result count = %d, want %d", len(got), count)
	}
	for i, seq := range got {
		if seq != i+1 {
			t.Fatalf("sorted sequences = %v, want contiguous 1..%d", got, count)
		}
	}
}
