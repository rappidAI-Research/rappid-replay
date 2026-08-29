package record

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"sync"

	"github.com/rappidAI-Research/rappid-replay/internal/event"
	"github.com/rappidAI-Research/rappid-replay/internal/persistence"
	"github.com/rappidAI-Research/rappid-replay/internal/state"
)

const (
	recorderSource             = "recorder.generic"
	maxTerminalBufferedSegment = 64 << 10
)

var oversizedTerminalMarker = []byte("[REDACTED: terminal segment exceeded privacy buffer]")

type eventSink struct {
	ctx       context.Context
	db        *persistence.DB
	sessionID string
	clock     *runClock

	mu       sync.Mutex
	firstErr error
}

func newEventSink(ctx context.Context, db *persistence.DB, sessionID string, clock *runClock) *eventSink {
	return &eventSink{ctx: ctx, db: db, sessionID: sessionID, clock: clock}
}

func (s *eventSink) append(eventType string, payload any) error {
	return s.appendTechnical(eventType, payload, false)
}

func (s *eventSink) appendTechnical(eventType string, payload any, redacted bool) error {
	return s.appendWithPrivacy(eventType, payload, event.Privacy{Classification: "technical", Redacted: redacted})
}

func (s *eventSink) appendWithPrivacy(eventType string, payload any, privacyMetadata event.Privacy) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.firstErr != nil {
		return s.firstErr
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		s.firstErr = fmt.Errorf("encode %s payload: %w", eventType, err)
		return s.firstErr
	}
	wall, monotonic := s.clock.sample()
	draft := event.NewDraft(
		s.sessionID,
		eventType,
		recorderSource,
		wall,
		privacyMetadata,
		encoded,
	)
	if _, err := s.db.AppendEvent(s.ctx, draft, monotonic); err != nil {
		s.firstErr = fmt.Errorf("append %s event: %w", eventType, err)
		return s.firstErr
	}
	return nil
}

// publishSnapshot serializes state.snapshot publication with every other event
// emitted through this sink. The monotonic clock sample is taken only after the
// shared lock is held, so concurrent terminal output and reconciliation cannot
// commit a newer event before a snapshot carrying an older monotonic timestamp.
func (s *eventSink) publishSnapshot(
	ctx context.Context,
	cas state.InspectableObjectStore,
	req persistence.PublishSnapshotRequest,
) (persistence.PublishedSnapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.firstErr != nil {
		return persistence.PublishedSnapshot{}, s.firstErr
	}
	wall, monotonic := s.clock.sample()
	req.WallTimeUTC = wall
	req.MonotonicNS = monotonic
	published, err := s.db.PublishSnapshot(ctx, cas, req)
	if err != nil {
		s.firstErr = fmt.Errorf("publish %s snapshot: %w", req.Role, err)
		return persistence.PublishedSnapshot{}, s.firstErr
	}
	return published, nil
}

// publishArtifact serializes artifact.discovered publication with terminal,
// process, filesystem, and snapshot events. The SQLite artifact row and its
// event are committed atomically by persistence.PublishArtifact.
func (s *eventSink) publishArtifact(
	ctx context.Context,
	req persistence.PublishArtifactRequest,
) (persistence.PublishedArtifact, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.firstErr != nil {
		return persistence.PublishedArtifact{}, s.firstErr
	}
	wall, monotonic := s.clock.sample()
	req.WallTimeUTC = wall
	req.MonotonicNS = monotonic
	published, err := s.db.PublishArtifact(ctx, req)
	if err != nil {
		s.firstErr = fmt.Errorf("publish artifact discovery: %w", err)
		return persistence.PublishedArtifact{}, s.firstErr
	}
	return published, nil
}

func (s *eventSink) err() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.firstErr
}

type streamEventWriter struct {
	sink      *eventSink
	stream    string
	output    io.Writer
	redaction adapterRedactionPolicy

	mu      sync.Mutex
	pending []byte
}

func (w *streamEventWriter) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	n := len(p)
	var outputErr error
	if w.output != nil {
		n, outputErr = w.output.Write(p)
		if n < 0 || n > len(p) {
			return 0, fmt.Errorf("terminal %s output writer returned invalid byte count %d", w.stream, n)
		}
	}
	if n > 0 {
		w.capture(p[:n])
	}
	if outputErr != nil {
		return n, outputErr
	}
	if n != len(p) {
		return n, io.ErrShortWrite
	}
	return n, nil
}

func (w *streamEventWriter) capture(p []byte) {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.pending = append(w.pending, p...)
	for len(w.pending) > 0 {
		if newline := bytes.IndexByte(w.pending, '\n'); newline >= 0 {
			segment := bytes.Clone(w.pending[:newline+1])
			w.pending = append(w.pending[:0], w.pending[newline+1:]...)
			_ = w.emitSegment(segment)
			continue
		}
		if len(w.pending) > maxTerminalBufferedSegment {
			originalBytes := len(w.pending)
			w.pending = w.pending[:0]
			_ = w.emitPersisted(oversizedTerminalMarker, originalBytes, true, "unterminated-segment-limit")
		}
		break
	}
}

// Flush persists a final unterminated stream segment after the child process
// and os/exec's copy goroutines have completed.
func (w *streamEventWriter) Flush() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.pending) == 0 {
		return w.sink.err()
	}
	segment := bytes.Clone(w.pending)
	w.pending = w.pending[:0]
	_ = w.emitSegment(segment)
	return w.sink.err()
}

func (w *streamEventWriter) emitSegment(segment []byte) error {
	persisted, redacted := w.redaction.redact(segment)
	reason := ""
	if redacted {
		reason = "privacy-filter"
	}
	return w.emitPersisted(persisted, len(segment), redacted, reason)
}

func (w *streamEventWriter) emitPersisted(persisted []byte, originalBytes int, redacted bool, reason string) error {
	payload := struct {
		Encoding    string `json:"encoding"`
		DataB64     string `json:"data_b64"`
		Bytes       int    `json:"bytes"`
		StoredBytes int    `json:"stored_bytes"`
		Redaction   string `json:"redaction,omitempty"`
	}{
		Encoding:    "base64",
		DataB64:     base64.StdEncoding.EncodeToString(persisted),
		Bytes:       originalBytes,
		StoredBytes: len(persisted),
		Redaction:   reason,
	}
	return w.sink.appendWithPrivacy(
		"terminal."+w.stream,
		payload,
		event.Privacy{Classification: "content", Redacted: redacted},
	)
}
