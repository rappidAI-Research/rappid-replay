package record

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"sync"

	"github.com/rappidAI-Research/rappid-replay/internal/event"
	"github.com/rappidAI-Research/rappid-replay/internal/persistence"
)

const recorderSource = "recorder.generic"

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
		event.Privacy{Classification: "technical"},
		encoded,
	)
	if _, err := s.db.AppendEvent(s.ctx, draft, monotonic); err != nil {
		s.firstErr = fmt.Errorf("append %s event: %w", eventType, err)
		return s.firstErr
	}
	return nil
}

func (s *eventSink) err() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.firstErr
}

type streamEventWriter struct {
	sink   *eventSink
	stream string
	output io.Writer
}

func (w streamEventWriter) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	n := len(p)
	if w.output != nil {
		var err error
		n, err = w.output.Write(p)
		if err != nil {
			return n, err
		}
		if n != len(p) {
			return n, io.ErrShortWrite
		}
	}

	// Stream persistence failures are remembered by the sink but deliberately do
	// not break the child's stdout/stderr pipe. The recorder can then wait for the
	// process, preserve as much evidence as possible, and terminate the session as
	// aborted instead of perturbing the recorded process with an artificial EPIPE.
	_ = w.sink.append("terminal."+w.stream, struct {
		Encoding string `json:"encoding"`
		DataB64  string `json:"data_b64"`
		Bytes    int    `json:"bytes"`
	}{
		Encoding: "base64",
		DataB64:  base64.StdEncoding.EncodeToString(p[:n]),
		Bytes:    n,
	})
	return n, nil
}
