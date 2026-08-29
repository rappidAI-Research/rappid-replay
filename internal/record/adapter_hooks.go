package record

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/rappidAI-Research/rappid-replay/internal/event"
	"github.com/rappidAI-Research/rappid-replay/internal/privacy"
	"github.com/rappidAI-Research/rappid-replay/pkg/adapter"
)

const (
	maxAdapterAttributes = 128
	maxAdapterHints      = 256
	maxAdapterLiteral    = 4 << 10
	maxAdapterEventBytes = 1 << 20
	adapterStopGrace     = 500 * time.Millisecond
)

var adapterAttributeKeyPattern = regexp.MustCompile(`^[a-z][a-z0-9]*(?:[._-][a-z0-9]+)*$`)

type adapterRedactionPolicy struct {
	environmentNames map[string]struct{}
	literals         [][]byte
}

func (p adapterRedactionPolicy) redact(data []byte) ([]byte, bool) {
	redacted, changed := privacy.RedactKnownSecrets(data)
	for _, literal := range p.literals {
		if len(literal) == 0 || !bytes.Contains(redacted, literal) {
			continue
		}
		redacted = bytes.ReplaceAll(redacted, literal, []byte("[REDACTED]"))
		changed = true
	}
	return redacted, changed
}

func (p adapterRedactionPolicy) redactEnvironment(name, value string) (string, bool) {
	if privacy.SensitiveEnvironmentName(name) {
		return privacy.EnvironmentRedactionMarker, true
	}
	if _, ok := p.environmentNames[strings.ToUpper(strings.TrimSpace(name))]; ok {
		return privacy.EnvironmentRedactionMarker, true
	}
	redacted, changed := p.redact([]byte(value))
	return string(redacted), changed
}

type adapterHookBridge struct {
	selected adapter.Adapter
	desc     adapter.Descriptor
	run      adapter.RunContext
	sink     *eventSink

	redaction adapterRedactionPolicy

	streamMu     sync.Mutex
	streamCancel context.CancelFunc
	streamDone   chan struct{}
}

func newAdapterHookBridge(selection adapter.Selection, sessionID, workingDir string, command []string, sink *eventSink) *adapterHookBridge {
	return &adapterHookBridge{
		selected: selection.Adapter,
		desc:     selection.Descriptor,
		run: adapter.RunContext{
			SessionID:  sessionID,
			Command:    append([]string(nil), command...),
			WorkingDir: workingDir,
		},
		sink: sink,
		redaction: adapterRedactionPolicy{
			environmentNames: make(map[string]struct{}),
		},
	}
}

func (b *adapterHookBridge) source() string {
	if b == nil || b.desc.ID == "" {
		return recorderSource
	}
	return "adapter." + b.desc.ID
}

func (b *adapterHookBridge) loadRedactionHints(ctx context.Context) error {
	if b == nil || b.selected == nil {
		return nil
	}
	hints, err := b.selected.RedactionHints(ctx, b.run)
	if err != nil {
		return b.reportError("redaction_hints", err)
	}
	if len(hints) > maxAdapterHints {
		return b.reportError("redaction_hints", fmt.Errorf("adapter returned %d hints, limit is %d", len(hints), maxAdapterHints))
	}

	environmentNames := make(map[string]struct{})
	literalSet := make(map[string]struct{})
	literals := make([][]byte, 0, len(hints))
	for _, hint := range hints {
		value := strings.TrimSpace(hint.Value)
		switch hint.Kind {
		case adapter.RedactEnvironmentName:
			if value == "" || len(value) > 256 || strings.ContainsAny(value, "=\x00") {
				if err := b.reportError("redaction_hints", fmt.Errorf("invalid environment-name hint")); err != nil {
					return err
				}
				continue
			}
			environmentNames[strings.ToUpper(value)] = struct{}{}
		case adapter.RedactLiteral:
			if len(hint.Value) < 4 || len(hint.Value) > maxAdapterLiteral {
				if err := b.reportError("redaction_hints", fmt.Errorf("literal hint length must be between 4 and %d bytes", maxAdapterLiteral)); err != nil {
					return err
				}
				continue
			}
			if _, exists := literalSet[hint.Value]; exists {
				continue
			}
			literalSet[hint.Value] = struct{}{}
			literals = append(literals, []byte(hint.Value))
		default:
			if err := b.reportError("redaction_hints", fmt.Errorf("unsupported redaction hint kind %q", hint.Kind)); err != nil {
				return err
			}
		}
	}
	sort.Slice(literals, func(i, j int) bool {
		if len(literals[i]) != len(literals[j]) {
			return len(literals[i]) > len(literals[j])
		}
		return bytes.Compare(literals[i], literals[j]) < 0
	})
	b.redaction = adapterRedactionPolicy{environmentNames: environmentNames, literals: literals}
	return nil
}

func (b *adapterHookBridge) emitEnvironment(ctx context.Context) error {
	if b == nil || b.selected == nil {
		return nil
	}
	metadata, err := b.selected.Environment(ctx, b.run)
	if err != nil {
		return b.reportError("environment", err)
	}
	attributes, redacted, err := b.sanitizeAttributes(metadata.Attributes)
	if err != nil {
		return b.reportError("environment", err)
	}
	if len(attributes) == 0 {
		return nil
	}
	return b.sink.appendWithSourceAndPrivacy(b.source(), "agent.environment", struct {
		Adapter    adapter.Descriptor `json:"adapter"`
		Attributes map[string]string  `json:"attributes"`
	}{Adapter: b.desc, Attributes: attributes}, event.Privacy{Classification: "technical", Redacted: redacted})
}

func (b *adapterHookBridge) enrichProcess(ctx context.Context, observation adapter.ProcessObservation) error {
	if b == nil || b.selected == nil {
		return nil
	}
	enrichment, err := b.selected.EnrichProcess(ctx, b.run, observation)
	if err != nil {
		return b.reportError("process_enrichment", err)
	}
	attributes, redacted, err := b.sanitizeAttributes(enrichment.Attributes)
	if err != nil {
		return b.reportError("process_enrichment", err)
	}
	if len(attributes) == 0 {
		return nil
	}
	return b.sink.appendWithSourceAndPrivacy(b.source(), "agent.process.enriched", struct {
		Adapter    adapter.Descriptor `json:"adapter"`
		PID        int                `json:"pid"`
		Attributes map[string]string  `json:"attributes"`
	}{Adapter: b.desc, PID: observation.PID, Attributes: attributes}, event.Privacy{Classification: "technical", Redacted: redacted})
}

func (b *adapterHookBridge) startEventStream(ctx context.Context) {
	if b == nil || b.selected == nil {
		return
	}
	b.streamMu.Lock()
	defer b.streamMu.Unlock()
	if b.streamDone != nil {
		return
	}
	streamCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	b.streamCancel = cancel
	b.streamDone = done
	go func() {
		defer close(done)
		err := b.selected.StreamEvents(streamCtx, b.run, b.emitAdapterEvent)
		if err != nil && streamCtx.Err() == nil {
			_ = b.reportError("event_stream", err)
		}
	}()
}

func (b *adapterHookBridge) stopEventStream() error {
	if b == nil {
		return nil
	}
	b.streamMu.Lock()
	cancel := b.streamCancel
	done := b.streamDone
	b.streamMu.Unlock()
	if done == nil {
		return nil
	}
	if cancel != nil {
		cancel()
	}
	select {
	case <-done:
		return b.sink.err()
	case <-time.After(adapterStopGrace):
		if err := b.reportError("event_stream", fmt.Errorf("adapter did not stop within %s after cancellation", adapterStopGrace)); err != nil {
			return err
		}
		return b.sink.err()
	}
}

func (b *adapterHookBridge) emitAdapterEvent(ctx context.Context, proposed adapter.AdapterEvent) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !strings.HasPrefix(proposed.Type, "agent.") && !strings.HasPrefix(proposed.Type, "external.") {
		return fmt.Errorf("adapter event type %q is outside allowed agent.* or external.* namespaces", proposed.Type)
	}
	payload := proposed.Payload
	if len(payload) == 0 {
		payload = json.RawMessage("null")
	}
	if len(payload) > maxAdapterEventBytes {
		return fmt.Errorf("adapter event payload is %d bytes, limit is %d", len(payload), maxAdapterEventBytes)
	}
	if !json.Valid(payload) {
		return fmt.Errorf("adapter event %q payload is not valid JSON", proposed.Type)
	}
	persisted, redacted := b.redaction.redact(payload)
	if !json.Valid(persisted) {
		return fmt.Errorf("adapter event %q became invalid JSON after privacy filtering", proposed.Type)
	}
	return b.sink.appendWithSourceAndPrivacy(
		b.source(), proposed.Type, json.RawMessage(persisted),
		event.Privacy{Classification: "content", Redacted: redacted},
	)
}

func (b *adapterHookBridge) sanitizeAttributes(input map[string]string) (map[string]string, bool, error) {
	if len(input) == 0 {
		return nil, false, nil
	}
	if len(input) > maxAdapterAttributes {
		return nil, false, fmt.Errorf("adapter returned %d attributes, limit is %d", len(input), maxAdapterAttributes)
	}
	prefix := b.desc.ID + "."
	out := make(map[string]string, len(input))
	redactedAny := false
	for key, value := range input {
		if !adapterAttributeKeyPattern.MatchString(key) || !strings.HasPrefix(key, prefix) {
			return nil, false, fmt.Errorf("adapter attribute %q must be a valid key namespaced by %q", key, prefix)
		}
		redacted, changed := b.redaction.redact([]byte(value))
		out[key] = string(redacted)
		redactedAny = redactedAny || changed
	}
	return out, redactedAny, nil
}

func (b *adapterHookBridge) reportError(stage string, cause error) error {
	if b == nil || b.sink == nil || cause == nil {
		return nil
	}
	message, redacted := b.redaction.redact([]byte(cause.Error()))
	return b.sink.appendWithSourceAndPrivacy(b.source(), "agent.adapter.error", struct {
		Adapter adapter.Descriptor `json:"adapter"`
		Stage   string             `json:"stage"`
		Message string             `json:"message"`
	}{Adapter: b.desc, Stage: stage, Message: string(message)}, event.Privacy{Classification: "technical", Redacted: redacted})
}
