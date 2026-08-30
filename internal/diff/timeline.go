package diff

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/rappidAI-Research/rappid-replay/internal/event"
	"github.com/rappidAI-Research/rappid-replay/internal/id"
	"github.com/rappidAI-Research/rappid-replay/internal/persistence"
)

var processRuntimePayloadKeys = map[string]struct{}{
	"pid":        {},
	"ppid":       {},
	"parent_pid": {},
	"root_pid":   {},
	"cwd":        {},
}

var sessionStartRuntimePayloadKeys = map[string]struct{}{
	"cwd":               {},
	"parent_session_id": {},
	"fork_event_seq":    {},
	"fork_state_id":     {},
}

var snapshotRuntimePayloadKeys = map[string]struct{}{
	"state_id": {},
}

var artifactRuntimePayloadKeys = map[string]struct{}{
	"artifact_id":   {},
	"from_state_id": {},
	"state_id":      {},
}

type normalizedEvent struct {
	summary EventSummary
	key     string
}

func normalizeEvents(ctx context.Context, db *persistence.DB, events []event.Event) ([]normalizedEvent, error) {
	roots := make(map[string]string)
	result := make([]normalizedEvent, 0, len(events))
	for _, item := range events {
		beforeRoot, err := resolveStateRoot(ctx, db, item.StateBefore, roots)
		if err != nil {
			return nil, fmt.Errorf("resolve state_before for event %d: %w", item.Seq, err)
		}
		afterRoot, err := resolveStateRoot(ctx, db, item.StateAfter, roots)
		if err != nil {
			return nil, fmt.Errorf("resolve state_after for event %d: %w", item.Seq, err)
		}
		payload, err := normalizedPayload(item.Type, item.Payload)
		if err != nil {
			return nil, fmt.Errorf("normalize payload for event %d: %w", item.Seq, err)
		}
		digest := sha256.Sum256(payload)
		payloadFingerprint := "sha256:" + hex.EncodeToString(digest[:])
		summary := EventSummary{
			Seq:                 item.Seq,
			Type:                item.Type,
			Source:              item.Source,
			StateBeforeRootTree: beforeRoot,
			StateAfterRootTree:  afterRoot,
			PayloadFingerprint:  payloadFingerprint,
			PrivacyClass:        item.Privacy.Classification,
			Redacted:            item.Privacy.Redacted,
		}
		keyBytes, err := json.Marshal(struct {
			Type       string          `json:"type"`
			Source     string          `json:"source"`
			BeforeRoot string          `json:"before_root,omitempty"`
			AfterRoot  string          `json:"after_root,omitempty"`
			Payload    json.RawMessage `json:"payload"`
			Privacy    string          `json:"privacy"`
			Redacted   bool            `json:"redacted"`
		}{
			Type: item.Type, Source: item.Source, BeforeRoot: beforeRoot, AfterRoot: afterRoot,
			Payload: payload, Privacy: item.Privacy.Classification, Redacted: item.Privacy.Redacted,
		})
		if err != nil {
			return nil, fmt.Errorf("encode technical event key: %w", err)
		}
		result = append(result, normalizedEvent{summary: summary, key: string(keyBytes)})
	}
	return result, nil
}

func resolveStateRoot(ctx context.Context, db *persistence.DB, raw string, cache map[string]string) (string, error) {
	if raw == "" {
		return "", nil
	}
	if root, ok := cache[raw]; ok {
		return root, nil
	}
	if db == nil {
		return "", fmt.Errorf("state-bearing event requires Replay database")
	}
	stateID, err := id.ParseState(raw)
	if err != nil {
		return "", err
	}
	record, err := db.GetState(ctx, stateID)
	if err != nil {
		return "", err
	}
	root := record.RootTreeID.String()
	cache[raw] = root
	return root, nil
}

func normalizedPayload(eventType string, raw json.RawMessage) (json.RawMessage, error) {
	if len(raw) == 0 {
		return json.RawMessage("null"), nil
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return nil, fmt.Errorf("trailing JSON value")
		}
		return nil, err
	}
	value = scrubRuntimeIdentity(eventType, value)
	encoded, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	return json.RawMessage(encoded), nil
}

// scrubRuntimeIdentity removes only fields allocated by the current Replay
// execution itself. Removal is event-specific and top-level: arbitrary adapter
// and provider payloads keep same-named fields because those can be meaningful
// evidence. State references in the envelope are separately normalized to the
// referenced root-tree identity.
func scrubRuntimeIdentity(eventType string, value any) any {
	object, ok := value.(map[string]any)
	if !ok {
		return value
	}
	var keys map[string]struct{}
	switch {
	case strings.HasPrefix(eventType, "process."):
		keys = processRuntimePayloadKeys
	case eventType == "session.started":
		keys = sessionStartRuntimePayloadKeys
	case eventType == "state.snapshot":
		keys = snapshotRuntimePayloadKeys
	case eventType == persistence.ArtifactEventType:
		keys = artifactRuntimePayloadKeys
	default:
		return value
	}
	clean := make(map[string]any, len(object))
	for key, child := range object {
		if _, volatile := keys[key]; volatile {
			continue
		}
		clean[key] = child
	}
	return clean
}

func compareTimeline(left, right []normalizedEvent) TimelineDiff {
	prefix := commonPrefix(left, right)
	result := TimelineDiff{
		LeftEvents:         len(left),
		RightEvents:        len(right),
		CommonPrefixEvents: prefix,
		Equal:              prefix == len(left) && prefix == len(right),
		LeftRemaining:      len(left) - prefix,
		RightRemaining:     len(right) - prefix,
	}
	if prefix < len(left) {
		summary := left[prefix].summary
		result.FirstLeft = &summary
	}
	if prefix < len(right) {
		summary := right[prefix].summary
		result.FirstRight = &summary
	}
	return result
}

func compareStream(left, right []normalizedEvent, namespace string) StreamDiff {
	leftFiltered := filterNamespace(left, namespace)
	rightFiltered := filterNamespace(right, namespace)
	prefix := commonPrefix(leftFiltered, rightFiltered)
	result := StreamDiff{
		LeftEvents:         len(leftFiltered),
		RightEvents:        len(rightFiltered),
		CommonPrefixEvents: prefix,
		Equal:              prefix == len(leftFiltered) && prefix == len(rightFiltered),
		LeftTypes:          countTypes(leftFiltered),
		RightTypes:         countTypes(rightFiltered),
	}
	if prefix < len(leftFiltered) {
		summary := leftFiltered[prefix].summary
		result.FirstLeft = &summary
	}
	if prefix < len(rightFiltered) {
		summary := rightFiltered[prefix].summary
		result.FirstRight = &summary
	}
	return result
}

func commonPrefix(left, right []normalizedEvent) int {
	limit := len(left)
	if len(right) < limit {
		limit = len(right)
	}
	for i := 0; i < limit; i++ {
		if left[i].key != right[i].key {
			return i
		}
	}
	return limit
}

func filterNamespace(events []normalizedEvent, namespace string) []normalizedEvent {
	filtered := make([]normalizedEvent, 0)
	for _, item := range events {
		if strings.HasPrefix(item.summary.Type, namespace) {
			filtered = append(filtered, item)
		}
	}
	return filtered
}

func countTypes(events []normalizedEvent) map[string]int {
	counts := make(map[string]int)
	for _, item := range events {
		counts[item.summary.Type]++
	}
	return counts
}

func outcomeFromEvents(session persistence.SessionRecord, finalRoot string, events []event.Event) OutcomeSide {
	result := OutcomeSide{Status: session.Status, FinalRootID: finalRoot}
	for i := len(events) - 1; i >= 0; i-- {
		if events[i].Type != "process.exited" {
			continue
		}
		var payload struct {
			ExitCode int  `json:"exit_code"`
			Success  bool `json:"success"`
		}
		if err := json.Unmarshal(events[i].Payload, &payload); err != nil {
			break
		}
		exitCode := payload.ExitCode
		success := payload.Success
		result.ExitCode = &exitCode
		result.Success = &success
		break
	}
	return result
}
