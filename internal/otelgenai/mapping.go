package otelgenai

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/rappidAI-Research/rappid-replay/internal/event"
)

var ErrNoGenAITelemetry = errors.New("no exportable GenAI telemetry")

type ExportOptions struct {
	ProviderName   string
	AgentName      string
	IncludeContent bool
}

type ImportOptions struct {
	Source         string
	IncludeContent bool
}

type codexSessionPayload struct {
	ThreadID        string `json:"thread_id"`
	Source          string `json:"source"`
	ModelProvider   string `json:"model_provider"`
	Model           string `json:"model"`
	ReasoningEffort string `json:"reasoning_effort"`
}

type messagePayload struct {
	Provider      string `json:"provider"`
	Role          string `json:"role"`
	Text          string `json:"text"`
	Phase         string `json:"phase"`
	Truncated     bool   `json:"truncated"`
	OriginalBytes int    `json:"original_bytes"`
}

type toolCallPayload struct {
	Provider      string `json:"provider"`
	Kind          string `json:"kind"`
	Name          string `json:"name"`
	Namespace     string `json:"namespace"`
	CallID        string `json:"call_id"`
	Arguments     string `json:"arguments"`
	Input         string `json:"input"`
	Truncated     bool   `json:"truncated"`
	OriginalBytes int    `json:"original_bytes"`
}

type toolResultPayload struct {
	Provider      string `json:"provider"`
	Kind          string `json:"kind"`
	CallID        string `json:"call_id"`
	Output        string `json:"output"`
	Truncated     bool   `json:"truncated"`
	OriginalBytes int    `json:"original_bytes"`
}

type usagePayload struct {
	Provider string           `json:"provider"`
	Tokens   map[string]int64 `json:"tokens"`
}

type toolPair struct {
	call       event.Event
	callValue  toolCallPayload
	result     *event.Event
	resultValue toolResultPayload
}

// ExportReplayEvents maps canonical Replay agent events to one OTLP/JSON trace.
// Replay does not invent model-call spans when the source evidence lacks model
// call boundaries. Instead it emits one agent invocation span, standardized
// execute_tool child spans where call/result boundaries exist, and Replay-
// namespaced span events for point observations such as assistant messages.
func ExportReplayEvents(input []event.Event, options ExportOptions) ([]byte, error) {
	events, err := canonicalEventOrder(input)
	if err != nil {
		return nil, err
	}
	if len(events) == 0 {
		return nil, ErrNoGenAITelemetry
	}

	var session codexSessionPayload
	var haveSession bool
	var messages []event.Event
	var latestUsage *event.Event
	calls := map[string]*toolPair{}
	var toolOrder []string
	adapterName := ""

	for _, item := range events {
		if !strings.HasPrefix(item.Type, "agent.") {
			continue
		}
		if adapterName == "" && strings.HasPrefix(item.Source, "adapter.") {
			adapterName = strings.TrimPrefix(item.Source, "adapter.")
		}
		switch item.Type {
		case "agent.codex.session":
			if err := json.Unmarshal(item.Payload, &session); err != nil {
				return nil, fmt.Errorf("decode agent.codex.session at seq %d: %w", item.Seq, err)
			}
			haveSession = true
		case "agent.message":
			messages = append(messages, item)
		case "agent.usage":
			copy := item
			latestUsage = &copy
		case "agent.tool_call":
			var payload toolCallPayload
			if err := json.Unmarshal(item.Payload, &payload); err != nil {
				return nil, fmt.Errorf("decode agent.tool_call at seq %d: %w", item.Seq, err)
			}
			key := toolKey(payload.CallID, item.Seq)
			if _, exists := calls[key]; exists {
				key = key + ":" + strconv.FormatUint(item.Seq, 10)
			}
			calls[key] = &toolPair{call: item, callValue: payload}
			toolOrder = append(toolOrder, key)
		case "agent.tool_result":
			var payload toolResultPayload
			if err := json.Unmarshal(item.Payload, &payload); err != nil {
				return nil, fmt.Errorf("decode agent.tool_result at seq %d: %w", item.Seq, err)
			}
			matched := false
			if payload.CallID != "" {
				for i := len(toolOrder) - 1; i >= 0; i-- {
					pair := calls[toolOrder[i]]
					if pair.callValue.CallID == payload.CallID && pair.result == nil {
						copy := item
						pair.result = &copy
						pair.resultValue = payload
						matched = true
						break
					}
				}
			}
			if !matched {
				key := "result:" + strconv.FormatUint(item.Seq, 10)
				copy := item
				calls[key] = &toolPair{call: item, callValue: toolCallPayload{Name: "unknown", CallID: payload.CallID}, result: &copy, resultValue: payload}
				toolOrder = append(toolOrder, key)
			}
		}
	}

	if !haveSession && len(messages) == 0 && len(toolOrder) == 0 && latestUsage == nil {
		return nil, ErrNoGenAITelemetry
	}
	provider := strings.TrimSpace(options.ProviderName)
	if provider == "" {
		provider = strings.TrimSpace(session.ModelProvider)
	}
	if provider == "" && adapterName == "codex" {
		provider = "openai"
	}
	if provider == "" {
		return nil, fmt.Errorf("OpenTelemetry GenAI export requires a provider name")
	}
	agentName := strings.TrimSpace(options.AgentName)
	if agentName == "" {
		agentName = adapterName
	}
	if agentName == "" {
		agentName = "rappid-replay-agent"
	}

	first, last := agentTimeBounds(events)
	traceID := deterministicID(16, "trace", events[0].SessionID)
	rootSpanID := deterministicID(8, "span", events[0].SessionID, "root")
	rootAttributes := []KeyValue{
		Attr("gen_ai.operation.name", StringValue("invoke_agent")),
		Attr("gen_ai.provider.name", StringValue(provider)),
		Attr("gen_ai.agent.name", StringValue(agentName)),
		Attr("rappid.replay.session.id", StringValue(events[0].SessionID)),
		Attr("rappid.replay.event.schema", StringValue(event.SchemaV1)),
		Attr("rappid.replay.otel.compatibility_snapshot", StringValue(CompatibilitySnapshot)),
	}
	if session.ThreadID != "" {
		rootAttributes = append(rootAttributes, Attr("gen_ai.conversation.id", StringValue(session.ThreadID)))
	}
	if session.Model != "" {
		rootAttributes = append(rootAttributes, Attr("gen_ai.request.model", StringValue(session.Model)))
	}
	if session.ReasoningEffort != "" {
		rootAttributes = append(rootAttributes, Attr("gen_ai.request.reasoning.level", StringValue(session.ReasoningEffort)))
	}
	if latestUsage != nil {
		var usage usagePayload
		if err := json.Unmarshal(latestUsage.Payload, &usage); err != nil {
			return nil, fmt.Errorf("decode agent.usage at seq %d: %w", latestUsage.Seq, err)
		}
		rootAttributes = append(rootAttributes, usageAttributes(usage.Tokens)...)
	}

	root := Span{
		TraceID:           traceID,
		SpanID:            rootSpanID,
		Name:              "invoke_agent " + agentName,
		Kind:              SpanKindInternal,
		StartTimeUnixNano: NanoTime(first.UnixNano()),
		EndTimeUnixNano:   NanoTime(last.UnixNano()),
		Attributes:        rootAttributes,
	}
	for _, item := range messages {
		var payload messagePayload
		if err := json.Unmarshal(item.Payload, &payload); err != nil {
			return nil, fmt.Errorf("decode agent.message at seq %d: %w", item.Seq, err)
		}
		attrs := []KeyValue{
			Attr("rappid.replay.seq", IntValue(int64(item.Seq))),
			Attr("rappid.replay.message.role", StringValue(payload.Role)),
		}
		if payload.Phase != "" {
			attrs = append(attrs, Attr("rappid.replay.message.phase", StringValue(payload.Phase)))
		}
		if payload.Truncated {
			attrs = append(attrs, Attr("rappid.replay.content.truncated", BoolValue(true)))
		}
		if payload.OriginalBytes > 0 {
			attrs = append(attrs, Attr("rappid.replay.content.original_bytes", IntValue(int64(payload.OriginalBytes))))
		}
		if options.IncludeContent && payload.Text != "" {
			attrs = append(attrs, Attr("rappid.replay.message.text", StringValue(payload.Text)))
		}
		root.Events = append(root.Events, SpanEvent{
			TimeUnixNano: NanoTime(item.WallTimeUTC.UnixNano()),
			Name:         "rappid.replay.agent.message",
			Attributes:   attrs,
		})
	}

	spans := []Span{root}
	for _, key := range toolOrder {
		pair := calls[key]
		name := strings.TrimSpace(pair.callValue.Name)
		if name == "" {
			name = "unknown"
		}
		end := pair.call.WallTimeUTC
		if pair.result != nil && pair.result.WallTimeUTC.After(end) {
			end = pair.result.WallTimeUTC
		}
		attrs := []KeyValue{
			Attr("gen_ai.operation.name", StringValue("execute_tool")),
			Attr("gen_ai.tool.name", StringValue(name)),
			Attr("gen_ai.agent.name", StringValue(agentName)),
			Attr("rappid.replay.seq", IntValue(int64(pair.call.Seq))),
		}
		if pair.callValue.CallID != "" {
			attrs = append(attrs, Attr("gen_ai.tool.call.id", StringValue(pair.callValue.CallID)))
		}
		if toolType := otelToolType(pair.callValue.Kind); toolType != "" {
			attrs = append(attrs, Attr("gen_ai.tool.type", StringValue(toolType)))
		}
		if options.IncludeContent {
			if content := firstNonEmpty(pair.callValue.Arguments, pair.callValue.Input); content != "" {
				attrs = append(attrs, Attr("gen_ai.tool.call.arguments", serializedAnyValue(content)))
			}
			if pair.result != nil && pair.resultValue.Output != "" {
				attrs = append(attrs, Attr("gen_ai.tool.call.result", serializedAnyValue(pair.resultValue.Output)))
			}
		}
		spans = append(spans, Span{
			TraceID:           traceID,
			SpanID:            deterministicID(8, "span", events[0].SessionID, "tool", key),
			ParentSpanID:      rootSpanID,
			Name:              "execute_tool " + name,
			Kind:              SpanKindInternal,
			StartTimeUnixNano: NanoTime(pair.call.WallTimeUTC.UnixNano()),
			EndTimeUnixNano:   NanoTime(end.UnixNano()),
			Attributes:        attrs,
		})
	}

	request := TraceExport{ResourceSpans: []ResourceSpans{{
		Resource: Resource{Attributes: []KeyValue{
			Attr("service.name", StringValue("rappidAI Replay")),
			Attr("rappid.replay.session.id", StringValue(events[0].SessionID)),
		}},
		ScopeSpans: []ScopeSpans{{
			Scope: InstrumentationScope{Name: "rappidAI/replay-otel-genai", Version: "1"},
			Spans: spans,
		}},
	}}}
	return MarshalTraceJSON(request)
}

func canonicalEventOrder(input []event.Event) ([]event.Event, error) {
	if len(input) == 0 {
		return nil, nil
	}
	out := append([]event.Event(nil), input...)
	sort.SliceStable(out, func(i, j int) bool { return out[i].Seq < out[j].Seq })
	sessionID := out[0].SessionID
	for i, item := range out {
		if item.Schema != event.SchemaV1 {
			return nil, fmt.Errorf("event at index %d has unsupported schema %q", i, item.Schema)
		}
		if item.SessionID != sessionID {
			return nil, fmt.Errorf("events span multiple Replay sessions")
		}
		if item.Seq == 0 || (i > 0 && item.Seq == out[i-1].Seq) {
			return nil, fmt.Errorf("events contain invalid or duplicate sequence %d", item.Seq)
		}
	}
	return out, nil
}

func agentTimeBounds(events []event.Event) (time.Time, time.Time) {
	var first, last time.Time
	for _, item := range events {
		if !strings.HasPrefix(item.Type, "agent.") {
			continue
		}
		if first.IsZero() || item.WallTimeUTC.Before(first) {
			first = item.WallTimeUTC
		}
		if last.IsZero() || item.WallTimeUTC.After(last) {
			last = item.WallTimeUTC
		}
	}
	if first.IsZero() {
		first = events[0].WallTimeUTC
	}
	if last.IsZero() {
		last = first
	}
	return first.UTC(), last.UTC()
}

func deterministicID(size int, parts ...string) string {
	hash := sha256.New()
	for _, part := range parts {
		hash.Write([]byte(part))
		hash.Write([]byte{0})
	}
	sum := hash.Sum(nil)
	return hex.EncodeToString(sum[:size])
}

func toolKey(callID string, seq uint64) string {
	if strings.TrimSpace(callID) != "" {
		return "call:" + callID
	}
	return "seq:" + strconv.FormatUint(seq, 10)
}

func otelToolType(kind string) string {
	switch kind {
	case "function_call":
		return "function"
	case "custom_tool_call":
		return "extension"
	default:
		return ""
	}
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

func usageAttributes(tokens map[string]int64) []KeyValue {
	mapping := map[string]string{
		"input_tokens":             "gen_ai.usage.input_tokens",
		"output_tokens":            "gen_ai.usage.output_tokens",
		"cached_input_tokens":      "gen_ai.usage.cache_read.input_tokens",
		"cache_write_input_tokens": "gen_ai.usage.cache_write.input_tokens",
		"reasoning_output_tokens":  "gen_ai.usage.reasoning.output_tokens",
	}
	keys := make([]string, 0, len(tokens))
	for key := range tokens {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	out := make([]KeyValue, 0, len(keys))
	for _, key := range keys {
		value := tokens[key]
		if value < 0 {
			continue
		}
		if target, ok := mapping[key]; ok {
			out = append(out, Attr(target, IntValue(value)))
		} else if key == "model_context_window" {
			out = append(out, Attr("rappid.replay.codex.model_context_window", IntValue(value)))
		}
	}
	return out
}

func serializedAnyValue(value string) AnyValue {
	decoder := json.NewDecoder(strings.NewReader(value))
	decoder.UseNumber()
	var decoded any
	if err := decoder.Decode(&decoded); err == nil {
		if converted, err := anyFromJSON(decoded, 0); err == nil {
			return converted
		}
	}
	return StringValue(value)
}

func anyFromJSON(value any, depth int) (AnyValue, error) {
	if depth > MaxAnyValueDepth {
		return AnyValue{}, fmt.Errorf("JSON value nesting exceeds %d", MaxAnyValueDepth)
	}
	switch typed := value.(type) {
	case string:
		return StringValue(typed), nil
	case bool:
		return BoolValue(typed), nil
	case json.Number:
		if integer, err := typed.Int64(); err == nil {
			return IntValue(integer), nil
		}
		floating, err := typed.Float64()
		if err != nil {
			return AnyValue{}, err
		}
		return DoubleValueOf(floating), nil
	case float64:
		return DoubleValueOf(typed), nil
	case []any:
		values := make([]AnyValue, 0, len(typed))
		for _, child := range typed {
			converted, err := anyFromJSON(child, depth+1)
			if err != nil {
				return AnyValue{}, err
			}
			values = append(values, converted)
		}
		return ArrayOf(values...), nil
	case map[string]any:
		keys := make([]string, 0, len(typed))
		for key := range typed {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		values := make([]KeyValue, 0, len(keys))
		for _, key := range keys {
			converted, err := anyFromJSON(typed[key], depth+1)
			if err != nil {
				return AnyValue{}, err
			}
			values = append(values, Attr(key, converted))
		}
		return KVList(values...), nil
	case nil:
		return StringValue("null"), nil
	default:
		return AnyValue{}, fmt.Errorf("unsupported JSON value %T", value)
	}
}

// ImportTraceJSON converts OpenTelemetry GenAI spans into additive Replay event
// drafts. The caller still owns canonical event sequence and monotonic timestamps.
// Imported attributes are conservatively classified as content because arbitrary
// instrumentation attributes can contain user or customer data.
func ImportTraceJSON(data []byte, sessionID string, options ImportOptions) ([]event.Draft, error) {
	request, err := ParseTraceJSON(data)
	if err != nil {
		return nil, err
	}
	source := strings.TrimSpace(options.Source)
	if source == "" {
		source = "otel"
	}
	type orderedDraft struct {
		draft event.Draft
		order int
	}
	var ordered []orderedDraft
	order := 0
	appendDraft := func(draft event.Draft) {
		ordered = append(ordered, orderedDraft{draft: draft, order: order})
		order++
	}

	for _, resource := range request.ResourceSpans {
		for _, scope := range resource.ScopeSpans {
			for _, span := range scope.Spans {
				attrs := attributesMap(span.Attributes)
				operation, _ := anyString(attrs["gen_ai.operation.name"])
				provider, _ := anyString(attrs["gen_ai.provider.name"])
				if operation == "" && provider == "" {
					continue
				}

				spanPayload := map[string]any{
					"trace_id": span.TraceID,
					"span_id": span.SpanID,
					"parent_span_id": span.ParentSpanID,
					"name": span.Name,
					"kind": span.Kind,
					"start_time_unix_nano": uint64(span.StartTimeUnixNano),
					"end_time_unix_nano": uint64(span.EndTimeUnixNano),
					"operation": operation,
					"provider": provider,
					"attributes": portableAttributes(span.Attributes, options.IncludeContent),
				}
				payload, err := json.Marshal(spanPayload)
				if err != nil {
					return nil, err
				}
				draft := event.NewDraft(sessionID, "agent.otel.span", source, nanoTime(span.StartTimeUnixNano), event.Privacy{Classification: "content"}, payload)
				draft.SpanID = span.SpanID
				appendDraft(draft)

				if operation == "execute_tool" {
					appendImportedToolEvents(&ordered, &order, sessionID, source, span, attrs, options.IncludeContent)
				}
				appendImportedUsage(&ordered, &order, sessionID, source, span, attrs)
				appendImportedOutputMessages(&ordered, &order, sessionID, source, span, attrs, options.IncludeContent)
				for _, spanEvent := range span.Events {
					appendImportedSpanEvent(&ordered, &order, sessionID, source, span, spanEvent, options.IncludeContent)
				}
			}
		}
	}

	sort.SliceStable(ordered, func(i, j int) bool {
		left := ordered[i].draft.WallTimeUTC
		right := ordered[j].draft.WallTimeUTC
		if left.Equal(right) {
			return ordered[i].order < ordered[j].order
		}
		return left.Before(right)
	})
	out := make([]event.Draft, 0, len(ordered))
	for _, item := range ordered {
		out = append(out, item.draft)
	}
	return out, nil
}

func appendImportedToolEvents(ordered *[]struct{ draft event.Draft; order int }, order *int, sessionID, source string, span Span, attrs map[string]AnyValue, includeContent bool) {
	name, _ := anyString(attrs["gen_ai.tool.name"])
	callID, _ := anyString(attrs["gen_ai.tool.call.id"])
	kind, _ := anyString(attrs["gen_ai.tool.type"])
	call := map[string]any{"provider": "otel", "kind": kind, "name": name, "call_id": callID}
	if includeContent {
		if value, ok := attrs["gen_ai.tool.call.arguments"]; ok {
			call["arguments"] = anyValuePortable(value)
		}
	}
	callJSON, _ := json.Marshal(call)
	draft := event.NewDraft(sessionID, "agent.tool_call", source, nanoTime(span.StartTimeUnixNano), event.Privacy{Classification: "content"}, callJSON)
	draft.SpanID = span.SpanID
	*ordered = append(*ordered, struct{ draft event.Draft; order int }{draft: draft, order: *order})
	*order++
	if result, ok := attrs["gen_ai.tool.call.result"]; ok {
		payload := map[string]any{"provider": "otel", "kind": kind, "call_id": callID}
		if includeContent {
			payload["output"] = anyValuePortable(result)
		}
		encoded, _ := json.Marshal(payload)
		resultDraft := event.NewDraft(sessionID, "agent.tool_result", source, nanoTime(span.EndTimeUnixNano), event.Privacy{Classification: "content"}, encoded)
		resultDraft.SpanID = span.SpanID
		*ordered = append(*ordered, struct{ draft event.Draft; order int }{draft: resultDraft, order: *order})
		*order++
	}
}

func appendImportedUsage(ordered *[]struct{ draft event.Draft; order int }, order *int, sessionID, source string, span Span, attrs map[string]AnyValue) {
	reverse := map[string]string{
		"gen_ai.usage.input_tokens":             "input_tokens",
		"gen_ai.usage.output_tokens":            "output_tokens",
		"gen_ai.usage.cache_read.input_tokens":  "cached_input_tokens",
		"gen_ai.usage.cache_write.input_tokens": "cache_write_input_tokens",
		"gen_ai.usage.reasoning.output_tokens":  "reasoning_output_tokens",
	}
	tokens := map[string]int64{}
	for sourceKey, target := range reverse {
		if value, ok := anyInt64(attrs[sourceKey]); ok && value >= 0 {
			tokens[target] = value
		}
	}
	if len(tokens) == 0 {
		return
	}
	encoded, _ := json.Marshal(usagePayload{Provider: "otel", Tokens: tokens})
	draft := event.NewDraft(sessionID, "agent.usage", source, nanoTime(span.EndTimeUnixNano), event.Privacy{Classification: "technical"}, encoded)
	draft.SpanID = span.SpanID
	*ordered = append(*ordered, struct{ draft event.Draft; order int }{draft: draft, order: *order})
	*order++
}

func appendImportedOutputMessages(ordered *[]struct{ draft event.Draft; order int }, order *int, sessionID, source string, span Span, attrs map[string]AnyValue, includeContent bool) {
	value, ok := attrs["gen_ai.output.messages"]
	if !ok || !includeContent {
		return
	}
	for _, message := range extractMessages(value) {
		encoded, _ := json.Marshal(message)
		draft := event.NewDraft(sessionID, "agent.message", source, nanoTime(span.EndTimeUnixNano), event.Privacy{Classification: "content"}, encoded)
		draft.SpanID = span.SpanID
		*ordered = append(*ordered, struct{ draft event.Draft; order int }{draft: draft, order: *order})
		*order++
	}
}

func appendImportedSpanEvent(ordered *[]struct{ draft event.Draft; order int }, order *int, sessionID, source string, span Span, spanEvent SpanEvent, includeContent bool) {
	attrs := attributesMap(spanEvent.Attributes)
	if spanEvent.Name == "rappid.replay.agent.message" {
		role, _ := anyString(attrs["rappid.replay.message.role"])
		phase, _ := anyString(attrs["rappid.replay.message.phase"])
		text := ""
		if includeContent {
			text, _ = anyString(attrs["rappid.replay.message.text"])
		}
		payload, _ := json.Marshal(messagePayload{Provider: "otel", Role: role, Phase: phase, Text: text})
		draft := event.NewDraft(sessionID, "agent.message", source, nanoTime(spanEvent.TimeUnixNano), event.Privacy{Classification: "content"}, payload)
		draft.SpanID = span.SpanID
		*ordered = append(*ordered, struct{ draft event.Draft; order int }{draft: draft, order: *order})
		*order++
		return
	}
	if spanEvent.Name == "gen_ai.client.inference.operation.details" && includeContent {
		if output, ok := attrs["gen_ai.output.messages"]; ok {
			for _, message := range extractMessages(output) {
				encoded, _ := json.Marshal(message)
				draft := event.NewDraft(sessionID, "agent.message", source, nanoTime(spanEvent.TimeUnixNano), event.Privacy{Classification: "content"}, encoded)
				draft.SpanID = span.SpanID
				*ordered = append(*ordered, struct{ draft event.Draft; order int }{draft: draft, order: *order})
				*order++
			}
		}
	}
}

func nanoTime(value NanoTime) time.Time {
	if value == 0 {
		return time.Unix(0, 0).UTC()
	}
	return time.Unix(0, int64(value)).UTC()
}

var sensitiveGenAIAttributes = map[string]struct{}{
	"gen_ai.input.messages": {},
	"gen_ai.output.messages": {},
	"gen_ai.system_instructions": {},
	"gen_ai.tool.definitions": {},
	"gen_ai.tool.call.arguments": {},
	"gen_ai.tool.call.result": {},
}

func portableAttributes(attributes []KeyValue, includeContent bool) map[string]any {
	out := make(map[string]any, len(attributes))
	for _, attribute := range attributes {
		if !includeContent {
			if _, sensitive := sensitiveGenAIAttributes[attribute.Key]; sensitive {
				continue
			}
		}
		out[attribute.Key] = anyValuePortable(attribute.Value)
	}
	return out
}

func anyValuePortable(value AnyValue) any {
	switch {
	case value.StringValue != nil:
		return *value.StringValue
	case value.BoolValue != nil:
		return *value.BoolValue
	case value.IntValue != nil:
		return int64(*value.IntValue)
	case value.DoubleValue != nil:
		return *value.DoubleValue
	case value.ArrayValue != nil:
		out := make([]any, 0, len(value.ArrayValue.Values))
		for _, child := range value.ArrayValue.Values {
			out = append(out, anyValuePortable(child))
		}
		return out
	case value.KVListValue != nil:
		out := make(map[string]any, len(value.KVListValue.Values))
		for _, child := range value.KVListValue.Values {
			out[child.Key] = anyValuePortable(child.Value)
		}
		return out
	case value.BytesValue != nil:
		return *value.BytesValue
	default:
		return nil
	}
}

func extractMessages(value AnyValue) []messagePayload {
	if value.ArrayValue == nil {
		return nil
	}
	var out []messagePayload
	for _, messageValue := range value.ArrayValue.Values {
		if messageValue.KVListValue == nil {
			continue
		}
		fields := attributesMap(messageValue.KVListValue.Values)
		role, _ := anyString(fields["role"])
		parts := fields["parts"]
		if parts.ArrayValue == nil {
			continue
		}
		var textParts []string
		for _, part := range parts.ArrayValue.Values {
			if part.KVListValue == nil {
				continue
			}
			partFields := attributesMap(part.KVListValue.Values)
			typeName, _ := anyString(partFields["type"])
			if typeName != "text" && typeName != "output_text" {
				continue
			}
			content, _ := anyString(partFields["content"])
			if content == "" {
				content, _ = anyString(partFields["text"])
			}
			if content != "" {
				textParts = append(textParts, content)
			}
		}
		if len(textParts) == 0 {
			continue
		}
		out = append(out, messagePayload{Provider: "otel", Role: role, Text: strings.Join(textParts, "\n")})
	}
	return out
}
