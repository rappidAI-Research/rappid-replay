package otelgenai

import (
	"bytes"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/rappidAI-Research/rappid-replay/internal/event"
)

const testSessionID = "0199419a-6c00-7000-8000-000000000001"

func TestExportReplayEventsMapsSemanticsAndDefaultsToMetadataOnly(t *testing.T) {
	events := sampleReplayEvents(t)
	first, err := ExportReplayEvents(events, ExportOptions{})
	if err != nil {
		t.Fatal(err)
	}
	second, err := ExportReplayEvents(events, ExportOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(first, second) {
		t.Fatal("same Replay events produced different OTLP JSON")
	}
	if bytes.Contains(first, []byte("assistant text")) || bytes.Contains(first, []byte("Paris")) || bytes.Contains(first, []byte("sunny")) {
		t.Fatalf("content appeared in metadata-only export: %s", first)
	}

	request, err := ParseTraceJSON(first)
	if err != nil {
		t.Fatal(err)
	}
	spans := request.ResourceSpans[0].ScopeSpans[0].Spans
	if len(spans) != 2 {
		t.Fatalf("exported %d spans, want 2", len(spans))
	}
	rootAttrs := attributesMap(spans[0].Attributes)
	if got, _ := anyString(rootAttrs["gen_ai.operation.name"]); got != "invoke_agent" {
		t.Fatalf("root operation = %q", got)
	}
	if got, _ := anyString(rootAttrs["gen_ai.provider.name"]); got != "openai" {
		t.Fatalf("provider = %q", got)
	}
	if got, ok := anyInt64(rootAttrs["gen_ai.usage.input_tokens"]); !ok || got != 12 {
		t.Fatalf("input token usage = %d, %v", got, ok)
	}
	if len(spans[0].Events) != 1 || spans[0].Events[0].Name != "rappid.replay.agent.message" {
		t.Fatalf("root events = %+v", spans[0].Events)
	}

	toolAttrs := attributesMap(spans[1].Attributes)
	if spans[1].Name != "execute_tool weather" || spans[1].ParentSpanID != spans[0].SpanID {
		t.Fatalf("tool span = %+v", spans[1])
	}
	if got, _ := anyString(toolAttrs["gen_ai.tool.call.id"]); got != "call-1" {
		t.Fatalf("tool call id = %q", got)
	}
	if _, exists := toolAttrs["gen_ai.tool.call.arguments"]; exists {
		t.Fatal("tool arguments exported without IncludeContent")
	}
}

func TestExportReplayEventsContentRequiresOptIn(t *testing.T) {
	encoded, err := ExportReplayEvents(sampleReplayEvents(t), ExportOptions{IncludeContent: true})
	if err != nil {
		t.Fatal(err)
	}
	request, err := ParseTraceJSON(encoded)
	if err != nil {
		t.Fatal(err)
	}
	spans := request.ResourceSpans[0].ScopeSpans[0].Spans
	messageAttrs := attributesMap(spans[0].Events[0].Attributes)
	if got, _ := anyString(messageAttrs["rappid.replay.message.text"]); got != "assistant text" {
		t.Fatalf("message text = %q", got)
	}
	toolAttrs := attributesMap(spans[1].Attributes)
	arguments, ok := toolAttrs["gen_ai.tool.call.arguments"]
	if !ok || arguments.KVListValue == nil || portableString(arguments) != `{"city":"Paris"}` {
		t.Fatalf("tool arguments = %+v", arguments)
	}
	if result, ok := toolAttrs["gen_ai.tool.call.result"]; !ok || result.KVListValue == nil {
		t.Fatalf("tool result = %+v", result)
	}
}

func TestExportReplayEventsRequiresKnownProvider(t *testing.T) {
	item := replayEvent(t, 1, "agent.message", "adapter.unknown", time.Unix(1_700_000_000, 0), messagePayload{Role: "assistant", Text: "hello"})
	if _, err := ExportReplayEvents([]event.Event{item}, ExportOptions{}); err == nil {
		t.Fatal("export without provider was accepted")
	}
	if _, err := ExportReplayEvents([]event.Event{item}, ExportOptions{ProviderName: "custom.provider"}); err != nil {
		t.Fatalf("explicit provider rejected: %v", err)
	}
}

func TestImportTraceJSONNormalizesGenAISemantics(t *testing.T) {
	encoded, err := MarshalTraceJSON(sampleExternalTrace())
	if err != nil {
		t.Fatal(err)
	}
	withContent, err := ImportTraceJSON(encoded, testSessionID, ImportOptions{IncludeContent: true})
	if err != nil {
		t.Fatal(err)
	}
	counts := draftTypeCounts(withContent)
	want := map[string]int{"agent.otel.span": 2, "agent.message": 1, "agent.usage": 1, "agent.tool_call": 1, "agent.tool_result": 1}
	for kind, count := range want {
		if counts[kind] != count {
			t.Fatalf("%s count = %d, want %d; all=%v", kind, counts[kind], count, counts)
		}
	}
	var call toolCallPayload
	for _, draft := range withContent {
		if draft.Type == "agent.tool_call" {
			if err := json.Unmarshal(draft.Payload, &call); err != nil {
				t.Fatal(err)
			}
		}
	}
	if call.Provider != "openai" || call.Name != "weather" || call.Arguments != `{"city":"Paris"}` {
		t.Fatalf("imported tool call = %+v", call)
	}

	withoutContent, err := ImportTraceJSON(encoded, testSessionID, ImportOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if draftTypeCounts(withoutContent)["agent.message"] != 0 {
		t.Fatal("output message imported without IncludeContent")
	}
	for _, draft := range withoutContent {
		if bytes.Contains(draft.Payload, []byte("external answer")) || bytes.Contains(draft.Payload, []byte("Paris")) || bytes.Contains(draft.Payload, []byte("sunny")) {
			t.Fatalf("content appeared in metadata-only import: %s", draft.Payload)
		}
	}
}

func TestUsageAttributesAreDeterministic(t *testing.T) {
	left := usageAttributes(map[string]int64{"output_tokens": 5, "input_tokens": 12, "cached_input_tokens": 3})
	right := usageAttributes(map[string]int64{"cached_input_tokens": 3, "input_tokens": 12, "output_tokens": 5})
	if !reflect.DeepEqual(left, right) {
		t.Fatalf("usage mapping depends on map iteration: %#v != %#v", left, right)
	}
}

func sampleReplayEvents(t *testing.T) []event.Event {
	t.Helper()
	base := time.Unix(1_700_000_000, 0).UTC()
	return []event.Event{
		replayEvent(t, 1, "agent.codex.session", "adapter.codex", base, codexSessionPayload{ThreadID: "thread-123", ModelProvider: "openai", Model: "gpt-test", ReasoningEffort: "medium"}),
		replayEvent(t, 2, "agent.message", "adapter.codex", base.Add(time.Second), messagePayload{Provider: "codex", Role: "assistant", Text: "assistant text", Phase: "final_answer"}),
		replayEvent(t, 3, "agent.tool_call", "adapter.codex", base.Add(2*time.Second), toolCallPayload{Provider: "codex", Kind: "function_call", Name: "weather", CallID: "call-1", Arguments: `{"city":"Paris"}`}),
		replayEvent(t, 4, "agent.tool_result", "adapter.codex", base.Add(3*time.Second), toolResultPayload{Provider: "codex", Kind: "function_call_output", CallID: "call-1", Output: `{"conditions":"sunny"}`}),
		replayEvent(t, 5, "agent.usage", "adapter.codex", base.Add(4*time.Second), usagePayload{Provider: "codex", Tokens: map[string]int64{"input_tokens": 12, "cached_input_tokens": 3, "output_tokens": 5, "reasoning_output_tokens": 2}}),
	}
}

func replayEvent(t *testing.T, seq uint64, kind, source string, wallTime time.Time, payload any) event.Event {
	t.Helper()
	encoded, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	draft := event.NewDraft(testSessionID, kind, source, wallTime, event.Privacy{Classification: "content"}, encoded)
	return draft.Stamp(seq, seq*100)
}

func sampleExternalTrace() TraceExport {
	message := ArrayOf(KVList(Attr("role", StringValue("assistant")), Attr("parts", ArrayOf(KVList(Attr("type", StringValue("text")), Attr("content", StringValue("external answer")))))))
	return TraceExport{ResourceSpans: []ResourceSpans{{ScopeSpans: []ScopeSpans{{Spans: []Span{
		{TraceID: "11111111111111111111111111111111", SpanID: "1111111111111111", Name: "chat gpt-test", Kind: SpanKindClient, StartTimeUnixNano: NanoTime(1_700_000_000_000_000_000), EndTimeUnixNano: NanoTime(1_700_000_001_000_000_000), Attributes: []KeyValue{Attr("gen_ai.operation.name", StringValue("chat")), Attr("gen_ai.provider.name", StringValue("openai")), Attr("gen_ai.request.model", StringValue("gpt-test")), Attr("gen_ai.usage.input_tokens", IntValue(10)), Attr("gen_ai.usage.output_tokens", IntValue(4)), Attr("gen_ai.output.messages", message)}},
		{TraceID: "11111111111111111111111111111111", SpanID: "2222222222222222", ParentSpanID: "1111111111111111", Name: "execute_tool weather", Kind: SpanKindInternal, StartTimeUnixNano: NanoTime(1_700_000_000_200_000_000), EndTimeUnixNano: NanoTime(1_700_000_000_300_000_000), Attributes: []KeyValue{Attr("gen_ai.operation.name", StringValue("execute_tool")), Attr("gen_ai.provider.name", StringValue("openai")), Attr("gen_ai.tool.name", StringValue("weather")), Attr("gen_ai.tool.type", StringValue("function")), Attr("gen_ai.tool.call.id", StringValue("call-1")), Attr("gen_ai.tool.call.arguments", KVList(Attr("city", StringValue("Paris")))), Attr("gen_ai.tool.call.result", KVList(Attr("conditions", StringValue("sunny"))))}},
	}}}}}}
}

func draftTypeCounts(drafts []event.Draft) map[string]int {
	counts := map[string]int{}
	for _, draft := range drafts {
		counts[draft.Type]++
	}
	return counts
}
