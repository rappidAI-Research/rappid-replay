package otelgenai

import (
	"bytes"
	"strings"
	"testing"
)

func TestMarshalAndParseTraceJSONUsesOTLPJSONEncoding(t *testing.T) {
	request := TraceExport{ResourceSpans: []ResourceSpans{{
		Resource: Resource{Attributes: []KeyValue{Attr("service.name", StringValue("replay-test"))}},
		ScopeSpans: []ScopeSpans{{
			Scope: InstrumentationScope{Name: "test-scope", Version: "1"},
			Spans: []Span{{
				TraceID:           "0123456789abcdef0123456789abcdef",
				SpanID:            "0123456789abcdef",
				Name:              "invoke_agent codex",
				Kind:              SpanKindInternal,
				StartTimeUnixNano: NanoTime(1_700_000_000_000_000_000),
				EndTimeUnixNano:   NanoTime(1_700_000_000_000_000_100),
				Attributes: []KeyValue{
					Attr("gen_ai.operation.name", StringValue("invoke_agent")),
					Attr("gen_ai.usage.input_tokens", IntValue(42)),
				},
				Events: []SpanEvent{{
					TimeUnixNano: NanoTime(1_700_000_000_000_000_050),
					Name:         "rappid.replay.agent.message",
					Attributes:   []KeyValue{Attr("rappid.replay.message.role", StringValue("assistant"))},
				}},
			}},
		}},
	}}}

	encoded, err := MarshalTraceJSON(request)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(encoded, []byte(`"startTimeUnixNano":"1700000000000000000"`)) {
		t.Fatalf("uint64 timestamp was not encoded as an OTLP decimal string: %s", encoded)
	}
	if !bytes.Contains(encoded, []byte(`"intValue":"42"`)) {
		t.Fatalf("int64 AnyValue was not encoded as an OTLP decimal string: %s", encoded)
	}

	decoded, err := ParseTraceJSON(encoded)
	if err != nil {
		t.Fatal(err)
	}
	span := decoded.ResourceSpans[0].ScopeSpans[0].Spans[0]
	if span.TraceID != request.ResourceSpans[0].ScopeSpans[0].Spans[0].TraceID || span.StartTimeUnixNano != NanoTime(1_700_000_000_000_000_000) {
		t.Fatalf("round trip changed span identity or time: %+v", span)
	}
}

func TestParseTraceJSONIgnoresUnknownFields(t *testing.T) {
	raw := []byte(`{
		"resourceSpans":[{"scopeSpans":[{"spans":[{
			"traceId":"0123456789abcdef0123456789abcdef",
			"spanId":"0123456789abcdef",
			"name":"chat gpt-test",
			"kind":3,
			"startTimeUnixNano":"1700000000000000000",
			"endTimeUnixNano":"1700000000000000010",
			"futureField":{"ignored":true}
		}]}]}],
		"futureTopLevel":"ignored"
	}`)
	if _, err := ParseTraceJSON(raw); err != nil {
		t.Fatalf("unknown OTLP fields should be ignored: %v", err)
	}
}

func TestTraceValidationRejectsMalformedIdentityAndEnums(t *testing.T) {
	base := Span{
		TraceID:           "0123456789abcdef0123456789abcdef",
		SpanID:            "0123456789abcdef",
		Name:              "invoke_agent codex",
		Kind:              SpanKindInternal,
		StartTimeUnixNano: 10,
		EndTimeUnixNano:   20,
	}
	request := func(span Span) TraceExport {
		return TraceExport{ResourceSpans: []ResourceSpans{{ScopeSpans: []ScopeSpans{{Spans: []Span{span}}}}}}
	}

	zeroTrace := base
	zeroTrace.TraceID = strings.Repeat("0", 32)
	if err := ValidateTraceExport(request(zeroTrace)); err == nil {
		t.Fatal("all-zero trace id was accepted")
	}

	badKind := base
	badKind.Kind = 99
	if err := ValidateTraceExport(request(badKind)); err == nil {
		t.Fatal("invalid span kind was accepted")
	}

	badStatus := base
	badStatus.Status.Code = 99
	if err := ValidateTraceExport(request(badStatus)); err == nil {
		t.Fatal("invalid status code was accepted")
	}

	backwards := base
	backwards.EndTimeUnixNano = 9
	if err := ValidateTraceExport(request(backwards)); err == nil {
		t.Fatal("backwards span time was accepted")
	}
}

func TestTraceValidationRejectsAmbiguousAndDeepAnyValues(t *testing.T) {
	value := StringValue("one")
	truth := true
	value.BoolValue = &truth
	if err := validateAnyValue(value, 0); err == nil {
		t.Fatal("AnyValue with multiple variants was accepted")
	}

	deep := StringValue("leaf")
	for range MaxAnyValueDepth + 1 {
		deep = ArrayOf(deep)
	}
	if err := validateAnyValue(deep, 0); err == nil {
		t.Fatal("overly deep AnyValue was accepted")
	}
}

func TestParseTraceJSONRejectsTrailingValues(t *testing.T) {
	raw := []byte(`{"resourceSpans":[]} {"resourceSpans":[]}`)
	if _, err := ParseTraceJSON(raw); err == nil {
		t.Fatal("multiple top-level JSON values were accepted")
	}
}
