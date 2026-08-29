// Package otelgenai implements offline OpenTelemetry GenAI interoperability.
// It deliberately uses OTLP/JSON file payloads rather than a network exporter so
// deterministic Replay operation never gains an implicit telemetry egress path.
package otelgenai

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

const (
	// CompatibilitySnapshot documents the OpenTelemetry GenAI semantic-convention
	// snapshot used when this mapping was implemented. The upstream GenAI
	// conventions are still Development and currently have no stable release.
	CompatibilitySnapshot = "open-telemetry/semantic-conventions-genai@67dff024110be5bd9f318006e733f4078e0f4c97"
	CoreSemconvVersion     = "1.44.0"

	MaxDocumentBytes = 64 << 20
	MaxSpans         = 100_000
	MaxAttributes    = 512
	MaxEventsPerSpan = 4_096
	MaxAnyValueDepth = 16
)

const (
	SpanKindUnspecified = 0
	SpanKindInternal    = 1
	SpanKindServer      = 2
	SpanKindClient      = 3
	SpanKindProducer    = 4
	SpanKindConsumer    = 5
)

// TraceExport is the OTLP ExportTraceServiceRequest JSON shape.
type TraceExport struct {
	ResourceSpans []ResourceSpans `json:"resourceSpans,omitempty"`
}

type ResourceSpans struct {
	Resource   Resource     `json:"resource,omitempty"`
	ScopeSpans []ScopeSpans `json:"scopeSpans,omitempty"`
	SchemaURL  string       `json:"schemaUrl,omitempty"`
}

type Resource struct {
	Attributes             []KeyValue `json:"attributes,omitempty"`
	DroppedAttributesCount uint32     `json:"droppedAttributesCount,omitempty"`
}

type ScopeSpans struct {
	Scope     InstrumentationScope `json:"scope,omitempty"`
	Spans     []Span               `json:"spans,omitempty"`
	SchemaURL string               `json:"schemaUrl,omitempty"`
}

type InstrumentationScope struct {
	Name                   string     `json:"name,omitempty"`
	Version                string     `json:"version,omitempty"`
	Attributes             []KeyValue `json:"attributes,omitempty"`
	DroppedAttributesCount uint32     `json:"droppedAttributesCount,omitempty"`
}

type Span struct {
	TraceID                string      `json:"traceId,omitempty"`
	SpanID                 string      `json:"spanId,omitempty"`
	TraceState             string      `json:"traceState,omitempty"`
	ParentSpanID           string      `json:"parentSpanId,omitempty"`
	Name                   string      `json:"name,omitempty"`
	Kind                   int32       `json:"kind,omitempty"`
	StartTimeUnixNano      NanoTime    `json:"startTimeUnixNano,omitempty"`
	EndTimeUnixNano        NanoTime    `json:"endTimeUnixNano,omitempty"`
	Attributes             []KeyValue  `json:"attributes,omitempty"`
	DroppedAttributesCount uint32      `json:"droppedAttributesCount,omitempty"`
	Events                 []SpanEvent `json:"events,omitempty"`
	DroppedEventsCount     uint32      `json:"droppedEventsCount,omitempty"`
	Status                 Status      `json:"status,omitempty"`
}

type SpanEvent struct {
	TimeUnixNano           NanoTime   `json:"timeUnixNano,omitempty"`
	Name                   string     `json:"name,omitempty"`
	Attributes             []KeyValue `json:"attributes,omitempty"`
	DroppedAttributesCount uint32     `json:"droppedAttributesCount,omitempty"`
}

type Status struct {
	Message string `json:"message,omitempty"`
	Code    int32  `json:"code,omitempty"`
}

type KeyValue struct {
	Key   string   `json:"key"`
	Value AnyValue `json:"value"`
}

// AnyValue is the OTLP protobuf AnyValue JSON representation. 64-bit integers
// are represented as decimal strings per OTLP's protobuf JSON rules.
type AnyValue struct {
	StringValue *string       `json:"stringValue,omitempty"`
	BoolValue   *bool         `json:"boolValue,omitempty"`
	IntValue    *ProtoInt64   `json:"intValue,omitempty"`
	DoubleValue *float64      `json:"doubleValue,omitempty"`
	ArrayValue  *ArrayValue   `json:"arrayValue,omitempty"`
	KVListValue *KeyValueList `json:"kvlistValue,omitempty"`
	BytesValue  *string       `json:"bytesValue,omitempty"`
}

type ArrayValue struct {
	Values []AnyValue `json:"values,omitempty"`
}

type KeyValueList struct {
	Values []KeyValue `json:"values,omitempty"`
}

type ProtoInt64 int64

func (v ProtoInt64) MarshalJSON() ([]byte, error) {
	return json.Marshal(strconv.FormatInt(int64(v), 10))
}

func (v *ProtoInt64) UnmarshalJSON(data []byte) error {
	if v == nil {
		return fmt.Errorf("nil ProtoInt64 receiver")
	}
	text := strings.TrimSpace(string(data))
	if len(text) >= 2 && text[0] == '"' && text[len(text)-1] == '"' {
		var decoded string
		if err := json.Unmarshal(data, &decoded); err != nil {
			return err
		}
		text = decoded
	}
	parsed, err := strconv.ParseInt(text, 10, 64)
	if err != nil {
		return fmt.Errorf("invalid OTLP int64 %q: %w", text, err)
	}
	*v = ProtoInt64(parsed)
	return nil
}

// NanoTime follows the OTLP JSON requirement that uint64 protobuf fields are
// emitted as decimal strings while accepting either strings or JSON numbers.
type NanoTime uint64

func (v NanoTime) MarshalJSON() ([]byte, error) {
	return json.Marshal(strconv.FormatUint(uint64(v), 10))
}

func (v *NanoTime) UnmarshalJSON(data []byte) error {
	if v == nil {
		return fmt.Errorf("nil NanoTime receiver")
	}
	text := strings.TrimSpace(string(data))
	if len(text) >= 2 && text[0] == '"' && text[len(text)-1] == '"' {
		var decoded string
		if err := json.Unmarshal(data, &decoded); err != nil {
			return err
		}
		text = decoded
	}
	parsed, err := strconv.ParseUint(text, 10, 64)
	if err != nil {
		return fmt.Errorf("invalid OTLP uint64 %q: %w", text, err)
	}
	*v = NanoTime(parsed)
	return nil
}

func StringValue(value string) AnyValue { return AnyValue{StringValue: &value} }
func BoolValue(value bool) AnyValue      { return AnyValue{BoolValue: &value} }
func IntValue(value int64) AnyValue {
	converted := ProtoInt64(value)
	return AnyValue{IntValue: &converted}
}
func DoubleValueOf(value float64) AnyValue { return AnyValue{DoubleValue: &value} }

func ArrayOf(values ...AnyValue) AnyValue {
	return AnyValue{ArrayValue: &ArrayValue{Values: append([]AnyValue(nil), values...)}}
}

func KVList(values ...KeyValue) AnyValue {
	return AnyValue{KVListValue: &KeyValueList{Values: append([]KeyValue(nil), values...)}}
}

func Attr(key string, value AnyValue) KeyValue { return KeyValue{Key: key, Value: value} }

// ParseTraceJSON parses a bounded OTLP/JSON ExportTraceServiceRequest. Unknown
// protobuf fields are ignored, matching the OTLP JSON receiver requirement.
func ParseTraceJSON(data []byte) (TraceExport, error) {
	if len(data) == 0 {
		return TraceExport{}, fmt.Errorf("OTLP trace document is empty")
	}
	if len(data) > MaxDocumentBytes {
		return TraceExport{}, fmt.Errorf("OTLP trace document is %d bytes, limit is %d", len(data), MaxDocumentBytes)
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	var request TraceExport
	if err := decoder.Decode(&request); err != nil {
		return TraceExport{}, fmt.Errorf("decode OTLP trace JSON: %w", err)
	}
	if decoder.More() {
		return TraceExport{}, fmt.Errorf("OTLP trace document contains multiple JSON values")
	}
	var trailing any
	if err := decoder.Decode(&trailing); err == nil {
		return TraceExport{}, fmt.Errorf("OTLP trace document contains trailing JSON data")
	}
	if err := ValidateTraceExport(request); err != nil {
		return TraceExport{}, err
	}
	return request, nil
}

func MarshalTraceJSON(request TraceExport) ([]byte, error) {
	if err := ValidateTraceExport(request); err != nil {
		return nil, err
	}
	encoded, err := json.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("encode OTLP trace JSON: %w", err)
	}
	if len(encoded) > MaxDocumentBytes {
		return nil, fmt.Errorf("encoded OTLP trace document is %d bytes, limit is %d", len(encoded), MaxDocumentBytes)
	}
	return encoded, nil
}

func ValidateTraceExport(request TraceExport) error {
	spanCount := 0
	for resourceIndex, resource := range request.ResourceSpans {
		if len(resource.Resource.Attributes) > MaxAttributes {
			return fmt.Errorf("resourceSpans[%d] has too many resource attributes", resourceIndex)
		}
		if err := validateAttributes(resource.Resource.Attributes, 0); err != nil {
			return fmt.Errorf("resourceSpans[%d] resource: %w", resourceIndex, err)
		}
		for scopeIndex, scope := range resource.ScopeSpans {
			if len(scope.Scope.Attributes) > MaxAttributes {
				return fmt.Errorf("resourceSpans[%d].scopeSpans[%d] has too many scope attributes", resourceIndex, scopeIndex)
			}
			if err := validateAttributes(scope.Scope.Attributes, 0); err != nil {
				return fmt.Errorf("resourceSpans[%d].scopeSpans[%d] scope: %w", resourceIndex, scopeIndex, err)
			}
			for spanIndex, span := range scope.Spans {
				spanCount++
				if spanCount > MaxSpans {
					return fmt.Errorf("OTLP trace document exceeds %d spans", MaxSpans)
				}
				if err := validateSpan(span); err != nil {
					return fmt.Errorf("resourceSpans[%d].scopeSpans[%d].spans[%d]: %w", resourceIndex, scopeIndex, spanIndex, err)
				}
			}
		}
	}
	return nil
}

func validateSpan(span Span) error {
	if span.Name == "" {
		return fmt.Errorf("span name is required")
	}
	if err := validateHexID(span.TraceID, 16, "traceId"); err != nil {
		return err
	}
	if err := validateHexID(span.SpanID, 8, "spanId"); err != nil {
		return err
	}
	if span.ParentSpanID != "" {
		if err := validateHexID(span.ParentSpanID, 8, "parentSpanId"); err != nil {
			return err
		}
	}
	if span.EndTimeUnixNano != 0 && span.StartTimeUnixNano != 0 && span.EndTimeUnixNano < span.StartTimeUnixNano {
		return fmt.Errorf("span end time precedes start time")
	}
	if len(span.Attributes) > MaxAttributes {
		return fmt.Errorf("span has too many attributes")
	}
	if err := validateAttributes(span.Attributes, 0); err != nil {
		return err
	}
	if len(span.Events) > MaxEventsPerSpan {
		return fmt.Errorf("span has too many events")
	}
	for index, event := range span.Events {
		if strings.TrimSpace(event.Name) == "" {
			return fmt.Errorf("event[%d] name is required", index)
		}
		if len(event.Attributes) > MaxAttributes {
			return fmt.Errorf("event[%d] has too many attributes", index)
		}
		if err := validateAttributes(event.Attributes, 0); err != nil {
			return fmt.Errorf("event[%d]: %w", index, err)
		}
	}
	return nil
}

func validateHexID(value string, bytesRequired int, field string) error {
	if len(value) != bytesRequired*2 {
		return fmt.Errorf("%s must be %d hex characters", field, bytesRequired*2)
	}
	decoded, err := hex.DecodeString(value)
	if err != nil {
		return fmt.Errorf("%s is not hexadecimal: %w", field, err)
	}
	allZero := true
	for _, b := range decoded {
		allZero = allZero && b == 0
	}
	if allZero {
		return fmt.Errorf("%s must not be all zero", field)
	}
	return nil
}

func validateAttributes(attributes []KeyValue, depth int) error {
	for index, attribute := range attributes {
		if strings.TrimSpace(attribute.Key) == "" {
			return fmt.Errorf("attribute[%d] key is required", index)
		}
		if err := validateAnyValue(attribute.Value, depth+1); err != nil {
			return fmt.Errorf("attribute %q: %w", attribute.Key, err)
		}
	}
	return nil
}

func validateAnyValue(value AnyValue, depth int) error {
	if depth > MaxAnyValueDepth {
		return fmt.Errorf("AnyValue nesting exceeds %d", MaxAnyValueDepth)
	}
	set := 0
	if value.StringValue != nil { set++ }
	if value.BoolValue != nil { set++ }
	if value.IntValue != nil { set++ }
	if value.DoubleValue != nil { set++ }
	if value.ArrayValue != nil { set++ }
	if value.KVListValue != nil { set++ }
	if value.BytesValue != nil { set++ }
	if set != 1 {
		return fmt.Errorf("AnyValue must contain exactly one value, got %d", set)
	}
	if value.ArrayValue != nil {
		for _, child := range value.ArrayValue.Values {
			if err := validateAnyValue(child, depth+1); err != nil {
				return err
			}
		}
	}
	if value.KVListValue != nil {
		if len(value.KVListValue.Values) > MaxAttributes {
			return fmt.Errorf("kvlist has too many values")
		}
		if err := validateAttributes(value.KVListValue.Values, depth+1); err != nil {
			return err
		}
	}
	return nil
}

func attributesMap(attributes []KeyValue) map[string]AnyValue {
	out := make(map[string]AnyValue, len(attributes))
	for _, attribute := range attributes {
		out[attribute.Key] = attribute.Value
	}
	return out
}

func anyString(value AnyValue) (string, bool) {
	if value.StringValue == nil {
		return "", false
	}
	return *value.StringValue, true
}

func anyInt64(value AnyValue) (int64, bool) {
	if value.IntValue == nil {
		return 0, false
	}
	return int64(*value.IntValue), true
}
