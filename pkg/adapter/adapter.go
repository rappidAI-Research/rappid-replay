// Package adapter defines the stable semantic contract used by Replay agent
// adapters. Built-in adapters use this Go API directly; a future out-of-process
// plugin transport must preserve the same semantics without relying on Go's
// unstable plugin ABI.
package adapter

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
)

const ContractVersion = "rappid.replay.adapter/1"

var adapterIDPattern = regexp.MustCompile(`^[a-z][a-z0-9]*(?:[._-][a-z0-9]+)*$`)

// Descriptor is the immutable identity persisted with a Replay session.
type Descriptor struct {
	ID      string `json:"id"`
	Version string `json:"version"`
}

// Validate checks the portable adapter identity representation.
func (d Descriptor) Validate() error {
	if !adapterIDPattern.MatchString(d.ID) {
		return fmt.Errorf("invalid adapter id %q", d.ID)
	}
	if strings.TrimSpace(d.Version) == "" {
		return fmt.Errorf("adapter %s version is required", d.ID)
	}
	return nil
}

// Capabilities declare additive enrichment surfaces. False means the generic
// recorder remains the only evidence source for that dimension.
type Capabilities struct {
	ModelCalls   bool `json:"model_calls"`
	ToolCalls    bool `json:"tool_calls"`
	Messages     bool `json:"messages"`
	TokenUsage   bool `json:"token_usage"`
	CostMetadata bool `json:"cost_metadata"`
	OTel         bool `json:"otel"`
	ExternalIO   bool `json:"external_io"`
}

// DetectionInput contains privacy-filtered command metadata. Replay must not
// pass raw secrets to adapters merely to choose an enrichment implementation.
type DetectionInput struct {
	Command    []string `json:"command"`
	WorkingDir string   `json:"working_dir"`
}

// Detection describes whether an adapter recognizes a run. Confidence is only
// used to rank matching specialized adapters; 0 is reserved for fallbacks.
type Detection struct {
	Matched    bool   `json:"matched"`
	Confidence uint8  `json:"confidence"`
	Reason     string `json:"reason,omitempty"`
}

// RunContext is the stable run metadata available to additive hooks. Command is
// the same privacy-filtered command used for detection.
type RunContext struct {
	SessionID  string   `json:"session_id"`
	Command    []string `json:"command"`
	WorkingDir string   `json:"working_dir"`
}

// ProcessObservation is a provider-independent process fact that an adapter may
// enrich. Arguments are privacy-filtered before crossing the adapter boundary.
type ProcessObservation struct {
	PID        int      `json:"pid"`
	ParentPID  int      `json:"parent_pid,omitempty"`
	Executable string   `json:"executable,omitempty"`
	Arguments  []string `json:"arguments,omitempty"`
}

// ProcessEnrichment contains adapter-owned attributes. Keys should be stable,
// namespaced strings such as "codex.role". Empty enrichment is valid.
type ProcessEnrichment struct {
	Attributes map[string]string `json:"attributes,omitempty"`
}

// AdapterEvent is an additive event proposed by an adapter. The recorder owns
// sequence allocation, timestamps, state references, privacy validation, and
// durable persistence; adapters never write canonical history directly.
type AdapterEvent struct {
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload,omitempty"`
}

// EventEmitter is supplied by the recorder when an adapter event stream is
// active. Implementations must treat an emitter error as terminal for that
// stream and return promptly.
type EventEmitter func(context.Context, AdapterEvent) error

// EnvironmentMetadata is adapter-specific, non-secret environment metadata.
// Values are intentionally strings so adapters do not smuggle opaque executable
// state into the deterministic core.
type EnvironmentMetadata struct {
	Attributes map[string]string `json:"attributes,omitempty"`
}

// RedactionHintKind identifies a privacy hint an adapter can contribute.
type RedactionHintKind string

const (
	RedactEnvironmentName RedactionHintKind = "environment-name"
	RedactLiteral         RedactionHintKind = "literal"
)

// RedactionHint is advisory input to Replay's privacy layer. It cannot weaken
// built-in redaction rules; the recorder decides whether and how a hint is used.
type RedactionHint struct {
	Kind  RedactionHintKind `json:"kind"`
	Value string            `json:"value"`
}

// Adapter is the v1 additive enrichment contract. None of these hooks may be a
// prerequisite for the Generic Recorder's deterministic capture path.
type Adapter interface {
	Descriptor() Descriptor
	Detect(context.Context, DetectionInput) (Detection, error)
	Capabilities() Capabilities
	EnrichProcess(context.Context, RunContext, ProcessObservation) (ProcessEnrichment, error)
	StreamEvents(context.Context, RunContext, EventEmitter) error
	Environment(context.Context, RunContext) (EnvironmentMetadata, error)
	RedactionHints(context.Context, RunContext) ([]RedactionHint, error)
}
