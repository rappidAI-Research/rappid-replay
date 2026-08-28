// Package generic provides Replay's mandatory no-assumptions adapter. It adds no
// provider-specific semantics and therefore works for every executable that the
// Generic Recorder can launch.
package generic

import (
	"context"

	"github.com/rappidAI-Research/rappid-replay/pkg/adapter"
)

const (
	ID      = "generic"
	Version = "1.0.0"
)

// Adapter is the built-in fallback implementation.
type Adapter struct{}

func New() *Adapter { return &Adapter{} }

func (*Adapter) Descriptor() adapter.Descriptor {
	return adapter.Descriptor{ID: ID, Version: Version}
}

func (*Adapter) Detect(context.Context, adapter.DetectionInput) (adapter.Detection, error) {
	return adapter.Detection{Matched: true, Confidence: 0, Reason: "generic recorder fallback"}, nil
}

func (*Adapter) Capabilities() adapter.Capabilities { return adapter.Capabilities{} }

func (*Adapter) EnrichProcess(context.Context, adapter.RunContext, adapter.ProcessObservation) (adapter.ProcessEnrichment, error) {
	return adapter.ProcessEnrichment{}, nil
}

func (*Adapter) StreamEvents(context.Context, adapter.RunContext, adapter.EventEmitter) error {
	return nil
}

func (*Adapter) Environment(context.Context, adapter.RunContext) (adapter.EnvironmentMetadata, error) {
	return adapter.EnvironmentMetadata{}, nil
}

func (*Adapter) RedactionHints(context.Context, adapter.RunContext) ([]adapter.RedactionHint, error) {
	return nil, nil
}
