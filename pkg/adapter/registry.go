package adapter

import (
	"context"
	"fmt"
	"sort"
)

// Diagnostic records an adapter failure without turning that failure into a
// recorder failure. Adapters enrich evidence; they never gate generic capture.
type Diagnostic struct {
	AdapterID string `json:"adapter_id"`
	Stage     string `json:"stage"`
	Message   string `json:"message"`
}

// Selection is the adapter chosen for one run plus any non-fatal detection
// diagnostics encountered while choosing it.
type Selection struct {
	Adapter     Adapter      `json:"-"`
	Descriptor  Descriptor   `json:"descriptor"`
	Detection   Detection    `json:"detection"`
	Diagnostics []Diagnostic `json:"diagnostics,omitempty"`
}

// Registry holds one mandatory fallback and zero or more specialized adapters.
// Specialized detection failures are recorded as diagnostics and skipped.
type Registry struct {
	fallback   Adapter
	candidates []Adapter
}

// NewRegistry validates unique adapter identities and installs fallback as the
// unconditional capture-safe selection when no specialized adapter matches.
func NewRegistry(fallback Adapter, candidates ...Adapter) (*Registry, error) {
	if fallback == nil {
		return nil, fmt.Errorf("adapter fallback is required")
	}
	if err := fallback.Descriptor().Validate(); err != nil {
		return nil, fmt.Errorf("invalid fallback adapter: %w", err)
	}
	seen := map[string]struct{}{fallback.Descriptor().ID: {}}
	validated := make([]Adapter, 0, len(candidates))
	for _, candidate := range candidates {
		if candidate == nil {
			return nil, fmt.Errorf("nil specialized adapter")
		}
		descriptor := candidate.Descriptor()
		if err := descriptor.Validate(); err != nil {
			return nil, fmt.Errorf("invalid specialized adapter: %w", err)
		}
		if _, exists := seen[descriptor.ID]; exists {
			return nil, fmt.Errorf("duplicate adapter id %q", descriptor.ID)
		}
		seen[descriptor.ID] = struct{}{}
		validated = append(validated, candidate)
	}
	sort.Slice(validated, func(i, j int) bool {
		return validated[i].Descriptor().ID < validated[j].Descriptor().ID
	})
	return &Registry{fallback: fallback, candidates: validated}, nil
}

// Select chooses the highest-confidence specialized match. Equal-confidence
// ties are deterministic by adapter ID. A broken specialized detector is never
// allowed to prevent the fallback from being selected.
func (r *Registry) Select(ctx context.Context, input DetectionInput) Selection {
	selection := Selection{
		Adapter:    r.fallback,
		Descriptor: r.fallback.Descriptor(),
		Detection: Detection{
			Matched:    true,
			Confidence: 0,
			Reason:     "fallback",
		},
	}
	bestConfidence := uint8(0)
	bestID := selection.Descriptor.ID

	for _, candidate := range r.candidates {
		descriptor := candidate.Descriptor()
		detection, err := candidate.Detect(ctx, input)
		if err != nil {
			selection.Diagnostics = append(selection.Diagnostics, Diagnostic{
				AdapterID: descriptor.ID,
				Stage:     "detect",
				Message:   err.Error(),
			})
			continue
		}
		if !detection.Matched {
			continue
		}
		if detection.Confidence == 0 {
			selection.Diagnostics = append(selection.Diagnostics, Diagnostic{
				AdapterID: descriptor.ID,
				Stage:     "detect",
				Message:   "matched specialized adapter reported zero confidence",
			})
			continue
		}
		if detection.Confidence < bestConfidence || (detection.Confidence == bestConfidence && descriptor.ID >= bestID) {
			continue
		}
		selection.Adapter = candidate
		selection.Descriptor = descriptor
		selection.Detection = detection
		bestConfidence = detection.Confidence
		bestID = descriptor.ID
	}
	return selection
}

// Descriptors returns a deterministic snapshot of all registered adapters,
// including fallback first followed by specialized adapters sorted by ID.
func (r *Registry) Descriptors() []Descriptor {
	out := make([]Descriptor, 0, len(r.candidates)+1)
	out = append(out, r.fallback.Descriptor())
	for _, candidate := range r.candidates {
		out = append(out, candidate.Descriptor())
	}
	return out
}
