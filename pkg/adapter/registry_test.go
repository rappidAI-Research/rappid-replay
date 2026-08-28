package adapter_test

import (
	"context"
	"errors"
	"testing"

	"github.com/rappidAI-Research/rappid-replay/adapters/generic"
	"github.com/rappidAI-Research/rappid-replay/pkg/adapter"
)

type stubAdapter struct {
	descriptor adapter.Descriptor
	detection  adapter.Detection
	detectErr  error
}

func (s stubAdapter) Descriptor() adapter.Descriptor { return s.descriptor }
func (s stubAdapter) Detect(context.Context, adapter.DetectionInput) (adapter.Detection, error) {
	return s.detection, s.detectErr
}
func (s stubAdapter) Capabilities() adapter.Capabilities { return adapter.Capabilities{} }
func (s stubAdapter) EnrichProcess(context.Context, adapter.RunContext, adapter.ProcessObservation) (adapter.ProcessEnrichment, error) {
	return adapter.ProcessEnrichment{}, nil
}
func (s stubAdapter) StreamEvents(context.Context, adapter.RunContext, adapter.EventEmitter) error {
	return nil
}
func (s stubAdapter) Environment(context.Context, adapter.RunContext) (adapter.EnvironmentMetadata, error) {
	return adapter.EnvironmentMetadata{}, nil
}
func (s stubAdapter) RedactionHints(context.Context, adapter.RunContext) ([]adapter.RedactionHint, error) {
	return nil, nil
}

func TestRegistrySelectsHighestConfidenceMatchDeterministically(t *testing.T) {
	registry, err := adapter.NewRegistry(
		generic.New(),
		stubAdapter{
			descriptor: adapter.Descriptor{ID: "zeta", Version: "1.0.0"},
			detection:  adapter.Detection{Matched: true, Confidence: 90, Reason: "zeta match"},
		},
		stubAdapter{
			descriptor: adapter.Descriptor{ID: "alpha", Version: "2.0.0"},
			detection:  adapter.Detection{Matched: true, Confidence: 90, Reason: "alpha match"},
		},
		stubAdapter{
			descriptor: adapter.Descriptor{ID: "weak", Version: "1.0.0"},
			detection:  adapter.Detection{Matched: true, Confidence: 40},
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	selection := registry.Select(context.Background(), adapter.DetectionInput{Command: []string{"agent"}})
	if selection.Descriptor.ID != "alpha" || selection.Descriptor.Version != "2.0.0" {
		t.Fatalf("selection = %+v, want alpha@2.0.0", selection.Descriptor)
	}
	if selection.Detection.Confidence != 90 || selection.Detection.Reason != "alpha match" {
		t.Fatalf("detection = %+v", selection.Detection)
	}
}

func TestRegistryFallsBackWhenSpecializedDetectionFails(t *testing.T) {
	registry, err := adapter.NewRegistry(
		generic.New(),
		stubAdapter{
			descriptor: adapter.Descriptor{ID: "broken", Version: "1.0.0"},
			detectErr:  errors.New("detector unavailable"),
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	selection := registry.Select(context.Background(), adapter.DetectionInput{Command: []string{"unknown"}})
	if selection.Descriptor.ID != generic.ID {
		t.Fatalf("selection = %s, want generic fallback", selection.Descriptor.ID)
	}
	if len(selection.Diagnostics) != 1 || selection.Diagnostics[0].AdapterID != "broken" || selection.Diagnostics[0].Stage != "detect" {
		t.Fatalf("diagnostics = %+v", selection.Diagnostics)
	}
}

func TestRegistryRejectsDuplicateAndInvalidAdapterIDs(t *testing.T) {
	if _, err := adapter.NewRegistry(generic.New(), stubAdapter{
		descriptor: adapter.Descriptor{ID: generic.ID, Version: "2.0.0"},
	}); err == nil {
		t.Fatal("NewRegistry() accepted duplicate adapter id")
	}
	if _, err := adapter.NewRegistry(generic.New(), stubAdapter{
		descriptor: adapter.Descriptor{ID: "Bad Adapter", Version: "1.0.0"},
	}); err == nil {
		t.Fatal("NewRegistry() accepted invalid adapter id")
	}
}
