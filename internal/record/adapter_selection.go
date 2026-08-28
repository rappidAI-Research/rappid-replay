package record

import (
	"context"
	"fmt"

	"github.com/rappidAI-Research/rappid-replay/adapters/generic"
	"github.com/rappidAI-Research/rappid-replay/pkg/adapter"
)

func selectRunAdapter(ctx context.Context, deps Dependencies, command []string, workingDir string) (adapter.Selection, error) {
	registry := deps.Adapters
	if registry == nil {
		var err error
		registry, err = adapter.NewRegistry(generic.New())
		if err != nil {
			return adapter.Selection{}, fmt.Errorf("initialize generic adapter registry: %w", err)
		}
	}
	return registry.Select(ctx, adapter.DetectionInput{
		Command:    append([]string(nil), command...),
		WorkingDir: workingDir,
	}), nil
}
