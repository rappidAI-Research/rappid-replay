package record

import (
	"context"
	"fmt"

	"github.com/rappidAI-Research/rappid-replay/adapters/generic"
	"github.com/rappidAI-Research/rappid-replay/internal/privacy"
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
	selection := registry.Select(ctx, adapter.DetectionInput{
		Command:    append([]string(nil), command...),
		WorkingDir: workingDir,
	})
	selection, _ = redactAdapterSelectionForPersistence(selection)
	return selection, nil
}

// redactAdapterSelectionForPersistence treats adapter-provided detection text as
// untrusted metadata. Hooks have not supplied additional redaction hints yet, so
// the built-in privacy policy is applied before session.started is persisted.
// The selected adapter handle and identity are preserved unchanged.
func redactAdapterSelectionForPersistence(selection adapter.Selection) (adapter.Selection, bool) {
	redactedAny := false
	if selection.Detection.Reason != "" {
		value, redacted := privacy.RedactKnownSecrets([]byte(selection.Detection.Reason))
		selection.Detection.Reason = string(value)
		redactedAny = redactedAny || redacted
	}
	if len(selection.Diagnostics) != 0 {
		diagnostics := append([]adapter.Diagnostic(nil), selection.Diagnostics...)
		for index := range diagnostics {
			value, redacted := privacy.RedactKnownSecrets([]byte(diagnostics[index].Message))
			diagnostics[index].Message = string(value)
			redactedAny = redactedAny || redacted
		}
		selection.Diagnostics = diagnostics
	}
	return selection, redactedAny
}
