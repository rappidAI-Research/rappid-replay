// Package privacy implements deterministic, local-only privacy transforms for
// evidence that is not required for byte-exact workspace reconstruction.
package privacy

import (
	"bytes"
	"regexp"
)

var knownSecretPatterns = []*regexp.Regexp{
	// Common provider and developer-platform token prefixes.
	regexp.MustCompile(`(?i)\bsk-[A-Za-z0-9_-]{16,}`),
	regexp.MustCompile(`\bgh[pousr]_[A-Za-z0-9]{20,}`),
	regexp.MustCompile(`\bgithub_pat_[A-Za-z0-9_]{20,}`),
	// AWS access-key identifiers are sensitive even though they are not the
	// corresponding secret access key.
	regexp.MustCompile(`\bAKIA[0-9A-Z]{16}\b`),
	// JWT-like bearer material.
	regexp.MustCompile(`\beyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\b`),
	// Explicit credential assignments and HTTP authorization lines. These are
	// deliberately conservative and require a credential-shaped label.
	regexp.MustCompile(`(?i)\b(?:api[_-]?key|access[_-]?token|auth[_-]?token|secret|password|passwd)\s*[:=]\s*[^\s'";]{4,}`),
	regexp.MustCompile(`(?i)\bauthorization\s*:\s*(?:bearer|basic)\s+[A-Za-z0-9._~+/=-]{8,}`),
}

var redactionMarker = []byte("[REDACTED]")

// RedactKnownSecrets removes known credential shapes from terminal or agent
// text before it is persisted in Replay's plaintext metadata/event index. The
// returned bytes are a new allocation whenever a redaction occurs.
func RedactKnownSecrets(input []byte) ([]byte, bool) {
	output := input
	redacted := false
	for _, pattern := range knownSecretPatterns {
		if !pattern.Match(output) {
			continue
		}
		if !redacted {
			output = bytes.Clone(output)
			redacted = true
		}
		output = pattern.ReplaceAll(output, redactionMarker)
	}
	if !redacted {
		return input, false
	}
	return output, true
}
