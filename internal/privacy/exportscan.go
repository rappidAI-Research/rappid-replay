package privacy

import (
	"bytes"
	"fmt"
	"regexp"
)

// SecretFinding identifies a known credential pattern without retaining the
// matched secret itself.
type SecretFinding struct {
	Source  string
	Pattern string
}

var exportSecretPatterns = []struct {
	name string
	re   *regexp.Regexp
}{
	{"private-key", regexp.MustCompile(`-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----`)},
	{"openai-key", regexp.MustCompile(`\bsk-(?:proj-)?[A-Za-z0-9_-]{20,}\b`)},
	{"github-token", regexp.MustCompile(`\b(?:gh[pousr]_[A-Za-z0-9]{20,}|github_pat_[A-Za-z0-9_]{20,})\b`)},
	{"aws-access-key", regexp.MustCompile(`\bAKIA[0-9A-Z]{16}\b`)},
	{"slack-token", regexp.MustCompile(`\bxox[baprs]-[A-Za-z0-9-]{16,}\b`)},
	{"credential-assignment", regexp.MustCompile(`(?i)\b(?:api[_-]?key|access[_-]?token|auth[_-]?token|password|passwd|secret)\b\s*[:=]\s*["']?[A-Za-z0-9_./+=-]{12,}`)},
}

const maxSecretFindings = 100

// ScanExportBytes performs a bounded known-pattern scan. It intentionally
// reports only source labels and pattern names so diagnostics cannot leak the
// credential they are warning about.
func ScanExportBytes(source string, data []byte) []SecretFinding {
	if len(data) == 0 {
		return nil
	}
	findings := make([]SecretFinding, 0)
	for _, pattern := range exportSecretPatterns {
		if pattern.re.FindIndex(data) != nil {
			findings = append(findings, SecretFinding{Source: source, Pattern: pattern.name})
			if len(findings) >= maxSecretFindings {
				break
			}
		}
	}
	return findings
}

// MergeSecretFindings deduplicates source/pattern pairs and enforces the global
// result bound used by export.
func MergeSecretFindings(groups ...[]SecretFinding) []SecretFinding {
	result := make([]SecretFinding, 0)
	seen := make(map[string]struct{})
	for _, group := range groups {
		for _, finding := range group {
			key := finding.Source + "\x00" + finding.Pattern
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			result = append(result, finding)
			if len(result) >= maxSecretFindings {
				return result
			}
		}
	}
	return result
}

// SecretScanError is returned by block-mode export. Error text contains no
// secret values.
type SecretScanError struct {
	Findings []SecretFinding
}

func (e *SecretScanError) Error() string {
	if e == nil || len(e.Findings) == 0 {
		return "export secret scan blocked archive"
	}
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "export secret scan found %d potential secret(s)", len(e.Findings))
	for i, finding := range e.Findings {
		if i >= 3 {
			buf.WriteString("; ...")
			break
		}
		fmt.Fprintf(&buf, "; %s in %s", finding.Pattern, finding.Source)
	}
	return buf.String()
}
