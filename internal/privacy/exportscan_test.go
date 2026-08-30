package privacy

import (
	"strings"
	"testing"
)

func TestScanExportBytesFindsKnownSecretsWithoutReturningValue(t *testing.T) {
	secret := "sk-proj-abcdefghijklmnopqrstuvwxyz123456"
	findings := ScanExportBytes("event 7", []byte("OPENAI_API_KEY="+secret))
	if len(findings) == 0 {
		t.Fatal("ScanExportBytes() did not find known API key")
	}
	for _, finding := range findings {
		if finding.Source != "event 7" {
			t.Fatalf("finding source = %q", finding.Source)
		}
		if strings.Contains(finding.Pattern, secret) || strings.Contains(finding.Source, secret) {
			t.Fatal("secret value leaked through finding metadata")
		}
	}
	err := (&SecretScanError{Findings: findings}).Error()
	if strings.Contains(err, secret) {
		t.Fatal("SecretScanError leaked secret value")
	}
}

func TestScanExportBytesIgnoresOrdinaryContent(t *testing.T) {
	findings := ScanExportBytes("workspace", []byte("ordinary source code without credentials"))
	if len(findings) != 0 {
		t.Fatalf("ScanExportBytes() findings = %+v", findings)
	}
}

func TestMergeSecretFindingsDeduplicates(t *testing.T) {
	finding := SecretFinding{Source: "object b3:abc", Pattern: "private-key"}
	merged := MergeSecretFindings([]SecretFinding{finding}, []SecretFinding{finding})
	if len(merged) != 1 || merged[0] != finding {
		t.Fatalf("MergeSecretFindings() = %+v", merged)
	}
}
