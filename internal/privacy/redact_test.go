package privacy

import (
	"bytes"
	"testing"
)

func TestRedactKnownSecrets(t *testing.T) {
	input := []byte("token=github_pat_ABCDEFGHIJKLMNOPQRSTUVWXYZ123456\nAuthorization: Bearer abcdefghijklmnopqrstuvwxyz.1234567890\npassword=hunter123\nplain output\n")
	got, redacted := RedactKnownSecrets(input)
	if !redacted {
		t.Fatal("RedactKnownSecrets() did not report redaction")
	}
	for _, forbidden := range [][]byte{
		[]byte("github_pat_ABCDEFGHIJKLMNOPQRSTUVWXYZ123456"),
		[]byte("abcdefghijklmnopqrstuvwxyz.1234567890"),
		[]byte("hunter123"),
	} {
		if bytes.Contains(got, forbidden) {
			t.Fatalf("redacted output still contains secret %q: %q", forbidden, got)
		}
	}
	if !bytes.Contains(got, []byte("plain output")) || !bytes.Contains(got, []byte("[REDACTED]")) {
		t.Fatalf("redacted output = %q", got)
	}
}

func TestRedactKnownSecretsLeavesOrdinaryBytesUntouched(t *testing.T) {
	input := []byte{0xff, 'h', 'e', 'l', 'l', 'o', '\n'}
	got, redacted := RedactKnownSecrets(input)
	if redacted {
		t.Fatal("ordinary output was unexpectedly redacted")
	}
	if !bytes.Equal(got, input) {
		t.Fatalf("ordinary output changed: %v != %v", got, input)
	}
}
