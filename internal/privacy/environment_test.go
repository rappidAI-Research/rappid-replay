package privacy

import "testing"

func TestRedactEnvironmentValue(t *testing.T) {
	tests := []struct {
		name     string
		variable string
		value    string
		want     string
		redacted bool
	}{
		{name: "password name", variable: "DATABASE_PASSWORD", value: "correct-horse", want: EnvironmentRedactionMarker, redacted: true},
		{name: "pass alias", variable: "DB_PASS", value: "correct-horse", want: EnvironmentRedactionMarker, redacted: true},
		{name: "api key name", variable: "OPENAI_API_KEY", value: "not-token-shaped", want: EnvironmentRedactionMarker, redacted: true},
		{name: "database url", variable: "DATABASE_URL", value: "postgres://user:password@example.invalid/db", want: EnvironmentRedactionMarker, redacted: true},
		{name: "dsn", variable: "ERROR_DSN", value: "https://public:secret@example.invalid/1", want: EnvironmentRedactionMarker, redacted: true},
		{name: "known secret in ordinary variable", variable: "MESSAGE", value: "token sk-1234567890abcdefghijkl", want: "token [REDACTED]", redacted: true},
		{name: "ordinary value", variable: "LANG", value: "en_US.UTF-8", want: "en_US.UTF-8", redacted: false},
		{name: "auth socket is metadata", variable: "SSH_AUTH_SOCK", value: "/tmp/agent.sock", want: "/tmp/agent.sock", redacted: false},
		{name: "key substring is not enough", variable: "KEYBOARD_LAYOUT", value: "de", want: "de", redacted: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, redacted := RedactEnvironmentValue(test.variable, test.value)
			if got != test.want || redacted != test.redacted {
				t.Fatalf("RedactEnvironmentValue(%q) = (%q, %v), want (%q, %v)", test.variable, got, redacted, test.want, test.redacted)
			}
		})
	}
}
