package privacy

import (
	"strings"
	"unicode"
)

const EnvironmentRedactionMarker = "[REDACTED]"

var sensitiveEnvironmentTokens = map[string]struct{}{
	"AUTHORIZATION": {},
	"COOKIE":        {},
	"CREDENTIAL":    {},
	"CREDENTIALS":   {},
	"DSN":           {},
	"PASS":          {},
	"PASSWD":        {},
	"PASSWORD":      {},
	"SECRET":        {},
	"TOKEN":         {},
}

var sensitiveEnvironmentExactNames = map[string]struct{}{
	"DATABASE_URL": {},
	"MONGODB_URI":  {},
	"MONGO_URL":    {},
	"MYSQL_URL":    {},
	"POSTGRES_URL": {},
	"REDIS_URL":    {},
}

// RedactEnvironmentValue applies a conservative name-based policy before the
// generic content redactor. Environment values associated with credential-like
// names are never persisted, even when their value has no recognizable token
// syntax. Other values still pass through known-secret pattern redaction.
func RedactEnvironmentValue(name, value string) (string, bool) {
	if SensitiveEnvironmentName(name) {
		return EnvironmentRedactionMarker, true
	}
	redacted, changed := RedactKnownSecrets([]byte(value))
	return string(redacted), changed
}

// SensitiveEnvironmentName detects credential-bearing environment variable
// names without treating every occurrence of "KEY" or "AUTH" as sensitive.
// This keeps ordinary variables such as KEYBOARD_LAYOUT or SSH_AUTH_SOCK useful
// while covering common API/access/private/session key conventions.
func SensitiveEnvironmentName(name string) bool {
	normalized := normalizeEnvironmentName(name)
	if normalized == "" {
		return false
	}
	if _, ok := sensitiveEnvironmentExactNames[normalized]; ok {
		return true
	}
	for _, token := range strings.Split(normalized, "_") {
		if _, ok := sensitiveEnvironmentTokens[token]; ok {
			return true
		}
	}
	for _, marker := range []string{
		"API_KEY", "APIKEY", "ACCESS_KEY", "PRIVATE_KEY", "CLIENT_SECRET",
		"SESSION_KEY", "SIGNING_KEY", "ENCRYPTION_KEY",
	} {
		if strings.Contains(normalized, marker) {
			return true
		}
	}
	return false
}

func normalizeEnvironmentName(name string) string {
	var builder strings.Builder
	lastSeparator := false
	for _, r := range strings.ToUpper(strings.TrimSpace(name)) {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			builder.WriteRune(r)
			lastSeparator = false
			continue
		}
		if !lastSeparator && builder.Len() > 0 {
			builder.WriteByte('_')
			lastSeparator = true
	}
	return strings.Trim(builder.String(), "_")
}
