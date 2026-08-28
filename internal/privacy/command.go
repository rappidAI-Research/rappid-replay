package privacy

import "strings"

var sensitiveArgumentNames = map[string]struct{}{
	"api-key":       {},
	"apikey":        {},
	"access-token":  {},
	"auth-token":    {},
	"token":         {},
	"secret":        {},
	"client-secret": {},
	"password":      {},
	"passwd":        {},
}

// RedactCommandArgs returns a copy suitable for durable session/process
// metadata. The original argv remains available to the child process only;
// Replay never depends on historical secrets to re-execute a run.
func RedactCommandArgs(args []string) ([]string, bool) {
	out := append([]string(nil), args...)
	redacted := false
	for index := 0; index < len(out); index++ {
		arg := out[index]
		name, value, hasValue := strings.Cut(arg, "=")
		if isSensitiveFlagName(name) {
			if hasValue {
				out[index] = name + "=[REDACTED]"
				redacted = true
				continue
			}
			if index+1 < len(out) {
				out[index+1] = "[REDACTED]"
				redacted = true
				index++
			}
			continue
		}

		// Environment-style assignments sometimes appear as arguments to launch
		// wrappers such as `env`. Treat credential-shaped names identically.
		if hasValue && isSensitiveAssignmentName(name) && value != "" {
			out[index] = name + "=[REDACTED]"
			redacted = true
		}
	}
	return out, redacted
}

func isSensitiveFlagName(value string) bool {
	value = strings.TrimSpace(value)
	if !strings.HasPrefix(value, "-") {
		return false
	}
	name := strings.ToLower(strings.TrimLeft(value, "-"))
	_, ok := sensitiveArgumentNames[name]
	return ok
}

func isSensitiveAssignmentName(value string) bool {
	name := strings.ToLower(strings.TrimSpace(value))
	name = strings.ReplaceAll(name, "_", "-")
	_, ok := sensitiveArgumentNames[name]
	return ok
}
