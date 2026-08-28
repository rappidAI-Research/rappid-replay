package record

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
)

func TestCaptureEnvironmentVariablesDeterministicAndRedacted(t *testing.T) {
	variables, redacted, malformed := captureEnvironmentVariables([]string{
		"LANG=en_US.UTF-8",
		"OPENAI_API_KEY=top-secret",
		"ZETA=last",
		"ALPHA=first",
		"ZETA=replaced",
		"BROKEN",
	}, "linux")
	if redacted != 1 {
		t.Fatalf("redacted count = %d, want 1", redacted)
	}
	if malformed != 1 {
		t.Fatalf("malformed count = %d, want 1", malformed)
	}
	if len(variables) != 5 {
		t.Fatalf("variable count = %d, want 5", len(variables))
	}
	for index := 1; index < len(variables); index++ {
		if variables[index-1].Name >= variables[index].Name {
			t.Fatalf("variables are not deterministically sorted: %+v", variables)
		}
	}
	byName := make(map[string]environmentVariable, len(variables))
	for _, variable := range variables {
		byName[variable.Name] = variable
	}
	if got := byName["OPENAI_API_KEY"]; got.Value != "[REDACTED]" || !got.Redacted {
		t.Fatalf("OPENAI_API_KEY = %+v, want redacted", got)
	}
	if got := byName["ZETA"].Value; got != "replaced" {
		t.Fatalf("duplicate ZETA value = %q, want last value", got)
	}
	if got := byName["BROKEN"]; !got.Malformed || got.Value != "" {
		t.Fatalf("malformed variable = %+v", got)
	}
}

func TestCaptureEnvironmentVariablesWindowsNamesAreCaseInsensitive(t *testing.T) {
	variables, _, _ := captureEnvironmentVariables([]string{
		"Path=first",
		"PATH=second",
	}, "windows")
	if len(variables) != 1 || variables[0].Value != "second" {
		t.Fatalf("windows duplicate variables = %+v, want last case-insensitive value", variables)
	}
}

func TestCaptureGitContextDoesNotPersistRemoteOrPaths(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not available")
	}
	root := t.TempDir()
	runGitTestCommand(t, root, "init", "-q")
	runGitTestCommand(t, root, "config", "user.name", "Replay Test")
	runGitTestCommand(t, root, "config", "user.email", "replay@example.invalid")
	if err := os.WriteFile(filepath.Join(root, "secret-name.txt"), []byte("one"), 0o600); err != nil {
		t.Fatal(err)
	}
	runGitTestCommand(t, root, "add", "secret-name.txt")
	runGitTestCommand(t, root, "commit", "-q", "-m", "initial")
	runGitTestCommand(t, root, "remote", "add", "origin", "https://user:password@example.invalid/private.git")
	if err := os.WriteFile(filepath.Join(root, "secret-name.txt"), []byte("two"), 0o600); err != nil {
		t.Fatal(err)
	}

	git := captureGitContext(context.Background(), root)
	if !git.Available || !git.Repository || git.Head == "" {
		t.Fatalf("git context = %+v, want repository with HEAD", git)
	}
	if !git.DirtyKnown || !git.Dirty {
		t.Fatalf("git dirty state = %+v, want known dirty workspace", git)
	}
	if runtime.GOOS != "windows" && git.Detached && git.Branch != "" {
		t.Fatalf("git context cannot be both detached and named branch: %+v", git)
	}
}

func runGitTestCommand(t *testing.T, dir string, args ...string) {
	t.Helper()
	command := exec.Command("git", args...)
	command.Dir = dir
	command.Env = append(os.Environ(), "GIT_TERMINAL_PROMPT=0", "GIT_OPTIONAL_LOCKS=0")
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("git %v: %v: %s", args, err, output)
	}
}
