package record

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"sort"
	"strings"
	"time"

	"github.com/rappidAI-Research/rappid-replay/internal/privacy"
)

const environmentSchemaV1 = "rappid.replay.environment/1"

type environmentFingerprint struct {
	Schema    string                `json:"schema"`
	OS        string                `json:"os"`
	Arch      string                `json:"arch"`
	GoVersion string                `json:"go_version"`
	Variables []environmentVariable `json:"variables"`
	Git       gitContext            `json:"git"`
}

type environmentVariable struct {
	Name      string `json:"name"`
	Value     string `json:"value"`
	Redacted  bool   `json:"redacted"`
	Malformed bool   `json:"malformed,omitempty"`
}

type environmentSummary struct {
	Schema         string `json:"schema"`
	Variables      int    `json:"variables"`
	Redacted       int    `json:"redacted"`
	Malformed      int    `json:"malformed"`
	GitAvailable   bool   `json:"git_available"`
	GitRepository  bool   `json:"git_repository"`
	GitDirtyKnown  bool   `json:"git_dirty_known"`
}

type gitContext struct {
	Available  bool   `json:"available"`
	Repository bool   `json:"repository"`
	Head       string `json:"head,omitempty"`
	Branch     string `json:"branch,omitempty"`
	Detached   bool   `json:"detached,omitempty"`
	Dirty      bool   `json:"dirty,omitempty"`
	DirtyKnown bool   `json:"dirty_known,omitempty"`
}

func captureExecutionEnvironment(ctx context.Context, workingDir string, configured []string) ([]byte, environmentSummary, gitContext, error) {
	effective := configured
	if effective == nil {
		effective = os.Environ()
	}
	variables, redactedCount, malformedCount := captureEnvironmentVariables(effective, runtime.GOOS)
	git := captureGitContext(ctx, workingDir)
	fingerprint := environmentFingerprint{
		Schema:    environmentSchemaV1,
		OS:        runtime.GOOS,
		Arch:      runtime.GOARCH,
		GoVersion: runtime.Version(),
		Variables: variables,
		Git:       git,
	}
	encoded, err := json.Marshal(fingerprint)
	if err != nil {
		return nil, environmentSummary{}, gitContext{}, fmt.Errorf("encode execution environment: %w", err)
	}
	summary := environmentSummary{
		Schema:        environmentSchemaV1,
		Variables:     len(variables),
		Redacted:      redactedCount,
		Malformed:     malformedCount,
		GitAvailable:  git.Available,
		GitRepository: git.Repository,
		GitDirtyKnown: git.DirtyKnown,
	}
	return encoded, summary, git, nil
}

func captureEnvironmentVariables(entries []string, goos string) ([]environmentVariable, int, int) {
	// execve semantics are effectively last-value-wins for duplicate names in
	// the environments Replay targets. Windows names are case-insensitive.
	byName := make(map[string]environmentVariable, len(entries))
	for _, entry := range entries {
		name, value, ok := strings.Cut(entry, "=")
		if name == "" {
			continue
		}
		redactedValue, redacted := privacy.RedactEnvironmentValue(name, value)
		item := environmentVariable{Name: name, Value: redactedValue, Redacted: redacted, Malformed: !ok}
		key := name
		if goos == "windows" {
			key = strings.ToUpper(name)
		}
		byName[key] = item
	}

	variables := make([]environmentVariable, 0, len(byName))
	for _, item := range byName {
		variables = append(variables, item)
	}
	sort.Slice(variables, func(i, j int) bool {
		left := variables[i].Name
		right := variables[j].Name
		if goos == "windows" {
			left = strings.ToUpper(left)
			right = strings.ToUpper(right)
		}
		return left < right
	})

	redactedCount := 0
	malformedCount := 0
	for _, item := range variables {
		if item.Redacted {
			redactedCount++
		}
		if item.Malformed {
			malformedCount++
		}
	}
	return variables, redactedCount, malformedCount
}

func captureGitContext(ctx context.Context, workingDir string) gitContext {
	gitPath, err := exec.LookPath("git")
	if err != nil {
		return gitContext{}
	}
	result := gitContext{Available: true}

	probeCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	inside, err := runGit(probeCtx, gitPath, workingDir, "rev-parse", "--is-inside-work-tree")
	if err != nil || strings.TrimSpace(inside) != "true" {
		return result
	}
	result.Repository = true

	if head, headErr := runGit(probeCtx, gitPath, workingDir, "rev-parse", "--verify", "HEAD"); headErr == nil {
		result.Head = strings.TrimSpace(head)
	}
	if branch, branchErr := runGit(probeCtx, gitPath, workingDir, "symbolic-ref", "--quiet", "--short", "HEAD"); branchErr == nil {
		result.Branch = strings.TrimSpace(branch)
	} else if result.Head != "" {
		result.Detached = true
	}
	if status, statusErr := runGit(probeCtx, gitPath, workingDir, "-c", "core.fsmonitor=false", "-c", "core.untrackedCache=false", "status", "--porcelain=v1", "--untracked-files=normal"); statusErr == nil {
		result.DirtyKnown = true
		result.Dirty = len(status) != 0
	}
	return result
}

func runGit(ctx context.Context, gitPath, workingDir string, args ...string) (string, error) {
	command := exec.CommandContext(ctx, gitPath, args...)
	command.Dir = workingDir
	command.Env = append(os.Environ(),
		"GIT_TERMINAL_PROMPT=0",
		"GIT_OPTIONAL_LOCKS=0",
		"GIT_PAGER=cat",
	)
	output, err := command.Output()
	if err != nil {
		if errors.Is(ctx.Err(), context.DeadlineExceeded) || errors.Is(ctx.Err(), context.Canceled) {
			return "", ctx.Err()
		}
		return "", err
	}
	return string(output), nil
}
