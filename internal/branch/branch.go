// Package branch implements deterministic branching from immutable Replay states
// and controlled re-execution from those exact historical workspaces.
package branch

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/rappidAI-Research/rappid-replay/internal/id"
	"github.com/rappidAI-Research/rappid-replay/internal/persistence"
	"github.com/rappidAI-Research/rappid-replay/internal/record"
	"github.com/rappidAI-Research/rappid-replay/internal/replay"
	"github.com/rappidAI-Research/rappid-replay/internal/store"
	"github.com/rappidAI-Research/rappid-replay/internal/terminal"
	"github.com/rappidAI-Research/rappid-replay/pkg/adapter"
)

// Mode is the architecture-defined rerun strategy.
type Mode string

const (
	ModeRecorded   Mode = "recorded"
	ModeLive       Mode = "live"
	ModeControlled Mode = "controlled"
	ModeHybrid     Mode = "hybrid"
)

// Dependencies are the local deterministic subsystems used by branch/rerun.
type Dependencies struct {
	DB       *persistence.DB
	CAS      *store.LocalStore
	Adapters *adapter.Registry
}

// CreateOptions controls a non-executing branch materialization.
type CreateOptions struct {
	StateID     id.StateID
	Destination string
	Force       bool
}

// Result identifies a materialized branch and its immutable source lineage.
type Result struct {
	Source      persistence.StateRecord
	Destination string
}

// RerunOptions controls live re-execution from one selected historical state.
// Command is always explicit: Replay never reconstructs secrets or silently
// reuses a redacted historical argv. ConfirmExecution is required because a
// live command can perform arbitrary external side effects.
type RerunOptions struct {
	StateID             id.StateID
	Destination         string
	Force               bool
	Mode                Mode
	ConfirmExecution    bool
	Command             []string
	TerminalInput       string
	PTY                 bool
	InitialTerminalSize terminal.Size
	TerminalResize      <-chan terminal.Size
	Stdin               io.Reader
	Stdout              io.Writer
	Stderr              io.Writer
	Env                 []string
}

// RerunResult contains both the branch point and the newly recorded child
// session. The child session persists parent_session_id + fork_event_seq.
type RerunResult struct {
	Branch Result
	Run    record.Result
	Mode   Mode
}

// DefaultDestination returns a deterministic branch workspace name. Existing
// destinations are never reused unless Force is explicit.
func DefaultDestination(stateID id.StateID) string {
	return "rappid-branch-" + stateID.String()
}

// ParseMode validates the stable rerun mode vocabulary.
func ParseMode(value string) (Mode, error) {
	mode := Mode(strings.TrimSpace(value))
	if mode == "" {
		mode = ModeLive
	}
	switch mode {
	case ModeRecorded, ModeLive, ModeControlled, ModeHybrid:
		return mode, nil
	default:
		return "", fmt.Errorf("invalid rerun mode %q (want recorded, live, controlled, or hybrid)", value)
	}
}

// Create authenticates and materializes a historical state without executing
// agents, tools, shell commands, or restored code.
func Create(ctx context.Context, deps Dependencies, options CreateOptions) (Result, error) {
	if err := validateDependencies(deps); err != nil {
		return Result{}, err
	}
	if strings.TrimSpace(options.Destination) == "" {
		return Result{}, fmt.Errorf("branch destination is required")
	}
	restored, err := replay.RestoreState(ctx, replay.Dependencies{DB: deps.DB, CAS: deps.CAS}, replay.RestoreOptions{
		StateID:     options.StateID,
		Destination: options.Destination,
		Force:       options.Force,
	})
	if err != nil {
		return Result{}, err
	}
	return Result{Source: restored.State, Destination: restored.Destination}, nil
}

// Rerun creates an exact branch workspace and, in live mode, records a new
// child session from that state. The Generic Recorder re-captures the restored
// workspace and refuses to execute if its canonical root does not equal the
// selected historical state's root object.
func Rerun(ctx context.Context, deps Dependencies, options RerunOptions) (RerunResult, error) {
	if err := validateDependencies(deps); err != nil {
		return RerunResult{}, err
	}
	mode, err := ParseMode(string(options.Mode))
	if err != nil {
		return RerunResult{}, err
	}
	if mode != ModeLive {
		return RerunResult{}, fmt.Errorf("rerun mode %q is reserved but not executable until cassette/playback support is available", mode)
	}
	if !options.ConfirmExecution {
		return RerunResult{}, fmt.Errorf("live rerun requires explicit execution confirmation")
	}
	if len(options.Command) == 0 || strings.TrimSpace(options.Command[0]) == "" {
		return RerunResult{}, fmt.Errorf("live rerun requires an explicit command; historical command secrets are never reconstructed")
	}
	if strings.TrimSpace(options.Destination) == "" {
		return RerunResult{}, fmt.Errorf("rerun destination is required")
	}

	branched, err := Create(ctx, deps, CreateOptions{
		StateID:     options.StateID,
		Destination: options.Destination,
		Force:       options.Force,
	})
	if err != nil {
		return RerunResult{}, fmt.Errorf("materialize rerun branch: %w", err)
	}

	result, err := record.Run(ctx, record.Dependencies{DB: deps.DB, CAS: deps.CAS, Adapters: deps.Adapters}, record.Options{
		Command:    append([]string(nil), options.Command...),
		WorkingDir: branched.Destination,
		// A branch workspace contains only files that were part of the selected
		// evidence state. Do not apply today's project ignore configuration to
		// the initial branch snapshot: doing so could silently drop historical
		// evidence before the exact-root check.
		Ignore:              nil,
		TerminalInput:       options.TerminalInput,
		PTY:                 options.PTY,
		InitialTerminalSize: options.InitialTerminalSize,
		TerminalResize:      options.TerminalResize,
		Stdin:               options.Stdin,
		Stdout:              options.Stdout,
		Stderr:              options.Stderr,
		Env:                 append([]string(nil), options.Env...),
		ParentSessionID:     branched.Source.SessionID,
		ForkEventSeq:        branched.Source.EventSeq,
		ForkStateID:         branched.Source.ID,
		ExpectedInitialRoot: branched.Source.RootTreeID,
	})
	if err != nil {
		return RerunResult{Branch: branched, Mode: mode, Run: result}, fmt.Errorf("execute branched rerun: %w", err)
	}
	return RerunResult{Branch: branched, Mode: mode, Run: result}, nil
}

func validateDependencies(deps Dependencies) error {
	if deps.DB == nil {
		return fmt.Errorf("branch database is required")
	}
	if deps.CAS == nil {
		return fmt.Errorf("branch CAS is required")
	}
	return nil
}
