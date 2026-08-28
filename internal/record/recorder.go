// Package record implements Replay's provider-independent command recorder.
package record

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/rappidAI-Research/rappid-replay/internal/id"
	"github.com/rappidAI-Research/rappid-replay/internal/ignore"
	"github.com/rappidAI-Research/rappid-replay/internal/persistence"
	"github.com/rappidAI-Research/rappid-replay/internal/state"
	"github.com/rappidAI-Research/rappid-replay/internal/store"
)

const snapshotAttempts = 3

// Dependencies are the already-open durable stores used by one recorder.
type Dependencies struct {
	DB  *persistence.DB
	CAS *store.LocalStore
}

// Options describes a generic child command recording. TerminalInput records
// the configured privacy policy; full byte-level stdin capture is deliberately
// rejected until the PTY recorder lands rather than silently providing weaker
// evidence than requested.
type Options struct {
	Command       []string
	WorkingDir    string
	Ignore        []string
	TerminalInput string
	Stdin         io.Reader
	Stdout        io.Writer
	Stderr        io.Writer
	Env           []string
}

// Result identifies the durable session and its state boundary after recording.
type Result struct {
	SessionID      id.SessionID
	InitialStateID id.StateID
	FinalStateID   id.StateID
	ExitCode       int
	StartedAt      time.Time
	EndedAt        time.Time
}

// Run records one command using the Generic Recorder baseline. This first Track
// B execution path captures the session/process lifecycle, stdout/stderr bytes,
// and exact initial/final workspace states. PTY semantics, filesystem watcher
// checkpoints, process-tree discovery, and richer environment capture are added
// by subsequent recorder slices without changing these persistence contracts.
func Run(ctx context.Context, deps Dependencies, options Options) (Result, error) {
	if ctx == nil {
		return Result{}, fmt.Errorf("context is required")
	}
	if deps.DB == nil {
		return Result{}, fmt.Errorf("recorder database is required")
	}
	if deps.CAS == nil {
		return Result{}, fmt.Errorf("recorder CAS is required")
	}
	if len(options.Command) == 0 || options.Command[0] == "" {
		return Result{}, fmt.Errorf("record command is required")
	}
	if options.TerminalInput == "" {
		options.TerminalInput = "metadata-only"
	}
	if options.TerminalInput == "full" {
		return Result{}, fmt.Errorf("full terminal input capture requires the PTY recorder and is not available in the generic pipe recorder")
	}
	if options.TerminalInput != "metadata-only" && options.TerminalInput != "off" {
		return Result{}, fmt.Errorf("unsupported terminal input policy %q", options.TerminalInput)
	}

	workingDir := options.WorkingDir
	if workingDir == "" {
		var err error
		workingDir, err = os.Getwd()
		if err != nil {
			return Result{}, fmt.Errorf("resolve recorder working directory: %w", err)
		}
	}
	absWorkingDir, err := filepath.Abs(workingDir)
	if err != nil {
		return Result{}, fmt.Errorf("resolve recorder working directory %q: %w", workingDir, err)
	}
	info, err := os.Stat(absWorkingDir)
	if err != nil {
		return Result{}, fmt.Errorf("stat recorder working directory: %w", err)
	}
	if !info.IsDir() {
		return Result{}, fmt.Errorf("recorder working directory %q is not a directory", absWorkingDir)
	}

	policy, err := ignore.New(options.Ignore)
	if err != nil {
		return Result{}, fmt.Errorf("compile recorder ignore policy: %w", err)
	}

	sessionID, err := id.NewSession()
	if err != nil {
		return Result{}, err
	}
	started := time.Now()
	clock := newRunClock(started)
	result := Result{SessionID: sessionID, StartedAt: started.UTC()}
	if err := deps.DB.CreateSession(ctx, persistence.SessionStart{
		ID:                   sessionID,
		Command:              append([]string(nil), options.Command...),
		CWD:                  absWorkingDir,
		StartedAt:            started,
		ReproducibilityLevel: "R0",
		AdapterID:            "generic",
	}); err != nil {
		return result, err
	}

	sink := newEventSink(ctx, deps.DB, sessionID.String(), clock)
	if err := sink.append("session.started", struct {
		Command       []string `json:"command"`
		CWD           string   `json:"cwd"`
		Recorder      string   `json:"recorder"`
		PTY           bool     `json:"pty"`
		TerminalInput string   `json:"terminal_input"`
		StdinAttached bool     `json:"stdin_attached"`
	}{
		Command:       append([]string(nil), options.Command...),
		CWD:           absWorkingDir,
		Recorder:      "generic",
		PTY:           false,
		TerminalInput: options.TerminalInput,
		StdinAttached: options.Stdin != nil,
	}); err != nil {
		return result, abortWithError(context.WithoutCancel(ctx), deps.DB, sink, clock, sessionID, "", err)
	}

	snapshotter := state.Snapshotter{CAS: deps.CAS, Exclude: policy.Exclude}
	initialSnapshot, err := captureWithRetry(ctx, snapshotter, absWorkingDir)
	if err != nil {
		return result, abortWithError(context.WithoutCancel(ctx), deps.DB, sink, clock, sessionID, "", fmt.Errorf("capture initial workspace state: %w", err))
	}
	initialStateID, err := id.NewState()
	if err != nil {
		return result, abortWithError(context.WithoutCancel(ctx), deps.DB, sink, clock, sessionID, "", err)
	}
	wall, monotonic := clock.sample()
	if _, err := deps.DB.PublishSnapshot(ctx, deps.CAS, persistence.PublishSnapshotRequest{
		StateID:     initialStateID,
		SessionID:   sessionID,
		RootTreeID:  initialSnapshot.RootTreeID,
		Role:        persistence.SnapshotInitial,
		WallTimeUTC: wall,
		MonotonicNS: monotonic,
		Source:      recorderSource,
	}); err != nil {
		return result, abortWithError(context.WithoutCancel(ctx), deps.DB, sink, clock, sessionID, "", fmt.Errorf("publish initial workspace state: %w", err))
	}
	result.InitialStateID = initialStateID
	lastState := initialStateID

	command := exec.CommandContext(ctx, options.Command[0], options.Command[1:]...)
	command.Dir = absWorkingDir
	command.Stdin = options.Stdin
	if options.Env != nil {
		command.Env = append([]string(nil), options.Env...)
	}

	streamGate := make(chan struct{})
	command.Stdout = gatedWriter{ready: streamGate, writer: streamEventWriter{sink: sink, stream: "stdout", output: options.Stdout}}
	command.Stderr = gatedWriter{ready: streamGate, writer: streamEventWriter{sink: sink, stream: "stderr", output: options.Stderr}}

	if err := command.Start(); err != nil {
		close(streamGate)
		return result, abortWithError(context.WithoutCancel(ctx), deps.DB, sink, clock, sessionID, lastState, fmt.Errorf("start recorded command: %w", err))
	}
	pid := command.Process.Pid
	startEventErr := sink.append("process.started", struct {
		PID     int      `json:"pid"`
		Path    string   `json:"path"`
		Command []string `json:"command"`
		CWD     string   `json:"cwd"`
	}{PID: pid, Path: command.Path, Command: append([]string(nil), options.Command...), CWD: absWorkingDir})
	close(streamGate)

	waitErr := command.Wait()
	exitCode := 0
	processSuccess := waitErr == nil
	if waitErr != nil {
		var exitErr *exec.ExitError
		if errors.As(waitErr, &exitErr) {
			exitCode = exitErr.ExitCode()
		} else {
			return result, abortWithError(context.WithoutCancel(ctx), deps.DB, sink, clock, sessionID, lastState, fmt.Errorf("wait for recorded command: %w", waitErr))
		}
	}
	result.ExitCode = exitCode

	if err := sink.append("process.exited", struct {
		PID      int  `json:"pid"`
		ExitCode int  `json:"exit_code"`
		Success  bool `json:"success"`
	}{PID: pid, ExitCode: exitCode, Success: processSuccess}); err != nil && startEventErr == nil {
		startEventErr = err
	}
	if err := sink.err(); err != nil && startEventErr == nil {
		startEventErr = err
	}

	cleanupCtx := context.WithoutCancel(ctx)
	finalSnapshot, snapshotErr := captureWithRetry(cleanupCtx, snapshotter, absWorkingDir)
	if snapshotErr == nil {
		finalStateID, idErr := id.NewState()
		if idErr != nil {
			snapshotErr = idErr
		} else {
			wall, monotonic = clock.sample()
			if _, publishErr := deps.DB.PublishSnapshot(cleanupCtx, deps.CAS, persistence.PublishSnapshotRequest{
				StateID:     finalStateID,
				SessionID:   sessionID,
				RootTreeID:  finalSnapshot.RootTreeID,
				Role:        persistence.SnapshotFinal,
				WallTimeUTC: wall,
				MonotonicNS: monotonic,
				StateBefore: lastState,
				Source:      recorderSource,
			}); publishErr != nil {
				snapshotErr = fmt.Errorf("publish final workspace state: %w", publishErr)
			} else {
				lastState = finalStateID
				result.FinalStateID = finalStateID
			}
		}
	}

	if ctx.Err() != nil {
		return result, abortWithError(cleanupCtx, deps.DB, sink, clock, sessionID, lastState, fmt.Errorf("recorded command context ended: %w", ctx.Err()))
	}
	if startEventErr != nil {
		return result, abortWithError(cleanupCtx, deps.DB, sink, clock, sessionID, lastState, startEventErr)
	}
	if snapshotErr != nil {
		return result, abortWithError(cleanupCtx, deps.DB, sink, clock, sessionID, lastState, fmt.Errorf("capture final workspace state: %w", snapshotErr))
	}

	ended, endMonotonic := clock.sample()
	result.EndedAt = ended
	if _, err := deps.DB.EndSession(cleanupCtx, persistence.SessionEnd{
		SessionID:   sessionID,
		Status:      persistence.SessionCompleted,
		EndedAt:     ended,
		MonotonicNS: endMonotonic,
		StateID:     result.FinalStateID,
		Source:      recorderSource,
		ExitCode:    &result.ExitCode,
	}); err != nil {
		return result, fmt.Errorf("complete recording session: %w", err)
	}
	return result, nil
}

type gatedWriter struct {
	ready  <-chan struct{}
	writer io.Writer
}

func (w gatedWriter) Write(p []byte) (int, error) {
	<-w.ready
	return w.writer.Write(p)
}

func captureWithRetry(ctx context.Context, snapshotter state.Snapshotter, root string) (state.Snapshot, error) {
	var lastErr error
	for attempt := 0; attempt < snapshotAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return state.Snapshot{}, err
		}
		snapshot, err := snapshotter.Capture(root)
		if err == nil {
			return snapshot, nil
		}
		lastErr = err
		if !errors.Is(err, state.ErrWorkspaceChanged) {
			return state.Snapshot{}, err
		}
		if attempt+1 < snapshotAttempts {
			select {
			case <-ctx.Done():
				return state.Snapshot{}, ctx.Err()
			case <-time.After(10 * time.Millisecond):
			}
		}
	}
	return state.Snapshot{}, lastErr
}

func abortWithError(ctx context.Context, db *persistence.DB, sink *eventSink, clock *runClock, sessionID id.SessionID, stateID id.StateID, cause error) error {
	ended, monotonic := clock.sample()
	reason := cause.Error()
	_, endErr := db.EndSession(ctx, persistence.SessionEnd{
		SessionID:   sessionID,
		Status:      persistence.SessionAborted,
		EndedAt:     ended,
		MonotonicNS: monotonic,
		StateID:     stateID,
		Source:      recorderSource,
		Reason:      reason,
	})
	if endErr != nil {
		return errors.Join(cause, fmt.Errorf("abort recording session: %w", endErr))
	}
	return cause
}
