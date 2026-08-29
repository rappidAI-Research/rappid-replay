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
	"github.com/rappidAI-Research/rappid-replay/internal/privacy"
	"github.com/rappidAI-Research/rappid-replay/internal/state"
	"github.com/rappidAI-Research/rappid-replay/internal/store"
	"github.com/rappidAI-Research/rappid-replay/internal/terminal"
	"github.com/rappidAI-Research/rappid-replay/pkg/adapter"
)

const snapshotAttempts = 3

// Dependencies are the already-open durable stores used by one recorder.
type Dependencies struct {
	DB       *persistence.DB
	CAS      *store.LocalStore
	Adapters *adapter.Registry
}

// Options describes a generic child command recording. PTY enables a real
// pseudo-terminal rather than the non-interactive stdout/stderr pipe fallback.
// Full input capture is only legal with PTY because the pipe recorder cannot
// provide interactive terminal semantics.
type Options struct {
	Command             []string
	WorkingDir          string
	Ignore              []string
	TerminalInput       string
	PTY                 bool
	InitialTerminalSize terminal.Size
	TerminalResize      <-chan terminal.Size
	Stdin               io.Reader
	Stdout              io.Writer
	Stderr              io.Writer
	Env                 []string
}

// Result identifies the durable session and its state boundary after recording.
type Result struct {
	SessionID      id.SessionID
	InitialStateID id.StateID
	FinalStateID   id.StateID
	AdapterID      string
	AdapterVersion string
	ExitCode       int
	StartedAt      time.Time
	EndedAt        time.Time
}

// Run records one command using the Generic Recorder baseline. Filesystem
// notifications are only triggers: every checkpoint is reconstructed by a full
// canonical reconciliation scan before publication, and the final snapshot is
// captured after the child exits regardless of watcher activity.
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
	if options.TerminalInput == "full" && !options.PTY {
		return Result{}, fmt.Errorf("full terminal input capture requires the PTY recorder")
	}
	if options.TerminalInput != "metadata-only" && options.TerminalInput != "off" && options.TerminalInput != "full" {
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
	recordedCommand, commandRedacted := privacy.RedactCommandArgs(options.Command)
	adapterSelection, err := selectRunAdapter(ctx, deps, recordedCommand, absWorkingDir)
	if err != nil {
		return Result{}, err
	}

	sessionID, err := id.NewSession()
	if err != nil {
		return Result{}, err
	}
	started := time.Now()
	clock := newRunClock(started)
	result := Result{
		SessionID:      sessionID,
		AdapterID:      adapterSelection.Descriptor.ID,
		AdapterVersion: adapterSelection.Descriptor.Version,
		StartedAt:      started.UTC(),
	}
	if err := deps.DB.CreateSession(ctx, persistence.SessionStart{
		ID:                   sessionID,
		Command:              recordedCommand,
		CWD:                  absWorkingDir,
		StartedAt:            started,
		ReproducibilityLevel: "R0",
		AdapterID:            adapterSelection.Descriptor.ID,
		AdapterVersion:       adapterSelection.Descriptor.Version,
	}); err != nil {
		return result, err
	}

	sink := newEventSink(ctx, deps.DB, sessionID.String(), clock)
	if err := sink.appendTechnical("session.started", struct {
		Command            []string             `json:"command"`
		CWD                string               `json:"cwd"`
		Recorder           string               `json:"recorder"`
		Adapter            adapter.Descriptor   `json:"adapter"`
		AdapterDetection   adapter.Detection    `json:"adapter_detection"`
		AdapterDiagnostics []adapter.Diagnostic `json:"adapter_diagnostics,omitempty"`
		PTY                bool                 `json:"pty"`
		TerminalInput      string               `json:"terminal_input"`
		StdinAttached      bool                 `json:"stdin_attached"`
	}{
		Command:            recordedCommand,
		CWD:                absWorkingDir,
		Recorder:           "generic",
		Adapter:            adapterSelection.Descriptor,
		AdapterDetection:   adapterSelection.Detection,
		AdapterDiagnostics: adapterSelection.Diagnostics,
		PTY:                options.PTY,
		TerminalInput:      options.TerminalInput,
		StdinAttached:      options.Stdin != nil,
	}, commandRedacted); err != nil {
		return result, abortWithError(context.WithoutCancel(ctx), deps.DB, sink, clock, sessionID, "", err)
	}

	hooks := newAdapterHookBridge(adapterSelection, sessionID.String(), absWorkingDir, recordedCommand, sink)
	if err := hooks.loadRedactionHints(ctx); err != nil {
		return result, abortWithError(context.WithoutCancel(ctx), deps.DB, sink, clock, sessionID, "", err)
	}
	sink.setRedactionPolicy(hooks.redaction)

	snapshotter := state.Snapshotter{CAS: deps.CAS, Exclude: policy.Exclude}
	initialSnapshot, err := captureWithRetry(ctx, snapshotter, absWorkingDir)
	if err != nil {
		return result, abortWithError(context.WithoutCancel(ctx), deps.DB, sink, clock, sessionID, "", fmt.Errorf("capture initial workspace state: %w", err))
	}
	initialStateID, err := id.NewState()
	if err != nil {
		return result, abortWithError(context.WithoutCancel(ctx), deps.DB, sink, clock, sessionID, "", err)
	}
	if _, err := sink.publishSnapshot(ctx, deps.CAS, persistence.PublishSnapshotRequest{
		StateID:    initialStateID,
		SessionID:  sessionID,
		RootTreeID: initialSnapshot.RootTreeID,
		Role:       persistence.SnapshotInitial,
		Source:     recorderSource,
	}); err != nil {
		return result, abortWithError(context.WithoutCancel(ctx), deps.DB, sink, clock, sessionID, "", fmt.Errorf("publish initial workspace state: %w", err))
	}
	result.InitialStateID = initialStateID
	position := checkpointPosition{StateID: initialStateID, RootTreeID: initialSnapshot.RootTreeID}

	fingerprint, environment, git, err := captureExecutionEnvironmentWithRedaction(ctx, absWorkingDir, options.Env, hooks.redaction)
	if err != nil {
		return result, abortWithError(context.WithoutCancel(ctx), deps.DB, sink, clock, sessionID, position.StateID, err)
	}
	if err := deps.DB.StoreEnvironment(ctx, sessionID, fingerprint); err != nil {
		return result, abortWithError(context.WithoutCancel(ctx), deps.DB, sink, clock, sessionID, position.StateID, fmt.Errorf("persist execution environment: %w", err))
	}
	if err := sink.append("session.environment", environment); err != nil {
		return result, abortWithError(context.WithoutCancel(ctx), deps.DB, sink, clock, sessionID, position.StateID, err)
	}
	if err := sink.append("git.context", git); err != nil {
		return result, abortWithError(context.WithoutCancel(ctx), deps.DB, sink, clock, sessionID, position.StateID, err)
	}
	if err := hooks.emitEnvironment(ctx); err != nil {
		return result, abortWithError(context.WithoutCancel(ctx), deps.DB, sink, clock, sessionID, position.StateID, err)
	}

	var watcher workspaceChangeSource
	if created, watcherErr := newWorkspaceWatcher(absWorkingDir, policy); watcherErr != nil {
		if err := sink.append("fs.watcher.unavailable", struct {
			Error string `json:"error"`
		}{Error: watcherErr.Error()}); err != nil {
			return result, abortWithError(context.WithoutCancel(ctx), deps.DB, sink, clock, sessionID, position.StateID, err)
		}
	} else {
		watcher = created
		defer func() { _ = watcher.Close() }()
		position, err = reconcileWorkspace(
			ctx, deps, sink, snapshotter, absWorkingDir, sessionID, position, "watcher-start",
		)
		if err != nil {
			return result, abortWithError(context.WithoutCancel(ctx), deps.DB, sink, clock, sessionID, position.StateID, err)
		}
	}

	commandCtx, cancelCommand := context.WithCancel(ctx)
	defer cancelCommand()
	execution, err := startRecordedExecution(
		commandCtx, cancelCommand, sink, options, absWorkingDir, recordedCommand, commandRedacted,
	)
	if err != nil {
		return result, abortWithError(context.WithoutCancel(ctx), deps.DB, sink, clock, sessionID, position.StateID, err)
	}
	pid := execution.PID()
	if err := hooks.enrichProcess(commandCtx, adapter.ProcessObservation{
		PID:        pid,
		Executable: options.Command[0],
		Arguments:  append([]string(nil), recordedCommand...),
	}); err != nil {
		cancelCommand()
		_ = execution.Wait()
		_ = execution.Finalize()
		return result, abortWithError(context.WithoutCancel(ctx), deps.DB, sink, clock, sessionID, position.StateID, err)
	}
	hooks.startEventStream(commandCtx)

	processTree := startProcessTreeMonitor(commandCtx, sink, hooks, pid, cancelCommand)

	var checkpoints *checkpointLoop
	if watcher != nil {
		checkpoints = startCheckpointLoop(
			commandCtx, deps, sink, snapshotter, absWorkingDir, sessionID, position,
			watcher, cancelCommand,
		)
	}

	waitErr := execution.Wait()
	executionErr := execution.Finalize()
	adapterStreamErr := hooks.stopEventStream()

	processTreeResult := processTree.Stop()
	processTreeErr := processTreeResult.Err

	var checkpointErr error
	if checkpoints != nil {
		checkpointResult := checkpoints.Stop()
		position = checkpointResult.Position
		checkpointErr = checkpointResult.Err
	}
	if watcher != nil {
		if closeErr := watcher.Close(); closeErr != nil && checkpointErr == nil {
			if err := sink.append("fs.watcher.error", struct {
				Error string `json:"error"`
			}{Error: fmt.Sprintf("close watcher: %v", closeErr)}); err != nil {
				checkpointErr = err
			}
		}
	}

	exitCode := 0
	processSuccess := waitErr == nil
	if waitErr != nil {
		var exitErr *exec.ExitError
		if errors.As(waitErr, &exitErr) {
			exitCode = exitErr.ExitCode()
		} else if commandCtx.Err() != nil && (ctx.Err() != nil || executionErr != nil || adapterStreamErr != nil || processTreeErr != nil || checkpointErr != nil) {
			exitCode = -1
		} else {
			return result, abortWithError(context.WithoutCancel(ctx), deps.DB, sink, clock, sessionID, position.StateID, fmt.Errorf("wait for recorded command: %w", waitErr))
		}
	}
	result.ExitCode = exitCode

	var timelineErr error
	if err := sink.append("process.exited", struct {
		PID      int  `json:"pid"`
		ExitCode int  `json:"exit_code"`
		Success  bool `json:"success"`
	}{PID: pid, ExitCode: exitCode, Success: processSuccess}); err != nil {
		timelineErr = err
	}
	if err := sink.err(); err != nil && timelineErr == nil {
		timelineErr = err
	}

	cleanupCtx := context.WithoutCancel(ctx)
	if ctx.Err() != nil {
		return result, abortWithError(cleanupCtx, deps.DB, sink, clock, sessionID, position.StateID, fmt.Errorf("recorded command context ended: %w", ctx.Err()))
	}
	if timelineErr != nil {
		return result, abortWithError(cleanupCtx, deps.DB, sink, clock, sessionID, position.StateID, timelineErr)
	}
	if executionErr != nil {
		return result, abortWithError(cleanupCtx, deps.DB, sink, clock, sessionID, position.StateID, executionErr)
	}
	if adapterStreamErr != nil {
		return result, abortWithError(cleanupCtx, deps.DB, sink, clock, sessionID, position.StateID, adapterStreamErr)
	}
	if processTreeErr != nil {
		return result, abortWithError(cleanupCtx, deps.DB, sink, clock, sessionID, position.StateID, processTreeErr)
	}
	if checkpointErr != nil {
		return result, abortWithError(cleanupCtx, deps.DB, sink, clock, sessionID, position.StateID, checkpointErr)
	}

	finalSnapshot, snapshotErr := captureWithRetry(cleanupCtx, snapshotter, absWorkingDir)
	if snapshotErr == nil {
		finalStateID, idErr := id.NewState()
		if idErr != nil {
			snapshotErr = idErr
		} else if _, publishErr := sink.publishSnapshot(cleanupCtx, deps.CAS, persistence.PublishSnapshotRequest{
			StateID:     finalStateID,
			SessionID:   sessionID,
			RootTreeID:  finalSnapshot.RootTreeID,
			Role:        persistence.SnapshotFinal,
			StateBefore: position.StateID,
			Source:      recorderSource,
		}); publishErr != nil {
			snapshotErr = fmt.Errorf("publish final workspace state: %w", publishErr)
		} else {
			previous := position
			position = checkpointPosition{StateID: finalStateID, RootTreeID: finalSnapshot.RootTreeID}
			result.FinalStateID = finalStateID
			if artifactErr := persistArtifactDelta(cleanupCtx, deps, sink, sessionID, previous, position); artifactErr != nil {
				snapshotErr = fmt.Errorf("publish final workspace artifacts: %w", artifactErr)
			}
		}
	}
	if snapshotErr != nil {
		return result, abortWithError(cleanupCtx, deps.DB, sink, clock, sessionID, position.StateID, fmt.Errorf("finalize workspace state: %w", snapshotErr))
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
