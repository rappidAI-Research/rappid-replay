package record

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/rappidAI-Research/rappid-replay/internal/id"
	"github.com/rappidAI-Research/rappid-replay/internal/persistence"
	"github.com/rappidAI-Research/rappid-replay/internal/state"
	"github.com/rappidAI-Research/rappid-replay/internal/store"
)

const checkpointDebounce = 100 * time.Millisecond

type checkpointPosition struct {
	StateID    id.StateID
	RootTreeID store.ObjectID
}

type checkpointLoopResult struct {
	Position checkpointPosition
	Err      error
}

type checkpointLoop struct {
	stop chan struct{}
	done chan checkpointLoopResult
	once sync.Once
}

func startCheckpointLoop(
	ctx context.Context,
	deps Dependencies,
	sink *eventSink,
	snapshotter state.Snapshotter,
	root string,
	sessionID id.SessionID,
	position checkpointPosition,
	source workspaceChangeSource,
	cancelCommand context.CancelFunc,
) *checkpointLoop {
	loop := &checkpointLoop{
		stop: make(chan struct{}),
		done: make(chan checkpointLoopResult, 1),
	}
	go func() {
		loop.done <- runCheckpointLoop(
			ctx, deps, sink, snapshotter, root, sessionID, position, source,
			loop.stop, cancelCommand,
		)
	}()
	return loop
}

func (l *checkpointLoop) Stop() checkpointLoopResult {
	l.once.Do(func() { close(l.stop) })
	return <-l.done
}

func runCheckpointLoop(
	ctx context.Context,
	deps Dependencies,
	sink *eventSink,
	snapshotter state.Snapshotter,
	root string,
	sessionID id.SessionID,
	position checkpointPosition,
	source workspaceChangeSource,
	stop <-chan struct{},
	cancelCommand context.CancelFunc,
) checkpointLoopResult {
	changes := source.Changes()
	watcherErrors := source.Errors()
	var timer *time.Timer
	var timerC <-chan time.Time

	stopTimer := func() {
		if timer == nil {
			return
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timerC = nil
	}
	defer stopTimer()

	schedule := func() {
		if timer == nil {
			timer = time.NewTimer(checkpointDebounce)
			timerC = timer.C
			return
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(checkpointDebounce)
		timerC = timer.C
	}

	fatal := func(err error) checkpointLoopResult {
		if cancelCommand != nil {
			cancelCommand()
		}
		return checkpointLoopResult{Position: position, Err: err}
	}

	for {
		select {
		case <-stop:
			return checkpointLoopResult{Position: position}
		case <-ctx.Done():
			return checkpointLoopResult{Position: position}
		case _, ok := <-changes:
			if !ok {
				changes = nil
				continue
			}
			schedule()
		case watcherErr, ok := <-watcherErrors:
			if !ok {
				watcherErrors = nil
				continue
			}
			if watcherErr == nil {
				continue
			}
			if err := sink.append("fs.watcher.error", struct {
				Error string `json:"error"`
			}{Error: watcherErr.Error()}); err != nil {
				return fatal(err)
			}
			// A backend error or queue overflow means the event stream cannot be
			// trusted for completeness. Trigger a full reconciliation immediately.
			schedule()
		case <-timerC:
			timerC = nil
			next, err := reconcileWorkspace(
				ctx, deps, sink, snapshotter, root, sessionID, position, "watcher",
			)
			if err != nil {
				if ctx.Err() != nil {
					return checkpointLoopResult{Position: position}
				}
				return fatal(err)
			}
			position = next
		}
	}
}

func reconcileWorkspace(
	ctx context.Context,
	deps Dependencies,
	sink *eventSink,
	snapshotter state.Snapshotter,
	root string,
	sessionID id.SessionID,
	position checkpointPosition,
	trigger string,
) (checkpointPosition, error) {
	snapshot, err := captureWithRetry(ctx, snapshotter, root)
	if err != nil {
		if ctx.Err() != nil {
			return position, ctx.Err()
		}
		// Intermediate reconciliation is opportunistic. A transiently unstable
		// workspace is not enough to kill the child process; the final snapshot
		// remains authoritative. Persist the loss of intermediate fidelity.
		if appendErr := sink.append("fs.reconcile.failed", struct {
			Trigger string `json:"trigger"`
			Error   string `json:"error"`
		}{Trigger: trigger, Error: err.Error()}); appendErr != nil {
			return position, appendErr
		}
		return position, nil
	}
	if snapshot.RootTreeID == position.RootTreeID {
		return position, nil
	}

	stateID, err := id.NewState()
	if err != nil {
		return position, err
	}
	if _, err := sink.publishSnapshot(ctx, deps.CAS, persistence.PublishSnapshotRequest{
		StateID:     stateID,
		SessionID:   sessionID,
		RootTreeID:  snapshot.RootTreeID,
		Role:        persistence.SnapshotCheckpoint,
		StateBefore: position.StateID,
		Source:      recorderSource,
	}); err != nil {
		return position, fmt.Errorf("publish reconciled workspace checkpoint: %w", err)
	}
	return checkpointPosition{StateID: stateID, RootTreeID: snapshot.RootTreeID}, nil
}
