package diff

import (
	"context"
	"fmt"

	"github.com/rappidAI-Research/rappid-replay/internal/id"
	"github.com/rappidAI-Research/rappid-replay/internal/persistence"
	"github.com/rappidAI-Research/rappid-replay/internal/replay"
)

const maxLineageDepth = 4096

// CompareSessions performs a read-only deterministic comparison. It never
// executes recorded code, agents, tools, or shell commands. CAS verification is
// completed before final-state tree comparison so corrupt evidence cannot be
// presented as a legitimate difference.
func CompareSessions(ctx context.Context, deps Dependencies, leftID, rightID id.SessionID, options Options) (Result, error) {
	if ctx == nil {
		return Result{}, fmt.Errorf("context is required")
	}
	if deps.DB == nil {
		return Result{}, fmt.Errorf("diff database is required")
	}
	if deps.CAS == nil {
		return Result{}, fmt.Errorf("diff CAS is required")
	}
	if _, err := id.ParseSession(leftID.String()); err != nil {
		return Result{}, fmt.Errorf("invalid left session id: %w", err)
	}
	if _, err := id.ParseSession(rightID.String()); err != nil {
		return Result{}, fmt.Errorf("invalid right session id: %w", err)
	}
	if options.MaxStateChanges < 0 {
		return Result{}, fmt.Errorf("max state changes cannot be negative")
	}
	if options.MaxStateChanges == 0 {
		options.MaxStateChanges = defaultMaxStateChanges
	}

	leftSession, err := deps.DB.GetSession(ctx, leftID)
	if err != nil {
		return Result{}, fmt.Errorf("load left session: %w", err)
	}
	rightSession, err := deps.DB.GetSession(ctx, rightID)
	if err != nil {
		return Result{}, fmt.Errorf("load right session: %w", err)
	}

	lineage, err := compareLineage(ctx, deps.DB, leftSession, rightSession)
	if err != nil {
		return Result{}, err
	}

	leftEvents, err := deps.DB.ListEvents(ctx, leftID)
	if err != nil {
		return Result{}, fmt.Errorf("load left timeline: %w", err)
	}
	rightEvents, err := deps.DB.ListEvents(ctx, rightID)
	if err != nil {
		return Result{}, fmt.Errorf("load right timeline: %w", err)
	}
	leftNormalized, err := normalizeEvents(ctx, deps.DB, leftEvents)
	if err != nil {
		return Result{}, fmt.Errorf("normalize left timeline: %w", err)
	}
	rightNormalized, err := normalizeEvents(ctx, deps.DB, rightEvents)
	if err != nil {
		return Result{}, fmt.Errorf("normalize right timeline: %w", err)
	}

	stateResult, leftFinalRoot, rightFinalRoot, err := compareFinalStates(ctx, deps, leftSession, rightSession, options.MaxStateChanges)
	if err != nil {
		return Result{}, err
	}
	timeline := compareTimeline(leftNormalized, rightNormalized)
	process := compareStream(leftNormalized, rightNormalized, "process.")
	agent := compareStream(leftNormalized, rightNormalized, "agent.")
	leftOutcome := outcomeFromEvents(leftSession, leftFinalRoot, leftEvents)
	rightOutcome := outcomeFromEvents(rightSession, rightFinalRoot, rightEvents)
	outcome := OutcomeDiff{Left: leftOutcome, Right: rightOutcome, Equal: equalOutcome(leftOutcome, rightOutcome)}

	result := Result{
		Left:     summarizeSession(leftSession),
		Right:    summarizeSession(rightSession),
		Lineage:  lineage,
		State:    stateResult,
		Timeline: timeline,
		Process:  process,
		Agent:    agent,
		Outcome:  outcome,
	}
	result.Identical = stateResult.Comparable && stateResult.Equal && timeline.Equal && process.Equal && agent.Equal && outcome.Equal
	return result, nil
}

func compareFinalStates(
	ctx context.Context,
	deps Dependencies,
	leftSession, rightSession persistence.SessionRecord,
	maxChanges int,
) (StateDiff, string, string, error) {
	leftMissing := leftSession.FinalStateID == ""
	rightMissing := rightSession.FinalStateID == ""
	if leftMissing || rightMissing {
		reason := "final_state_unavailable"
		switch {
		case leftMissing && !rightMissing:
			reason = "left_final_state_unavailable"
		case !leftMissing && rightMissing:
			reason = "right_final_state_unavailable"
		}
		return StateDiff{
			Comparable:   false,
			Reason:       reason,
			LeftStateID:  leftSession.FinalStateID.String(),
			RightStateID: rightSession.FinalStateID.String(),
		}, "", "", nil
	}

	leftVerified, err := replay.VerifyState(ctx, replay.Dependencies{DB: deps.DB, CAS: deps.CAS}, leftSession.FinalStateID)
	if err != nil {
		return StateDiff{}, "", "", fmt.Errorf("verify left final state: %w", err)
	}
	rightVerified, err := replay.VerifyState(ctx, replay.Dependencies{DB: deps.DB, CAS: deps.CAS}, rightSession.FinalStateID)
	if err != nil {
		return StateDiff{}, "", "", fmt.Errorf("verify right final state: %w", err)
	}

	stateResult, err := diffTrees(deps.CAS, leftVerified.State.RootTreeID, rightVerified.State.RootTreeID, maxChanges)
	if err != nil {
		return StateDiff{}, "", "", fmt.Errorf("compare final workspace states: %w", err)
	}
	stateResult.LeftStateID = leftVerified.State.ID.String()
	stateResult.RightStateID = rightVerified.State.ID.String()
	return stateResult, leftVerified.State.RootTreeID.String(), rightVerified.State.RootTreeID.String(), nil
}

func summarizeSession(record persistence.SessionRecord) SessionSummary {
	return SessionSummary{
		SessionID:            record.ID.String(),
		ParentSessionID:      record.ParentSessionID.String(),
		ForkEventSeq:         record.ForkEventSeq,
		Status:               record.Status,
		InitialStateID:       record.InitialStateID.String(),
		FinalStateID:         record.FinalStateID.String(),
		ReproducibilityLevel: record.ReproducibilityLevel,
		AdapterID:            record.AdapterID,
		AdapterVersion:       record.AdapterVersion,
	}
}

func equalOutcome(left, right OutcomeSide) bool {
	if left.Status != right.Status || left.FinalRootID != right.FinalRootID {
		return false
	}
	if !equalIntPointer(left.ExitCode, right.ExitCode) {
		return false
	}
	return equalBoolPointer(left.Success, right.Success)
}

func equalIntPointer(left, right *int) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
}

func equalBoolPointer(left, right *bool) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
}

func compareLineage(ctx context.Context, db *persistence.DB, left, right persistence.SessionRecord) (LineageDiff, error) {
	leftChain, err := loadLineage(ctx, db, left)
	if err != nil {
		return LineageDiff{}, fmt.Errorf("load left lineage: %w", err)
	}
	rightChain, err := loadLineage(ctx, db, right)
	if err != nil {
		return LineageDiff{}, fmt.Errorf("load right lineage: %w", err)
	}

	leftIndex := make(map[string]int, len(leftChain))
	for i, record := range leftChain {
		leftIndex[record.ID.String()] = i
	}
	for rightDepth, record := range rightChain {
		leftDepth, ok := leftIndex[record.ID.String()]
		if !ok {
			continue
		}
		leftFork := forkFromCommon(leftChain, leftDepth)
		rightFork := forkFromCommon(rightChain, rightDepth)
		return LineageDiff{
			Related:               true,
			CommonSessionID:       record.ID.String(),
			LeftDepth:             leftDepth,
			RightDepth:            rightDepth,
			LeftForkEventSeq:      leftFork,
			RightForkEventSeq:     rightFork,
			SharedThroughEventSeq: sharedThrough(leftFork, rightFork),
		}, nil
	}
	return LineageDiff{Related: false}, nil
}

func loadLineage(ctx context.Context, db *persistence.DB, start persistence.SessionRecord) ([]persistence.SessionRecord, error) {
	chain := make([]persistence.SessionRecord, 0, 8)
	seen := make(map[string]struct{})
	current := start
	for depth := 0; depth < maxLineageDepth; depth++ {
		key := current.ID.String()
		if _, duplicate := seen[key]; duplicate {
			return nil, fmt.Errorf("session lineage contains a cycle at %s", current.ID)
		}
		seen[key] = struct{}{}
		chain = append(chain, current)
		if current.ParentSessionID == "" {
			return chain, nil
		}
		parent, err := db.GetSession(ctx, current.ParentSessionID)
		if err != nil {
			return nil, fmt.Errorf("load parent %s: %w", current.ParentSessionID, err)
		}
		current = parent
	}
	return nil, fmt.Errorf("session lineage exceeds %d ancestors", maxLineageDepth)
}

func forkFromCommon(chain []persistence.SessionRecord, commonDepth int) uint64 {
	if commonDepth <= 0 {
		return 0
	}
	return chain[commonDepth-1].ForkEventSeq
}

func sharedThrough(leftFork, rightFork uint64) uint64 {
	switch {
	case leftFork == 0:
		return rightFork
	case rightFork == 0:
		return leftFork
	case leftFork < rightFork:
		return leftFork
	default:
		return rightFork
	}
}
