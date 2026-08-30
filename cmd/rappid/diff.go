package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"

	"github.com/rappidAI-Research/rappid-replay/internal/app"
	replaydiff "github.com/rappidAI-Research/rappid-replay/internal/diff"
	"github.com/rappidAI-Research/rappid-replay/internal/id"
)

const textDiffChangeLimit = 50

func runDiff(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("rappid replay diff", flag.ContinueOnError)
	flags.SetOutput(stderr)
	dataDir := flags.String("data-dir", "", "Replay local data directory")
	jsonOutput := flags.Bool("json", false, "emit the complete comparison result as JSON")
	maxStateChanges := flags.Int("max-state-changes", 10000, "maximum path-level state changes retained in the result")
	flags.Usage = func() { printDiffUsage(stderr) }
	if err := flags.Parse(args); err != nil {
		return 2
	}
	if flags.NArg() != 2 {
		printDiffUsage(stderr)
		return 2
	}
	leftID, err := id.ParseSession(flags.Arg(0))
	if err != nil {
		fmt.Fprintf(stderr, "rappid: invalid left session id: %v\n", err)
		return 2
	}
	rightID, err := id.ParseSession(flags.Arg(1))
	if err != nil {
		fmt.Fprintf(stderr, "rappid: invalid right session id: %v\n", err)
		return 2
	}
	if *maxStateChanges < 0 {
		fmt.Fprintln(stderr, "rappid: --max-state-changes cannot be negative")
		return 2
	}

	layout, err := app.ResolveLayout(*dataDir)
	if err != nil {
		fmt.Fprintf(stderr, "rappid: resolve local runtime: %v\n", err)
		return 1
	}
	runtime, err := app.OpenRuntime(ctx, layout.Root)
	if err != nil {
		fmt.Fprintf(stderr, "rappid: open local runtime: %v\n", err)
		return 1
	}
	defer closeRuntime(runtime, stderr)

	result, err := replaydiff.CompareSessions(ctx, replaydiff.Dependencies{DB: runtime.DB, CAS: runtime.CAS}, leftID, rightID, replaydiff.Options{
		MaxStateChanges: *maxStateChanges,
	})
	if err != nil {
		fmt.Fprintf(stderr, "rappid: diff failed: %v\n", err)
		return 1
	}
	if *jsonOutput {
		encoder := json.NewEncoder(stdout)
		encoder.SetEscapeHTML(false)
		if err := encoder.Encode(result); err != nil {
			fmt.Fprintf(stderr, "rappid: encode JSON result: %v\n", err)
			return 1
		}
		return 0
	}
	printDiffResult(stdout, result)
	return 0
}

func printDiffResult(w io.Writer, result replaydiff.Result) {
	fmt.Fprintf(w, "compare %s -> %s\n", result.Left.SessionID, result.Right.SessionID)
	if result.Lineage.Related {
		fmt.Fprintf(w, "lineage: common session %s", result.Lineage.CommonSessionID)
		if result.Lineage.SharedThroughEventSeq != 0 {
			fmt.Fprintf(w, " through event %d", result.Lineage.SharedThroughEventSeq)
		}
		fmt.Fprintln(w)
	} else {
		fmt.Fprintln(w, "lineage: unrelated sessions")
	}

	if !result.State.Comparable {
		fmt.Fprintf(w, "state: not comparable (%s)\n", result.State.Reason)
	} else if result.State.Equal {
		fmt.Fprintln(w, "state: equal")
	} else {
		fmt.Fprintf(w, "state: %d changes (+%d -%d ~%d type:%d)\n",
			result.State.TotalChanges, result.State.Added, result.State.Removed,
			result.State.Modified, result.State.TypeChanged,
		)
		limit := len(result.State.Changes)
		if limit > textDiffChangeLimit {
			limit = textDiffChangeLimit
		}
		for _, change := range result.State.Changes[:limit] {
			if change.Reason == "" {
				fmt.Fprintf(w, "  %s %s\n", change.Change, change.DisplayPath)
			} else {
				fmt.Fprintf(w, "  %s %s (%s)\n", change.Change, change.DisplayPath, change.Reason)
			}
		}
		if result.State.TotalChanges > limit {
			fmt.Fprintf(w, "  ... %d additional changes not shown in text output\n", result.State.TotalChanges-limit)
		}
		if result.State.ChangesTruncated {
			fmt.Fprintf(w, "  result retention truncated at %d path changes; increase --max-state-changes for JSON detail\n", len(result.State.Changes))
		}
	}

	fmt.Fprintf(w, "timeline: common prefix %d events; left=%d right=%d equal=%t\n",
		result.Timeline.CommonPrefixEvents, result.Timeline.LeftEvents, result.Timeline.RightEvents, result.Timeline.Equal)
	printFirstDivergence(w, "timeline divergence", result.Timeline.FirstLeft, result.Timeline.FirstRight)
	fmt.Fprintf(w, "process: common prefix %d/%d:%d equal=%t\n",
		result.Process.CommonPrefixEvents, result.Process.LeftEvents, result.Process.RightEvents, result.Process.Equal)
	printFirstDivergence(w, "process divergence", result.Process.FirstLeft, result.Process.FirstRight)
	fmt.Fprintf(w, "agent: common prefix %d/%d:%d equal=%t\n",
		result.Agent.CommonPrefixEvents, result.Agent.LeftEvents, result.Agent.RightEvents, result.Agent.Equal)
	printFirstDivergence(w, "agent divergence", result.Agent.FirstLeft, result.Agent.FirstRight)

	fmt.Fprintf(w, "outcome: left=%s", formatOutcome(result.Outcome.Left))
	fmt.Fprintf(w, " right=%s equal=%t\n", formatOutcome(result.Outcome.Right), result.Outcome.Equal)
	fmt.Fprintf(w, "technically identical: %t\n", result.Identical)
}

func printFirstDivergence(w io.Writer, label string, left, right *replaydiff.EventSummary) {
	if left == nil && right == nil {
		return
	}
	fmt.Fprintf(w, "%s:", label)
	if left != nil {
		fmt.Fprintf(w, " left #%d %s", left.Seq, left.Type)
	} else {
		fmt.Fprint(w, " left <end>")
	}
	if right != nil {
		fmt.Fprintf(w, " | right #%d %s", right.Seq, right.Type)
	} else {
		fmt.Fprint(w, " | right <end>")
	}
	fmt.Fprintln(w)
}

func formatOutcome(side replaydiff.OutcomeSide) string {
	text := side.Status
	if side.ExitCode != nil {
		text += fmt.Sprintf("/exit=%d", *side.ExitCode)
	}
	return text
}

func printDiffUsage(w io.Writer) {
	fmt.Fprintln(w, "Usage: rappid replay diff [--data-dir DIR] [--json] [--max-state-changes N] <left-session-id> <right-session-id>")
}
