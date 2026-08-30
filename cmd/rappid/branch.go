package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/rappidAI-Research/rappid-replay/internal/app"
	branchop "github.com/rappidAI-Research/rappid-replay/internal/branch"
	"github.com/rappidAI-Research/rappid-replay/internal/id"
	"github.com/rappidAI-Research/rappid-replay/internal/terminal"
)

func runBranch(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("rappid replay branch", flag.ContinueOnError)
	flags.SetOutput(stderr)
	dataDir := flags.String("data-dir", "", "Replay local data directory")
	destination := flags.String("to", "", "branch destination directory")
	force := flags.Bool("force", false, "replace an existing destination directory")
	jsonOutput := flags.Bool("json", false, "emit branch result as JSON")
	flags.Usage = func() { printBranchUsage(stderr) }
	if err := flags.Parse(args); err != nil {
		return 2
	}
	if flags.NArg() != 1 {
		printBranchUsage(stderr)
		return 2
	}
	stateID, err := id.ParseState(flags.Arg(0))
	if err != nil {
		fmt.Fprintf(stderr, "rappid: invalid state id: %v\n", err)
		return 2
	}
	target := *destination
	if target == "" {
		target = branchop.DefaultDestination(stateID)
	}
	layout, err := app.ResolveLayout(*dataDir)
	if err != nil {
		fmt.Fprintf(stderr, "rappid: resolve local runtime: %v\n", err)
		return 1
	}
	if err := app.ValidateMaterializationSeparation(target, layout.Root); err != nil {
		fmt.Fprintf(stderr, "rappid: unsafe branch destination: %v\n", err)
		return 1
	}
	runtime, err := app.OpenRuntime(ctx, layout.Root)
	if err != nil {
		fmt.Fprintf(stderr, "rappid: open local runtime: %v\n", err)
		return 1
	}
	defer closeRuntime(runtime, stderr)

	result, err := branchop.Create(ctx, branchop.Dependencies{DB: runtime.DB, CAS: runtime.CAS}, branchop.CreateOptions{
		StateID: stateID, Destination: target, Force: *force,
	})
	if err != nil {
		fmt.Fprintf(stderr, "rappid: branch failed: %v\n", err)
		return 1
	}
	if *jsonOutput {
		payload := struct {
			StateID         string `json:"state_id"`
			ParentSessionID string `json:"parent_session_id"`
			ForkEventSeq    uint64 `json:"fork_event_seq"`
			RootTreeID      string `json:"root_tree_id"`
			Destination     string `json:"destination"`
		}{
			StateID: result.Source.ID.String(), ParentSessionID: result.Source.SessionID.String(),
			ForkEventSeq: result.Source.EventSeq, RootTreeID: result.Source.RootTreeID.String(),
			Destination: result.Destination,
		}
		if err := json.NewEncoder(stdout).Encode(payload); err != nil {
			fmt.Fprintf(stderr, "rappid: encode JSON result: %v\n", err)
			return 1
		}
		return 0
	}
	fmt.Fprintf(stdout, "branched %s at event %d to %s\n", result.Source.ID, result.Source.EventSeq, result.Destination)
	return 0
}

func runRerun(ctx context.Context, args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("rappid replay rerun", flag.ContinueOnError)
	flags.SetOutput(stderr)
	dataDir := flags.String("data-dir", "", "Replay local data directory")
	destination := flags.String("to", "", "rerun branch destination directory")
	force := flags.Bool("force", false, "replace an existing destination directory before rerun")
	jsonOutput := flags.Bool("json", false, "emit rerun result as JSON on stdout")
	ptyMode := flags.String("pty", "auto", "terminal mode: auto, on, or off")
	terminalInput := flags.String("terminal-input", "metadata-only", "terminal input capture: metadata-only, full, or off")
	modeText := flags.String("mode", "live", "rerun mode: recorded, live, controlled, or hybrid")
	confirmExecution := flags.Bool("confirm-execution", false, "confirm execution of the branched command and its possible side effects")
	flags.Usage = func() { printRerunUsage(stderr) }
	if err := flags.Parse(args); err != nil {
		return 2
	}
	remaining := flags.Args()
	if len(remaining) < 3 || remaining[1] != "--" {
		fmt.Fprintln(stderr, "error: rerun requires <state-id> -- <command> [args...]")
		printRerunUsage(stderr)
		return 2
	}
	stateID, err := id.ParseState(remaining[0])
	if err != nil {
		fmt.Fprintf(stderr, "rappid: invalid state id: %v\n", err)
		return 2
	}
	command := remaining[2:]
	if len(command) == 0 || command[0] == "" {
		fmt.Fprintln(stderr, "error: rerun command is required after --")
		return 2
	}
	mode, err := branchop.ParseMode(*modeText)
	if err != nil {
		fmt.Fprintf(stderr, "rappid: %v\n", err)
		return 2
	}
	if *terminalInput != "metadata-only" && *terminalInput != "full" && *terminalInput != "off" {
		fmt.Fprintf(stderr, "rappid: invalid --terminal-input value %q\n", *terminalInput)
		return 2
	}

	target := *destination
	if target == "" {
		target = branchop.DefaultDestination(stateID)
	}
	layout, err := app.ResolveLayout(*dataDir)
	if err != nil {
		fmt.Fprintf(stderr, "rappid: resolve local runtime: %v\n", err)
		return 1
	}
	if err := app.ValidateMaterializationSeparation(target, layout.Root); err != nil {
		fmt.Fprintf(stderr, "rappid: unsafe rerun destination: %v\n", err)
		return 1
	}

	childStdout, childStderr := stdout, stderr
	if *jsonOutput {
		childStdout = stderr
		childStderr = stderr
	}
	usePTY, err := resolvePTYMode(*ptyMode, stdin, childStdout)
	if err != nil {
		fmt.Fprintf(stderr, "rappid: %v\n", err)
		return 2
	}
	if *terminalInput == "full" && !usePTY {
		fmt.Fprintln(stderr, "rappid: full terminal input capture requires --pty on or an auto-detected terminal")
		return 2
	}

	initialSize := terminal.Size{}
	var resize <-chan terminal.Size
	var stopResize func()
	if usePTY {
		initialSize = terminal.Size{Columns: 80, Rows: 24}
		if outputFile, ok := childStdout.(*os.File); ok && terminal.IsTerminal(outputFile) {
			if size, sizeErr := terminal.HostSize(outputFile); sizeErr == nil {
				initialSize = size
			}
			resize, stopResize = watchTerminalSize(ctx, outputFile, initialSize)
		}
	}
	if stopResize != nil {
		defer stopResize()
	}

	var restoreTerminal func() error
	if usePTY {
		if inputFile, ok := stdin.(*os.File); ok && terminal.IsTerminal(inputFile) {
			restoreTerminal, err = terminal.MakeRaw(inputFile)
			if err != nil {
				fmt.Fprintf(stderr, "rappid: enter terminal raw mode: %v\n", err)
				return 1
			}
		}
	}

	runtime, err := app.OpenRuntime(ctx, layout.Root)
	if err != nil {
		if restoreTerminal != nil {
			_ = restoreTerminal()
		}
		fmt.Fprintf(stderr, "rappid: open local runtime: %v\n", err)
		return 1
	}
	defer closeRuntime(runtime, stderr)

	result, rerunErr := branchop.Rerun(ctx, branchop.Dependencies{DB: runtime.DB, CAS: runtime.CAS}, branchop.RerunOptions{
		StateID:             stateID,
		Destination:         target,
		Force:               *force,
		Mode:                mode,
		ConfirmExecution:    *confirmExecution,
		Command:             command,
		TerminalInput:       *terminalInput,
		PTY:                 usePTY,
		InitialTerminalSize: initialSize,
		TerminalResize:      resize,
		Stdin:               stdin,
		Stdout:              childStdout,
		Stderr:              childStderr,
	})
	if restoreTerminal != nil {
		if restoreErr := restoreTerminal(); restoreErr != nil && rerunErr == nil {
			rerunErr = fmt.Errorf("restore terminal mode: %w", restoreErr)
		}
		restoreTerminal = nil
	}
	if rerunErr != nil {
		fmt.Fprintf(stderr, "rappid: rerun failed: %v\n", rerunErr)
		return 1
	}

	if *jsonOutput {
		payload := struct {
			Mode            string `json:"mode"`
			SourceStateID   string `json:"source_state_id"`
			ParentSessionID string `json:"parent_session_id"`
			ForkEventSeq    uint64 `json:"fork_event_seq"`
			Workspace       string `json:"workspace"`
			SessionID       string `json:"session_id"`
			InitialStateID  string `json:"initial_state_id"`
			FinalStateID    string `json:"final_state_id"`
			ExitCode        int    `json:"exit_code"`
		}{
			Mode: string(result.Mode), SourceStateID: result.Branch.Source.ID.String(),
			ParentSessionID: result.Branch.Source.SessionID.String(), ForkEventSeq: result.Branch.Source.EventSeq,
			Workspace: result.Branch.Destination, SessionID: result.Run.SessionID.String(),
			InitialStateID: result.Run.InitialStateID.String(), FinalStateID: result.Run.FinalStateID.String(),
			ExitCode: result.Run.ExitCode,
		}
		if err := json.NewEncoder(stdout).Encode(payload); err != nil {
			fmt.Fprintf(stderr, "rappid: encode JSON result: %v\n", err)
			return 1
		}
	} else {
		fmt.Fprintf(stderr, "\nrappid replay: reran %s as %s from event %d (exit %d)\n",
			result.Branch.Source.ID, result.Run.SessionID, result.Branch.Source.EventSeq, result.Run.ExitCode)
	}

	if result.Run.ExitCode < 0 || result.Run.ExitCode > 255 {
		return 1
	}
	return result.Run.ExitCode
}

func printBranchUsage(w io.Writer) {
	fmt.Fprintln(w, "Usage: rappid replay branch [--data-dir DIR] [--to DIR] [--force] [--json] <state-id>")
}

func printRerunUsage(w io.Writer) {
	fmt.Fprintln(w, "Usage: rappid replay rerun [--data-dir DIR] [--to DIR] [--force] [--json] [--pty auto|on|off] [--terminal-input metadata-only|full|off] [--mode live] --confirm-execution <state-id> -- <command> [args...]")
}
