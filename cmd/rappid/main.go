package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"time"

	"github.com/rappidAI-Research/rappid-replay/internal/app"
	"github.com/rappidAI-Research/rappid-replay/internal/config"
	"github.com/rappidAI-Research/rappid-replay/internal/id"
	"github.com/rappidAI-Research/rappid-replay/internal/record"
	"github.com/rappidAI-Research/rappid-replay/internal/replay"
	"github.com/rappidAI-Research/rappid-replay/internal/terminal"
)

const terminalResizePollInterval = 200 * time.Millisecond

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	os.Exit(run(ctx, os.Args[1:], os.Stdin, os.Stdout, os.Stderr))
}

func run(ctx context.Context, args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	if len(args) < 2 || args[0] != "replay" {
		printUsage(stderr)
		return 2
	}
	switch args[1] {
	case "record":
		return runRecord(ctx, args[2:], stdin, stdout, stderr)
	case "verify":
		return runVerify(ctx, args[2:], stdout, stderr)
	case "restore":
		return runRestore(ctx, args[2:], stdout, stderr)
	case "branch":
		return runBranch(ctx, args[2:], stdout, stderr)
	case "rerun":
		return runRerun(ctx, args[2:], stdin, stdout, stderr)
	default:
		printUsage(stderr)
		return 2
	}
}

func runRecord(ctx context.Context, args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("rappid replay record", flag.ContinueOnError)
	flags.SetOutput(stderr)
	dataDir := flags.String("data-dir", "", "Replay local data directory")
	workingDir := flags.String("cwd", "", "working directory to record (default: current directory)")
	jsonOutput := flags.Bool("json", false, "emit the recording result as JSON on stdout")
	ptyMode := flags.String("pty", "auto", "terminal mode: auto, on, or off")
	flags.Usage = func() { printRecordUsage(stderr) }
	if err := flags.Parse(args); err != nil {
		return 2
	}
	command := flags.Args()
	if len(command) == 0 {
		fmt.Fprintln(stderr, "error: command is required after --")
		printRecordUsage(stderr)
		return 2
	}

	workspace := *workingDir
	if workspace == "" {
		var err error
		workspace, err = os.Getwd()
		if err != nil {
			fmt.Fprintf(stderr, "rappid: resolve working directory: %v\n", err)
			return 1
		}
	}
	layout, err := app.ResolveLayout(*dataDir)
	if err != nil {
		fmt.Fprintf(stderr, "rappid: resolve local runtime: %v\n", err)
		return 1
	}
	if err := app.ValidateWorkspaceSeparation(workspace, layout.Root); err != nil {
		fmt.Fprintf(stderr, "rappid: unsafe storage layout: %v\n", err)
		return 1
	}

	resolution, err := config.Load(config.LoadOptions{WorkingDir: workspace})
	if err != nil {
		fmt.Fprintf(stderr, "rappid: load configuration: %v\n", err)
		return 1
	}
	runtime, err := app.OpenRuntime(ctx, layout.Root)
	if err != nil {
		fmt.Fprintf(stderr, "rappid: open local runtime: %v\n", err)
		return 1
	}
	defer closeRuntime(runtime, stderr)

	childStdout, childStderr := stdout, stderr
	if *jsonOutput {
		// Keep stdout machine-readable. Child output remains observable but is
		// routed to stderr while its original stdout/stderr identity is preserved
		// in pipe-mode terminal events. PTY mode intentionally has one combined
		// terminal output stream.
		childStdout = stderr
		childStderr = stderr
	}

	usePTY, err := resolvePTYMode(*ptyMode, stdin, childStdout)
	if err != nil {
		fmt.Fprintf(stderr, "rappid: %v\n", err)
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

	result, recordErr := record.Run(ctx, record.Dependencies{DB: runtime.DB, CAS: runtime.CAS}, record.Options{
		Command:             command,
		WorkingDir:          workspace,
		Ignore:              resolution.Config.Record.Ignore,
		TerminalInput:       resolution.Config.Record.TerminalInput,
		PTY:                 usePTY,
		InitialTerminalSize: initialSize,
		TerminalResize:      resize,
		Stdin:               stdin,
		Stdout:              childStdout,
		Stderr:              childStderr,
	})
	if restoreTerminal != nil {
		if restoreErr := restoreTerminal(); restoreErr != nil && recordErr == nil {
			recordErr = fmt.Errorf("restore terminal mode: %w", restoreErr)
		}
		restoreTerminal = nil
	}
	if recordErr != nil {
		fmt.Fprintf(stderr, "rappid: record failed: %v\n", recordErr)
		return 1
	}

	if *jsonOutput {
		payload := struct {
			SessionID      string `json:"session_id"`
			InitialStateID string `json:"initial_state_id"`
			FinalStateID   string `json:"final_state_id"`
			ExitCode       int    `json:"exit_code"`
		}{
			SessionID:      result.SessionID.String(),
			InitialStateID: result.InitialStateID.String(),
			FinalStateID:   result.FinalStateID.String(),
			ExitCode:       result.ExitCode,
		}
		if err := json.NewEncoder(stdout).Encode(payload); err != nil {
			fmt.Fprintf(stderr, "rappid: encode JSON result: %v\n", err)
			return 1
		}
	} else {
		fmt.Fprintf(stderr, "\nrappid replay: recorded %s (exit %d)\n", result.SessionID, result.ExitCode)
	}

	if result.ExitCode < 0 || result.ExitCode > 255 {
		return 1
	}
	return result.ExitCode
}

func runVerify(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("rappid replay verify", flag.ContinueOnError)
	flags.SetOutput(stderr)
	dataDir := flags.String("data-dir", "", "Replay local data directory")
	jsonOutput := flags.Bool("json", false, "emit verification result as JSON")
	flags.Usage = func() { printVerifyUsage(stderr) }
	if err := flags.Parse(args); err != nil {
		return 2
	}
	if flags.NArg() != 1 {
		printVerifyUsage(stderr)
		return 2
	}
	stateID, err := id.ParseState(flags.Arg(0))
	if err != nil {
		fmt.Fprintf(stderr, "rappid: invalid state id: %v\n", err)
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

	result, err := replay.VerifyState(ctx, replay.Dependencies{DB: runtime.DB, CAS: runtime.CAS}, stateID)
	if err != nil {
		fmt.Fprintf(stderr, "rappid: verify failed: %v\n", err)
		return 1
	}
	if *jsonOutput {
		payload := verificationPayload(result)
		if err := json.NewEncoder(stdout).Encode(payload); err != nil {
			fmt.Fprintf(stderr, "rappid: encode JSON result: %v\n", err)
			return 1
		}
		return 0
	}
	fmt.Fprintf(stdout, "verified %s (%d files, %d directories, %d symlinks, %d bytes)\n",
		result.State.ID,
		result.Verification.Files,
		result.Verification.Directories,
		result.Verification.Symlinks,
		result.Verification.FileBytes,
	)
	return 0
}

func runRestore(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("rappid replay restore", flag.ContinueOnError)
	flags.SetOutput(stderr)
	dataDir := flags.String("data-dir", "", "Replay local data directory")
	destination := flags.String("to", "", "restore destination directory")
	force := flags.Bool("force", false, "replace an existing destination directory")
	jsonOutput := flags.Bool("json", false, "emit restore result as JSON")
	flags.Usage = func() { printRestoreUsage(stderr) }
	if err := flags.Parse(args); err != nil {
		return 2
	}
	if flags.NArg() != 1 {
		printRestoreUsage(stderr)
		return 2
	}
	stateID, err := id.ParseState(flags.Arg(0))
	if err != nil {
		fmt.Fprintf(stderr, "rappid: invalid state id: %v\n", err)
		return 2
	}
	target := *destination
	if target == "" {
		target = replay.DefaultRestoreDestination(stateID)
	}
	layout, err := app.ResolveLayout(*dataDir)
	if err != nil {
		fmt.Fprintf(stderr, "rappid: resolve local runtime: %v\n", err)
		return 1
	}
	if err := app.ValidateMaterializationSeparation(target, layout.Root); err != nil {
		fmt.Fprintf(stderr, "rappid: unsafe restore destination: %v\n", err)
		return 1
	}
	runtime, err := app.OpenRuntime(ctx, layout.Root)
	if err != nil {
		fmt.Fprintf(stderr, "rappid: open local runtime: %v\n", err)
		return 1
	}
	defer closeRuntime(runtime, stderr)

	result, err := replay.RestoreState(ctx, replay.Dependencies{DB: runtime.DB, CAS: runtime.CAS}, replay.RestoreOptions{
		StateID: stateID, Destination: target, Force: *force,
	})
	if err != nil {
		fmt.Fprintf(stderr, "rappid: restore failed: %v\n", err)
		return 1
	}
	if *jsonOutput {
		payload := struct {
			StateID     string `json:"state_id"`
			SessionID   string `json:"session_id"`
			RootTreeID  string `json:"root_tree_id"`
			Destination string `json:"destination"`
			Files       int    `json:"files"`
			Directories int    `json:"directories"`
			Symlinks    int    `json:"symlinks"`
			FileBytes   int64  `json:"file_bytes"`
		}{
			StateID: result.State.ID.String(), SessionID: result.State.SessionID.String(),
			RootTreeID: result.State.RootTreeID.String(), Destination: result.Destination,
			Files: result.Verification.Files, Directories: result.Verification.Directories,
			Symlinks: result.Verification.Symlinks, FileBytes: result.Verification.FileBytes,
		}
		if err := json.NewEncoder(stdout).Encode(payload); err != nil {
			fmt.Fprintf(stderr, "rappid: encode JSON result: %v\n", err)
			return 1
		}
		return 0
	}
	fmt.Fprintf(stdout, "restored %s to %s\n", result.State.ID, result.Destination)
	return 0
}

func verificationPayload(result replay.VerifyResult) struct {
	StateID     string `json:"state_id"`
	SessionID   string `json:"session_id"`
	RootTreeID  string `json:"root_tree_id"`
	EventSeq    uint64 `json:"event_seq"`
	Trees       int    `json:"trees"`
	Files       int    `json:"files"`
	Directories int    `json:"directories"`
	Symlinks    int    `json:"symlinks"`
	FileBytes   int64  `json:"file_bytes"`
} {
	return struct {
		StateID     string `json:"state_id"`
		SessionID   string `json:"session_id"`
		RootTreeID  string `json:"root_tree_id"`
		EventSeq    uint64 `json:"event_seq"`
		Trees       int    `json:"trees"`
		Files       int    `json:"files"`
		Directories int    `json:"directories"`
		Symlinks    int    `json:"symlinks"`
		FileBytes   int64  `json:"file_bytes"`
	}{
		StateID: result.State.ID.String(), SessionID: result.State.SessionID.String(),
		RootTreeID: result.State.RootTreeID.String(), EventSeq: result.State.EventSeq,
		Trees: result.Verification.Trees, Files: result.Verification.Files,
		Directories: result.Verification.Directories, Symlinks: result.Verification.Symlinks,
		FileBytes: result.Verification.FileBytes,
	}
}

func closeRuntime(runtime *app.Runtime, stderr io.Writer) {
	if err := runtime.Close(); err != nil {
		fmt.Fprintf(stderr, "rappid: close local runtime: %v\n", err)
	}
}

func resolvePTYMode(mode string, stdin io.Reader, output io.Writer) (bool, error) {
	switch mode {
	case "on":
		return true, nil
	case "off":
		return false, nil
	case "auto":
		inputFile, inputOK := stdin.(*os.File)
		outputFile, outputOK := output.(*os.File)
		return inputOK && outputOK && terminal.IsTerminal(inputFile) && terminal.IsTerminal(outputFile), nil
	default:
		return false, fmt.Errorf("invalid --pty value %q (want auto, on, or off)", mode)
	}
}

func watchTerminalSize(parent context.Context, file *os.File, initial terminal.Size) (<-chan terminal.Size, func()) {
	ctx, cancel := context.WithCancel(parent)
	changes := make(chan terminal.Size, 1)
	go func() {
		defer close(changes)
		last := initial
		ticker := time.NewTicker(terminalResizePollInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				size, err := terminal.HostSize(file)
				if err != nil || size == last {
					continue
				}
				select {
				case changes <- size:
					last = size
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return changes, cancel
}

func printUsage(w io.Writer) {
	fmt.Fprintln(w, "Usage: rappid replay <record|verify|restore|branch|rerun> ...")
}

func printRecordUsage(w io.Writer) {
	fmt.Fprintln(w, "Usage: rappid replay record [--data-dir DIR] [--cwd DIR] [--json] [--pty auto|on|off] -- <command> [args...]")
}

func printVerifyUsage(w io.Writer) {
	fmt.Fprintln(w, "Usage: rappid replay verify [--data-dir DIR] [--json] <state-id>")
}

func printRestoreUsage(w io.Writer) {
	fmt.Fprintln(w, "Usage: rappid replay restore [--data-dir DIR] [--to DIR] [--force] [--json] <state-id>")
}
