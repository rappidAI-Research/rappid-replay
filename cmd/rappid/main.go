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
	"github.com/rappidAI-Research/rappid-replay/internal/record"
	"github.com/rappidAI-Research/rappid-replay/internal/terminal"
)

const terminalResizePollInterval = 200 * time.Millisecond

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	os.Exit(run(ctx, os.Args[1:], os.Stdin, os.Stdout, os.Stderr))
}

func run(ctx context.Context, args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	if len(args) < 2 || args[0] != "replay" || args[1] != "record" {
		printUsage(stderr)
		return 2
	}

	flags := flag.NewFlagSet("rappid replay record", flag.ContinueOnError)
	flags.SetOutput(stderr)
	dataDir := flags.String("data-dir", "", "Replay local data directory")
	workingDir := flags.String("cwd", "", "working directory to record (default: current directory)")
	jsonOutput := flags.Bool("json", false, "emit the recording result as JSON on stdout")
	ptyMode := flags.String("pty", "auto", "terminal mode: auto, on, or off")
	flags.Usage = func() { printRecordUsage(stderr) }
	if err := flags.Parse(args[2:]); err != nil {
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
	defer func() {
		if err := runtime.Close(); err != nil {
			fmt.Fprintf(stderr, "rappid: close local runtime: %v\n", err)
		}
	}()

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
	fmt.Fprintln(w, "Usage: rappid replay record [flags] -- <command> [args...]")
}

func printRecordUsage(w io.Writer) {
	fmt.Fprintln(w, "Usage: rappid replay record [--data-dir DIR] [--cwd DIR] [--json] [--pty auto|on|off] -- <command> [args...]")
}
