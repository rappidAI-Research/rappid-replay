package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"

	"github.com/rappidAI-Research/rappid-replay/internal/app"
	"github.com/rappidAI-Research/rappid-replay/internal/config"
	"github.com/rappidAI-Research/rappid-replay/internal/record"
)

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

	resolution, err := config.Load(config.LoadOptions{WorkingDir: *workingDir})
	if err != nil {
		fmt.Fprintf(stderr, "rappid: load configuration: %v\n", err)
		return 1
	}
	runtime, err := app.OpenRuntime(ctx, *dataDir)
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
		// in Replay's terminal.* events.
		childStdout = stderr
		childStderr = stderr
	}

	result, err := record.Run(ctx, record.Dependencies{DB: runtime.DB, CAS: runtime.CAS}, record.Options{
		Command:       command,
		WorkingDir:    *workingDir,
		Ignore:        resolution.Config.Record.Ignore,
		TerminalInput: resolution.Config.Record.TerminalInput,
		Stdin:         stdin,
		Stdout:        childStdout,
		Stderr:        childStderr,
	})
	if err != nil {
		fmt.Fprintf(stderr, "rappid: record failed: %v\n", err)
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

func printUsage(w io.Writer) {
	fmt.Fprintln(w, "Usage: rappid replay record [flags] -- <command> [args...]")
}

func printRecordUsage(w io.Writer) {
	fmt.Fprintln(w, "Usage: rappid replay record [--data-dir DIR] [--cwd DIR] [--json] -- <command> [args...]")
}
