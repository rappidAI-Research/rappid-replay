package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/rappidAI-Research/rappid-replay/internal/app"
	"github.com/rappidAI-Research/rappid-replay/internal/config"
	"github.com/rappidAI-Research/rappid-replay/internal/id"
	"github.com/rappidAI-Research/rappid-replay/internal/portable"
)

func runExport(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("rappid replay export", flag.ContinueOnError)
	flags.SetOutput(stderr)
	dataDir := flags.String("data-dir", "", "Replay local data directory")
	out := flags.String("out", "", "output .rplay path (default: <session-id>.rplay)")
	force := flags.Bool("force", false, "replace an existing archive")
	jsonOutput := flags.Bool("json", false, "emit export result as JSON")
	secretScan := flags.String("secret-scan", "", "export secret scan: block, warn, or off")
	flags.Usage = func() { printExportUsage(stderr) }
	if err := flags.Parse(args); err != nil {
		return 2
	}
	if flags.NArg() != 1 {
		printExportUsage(stderr)
		return 2
	}
	sessionID, err := id.ParseSession(flags.Arg(0))
	if err != nil {
		fmt.Fprintf(stderr, "rappid: invalid session id: %v\n", err)
		return 2
	}
	cwd, err := os.Getwd()
	if err != nil {
		fmt.Fprintf(stderr, "rappid: resolve working directory: %v\n", err)
		return 1
	}
	resolution, err := config.Load(config.LoadOptions{WorkingDir: cwd})
	if err != nil {
		fmt.Fprintf(stderr, "rappid: load configuration: %v\n", err)
		return 1
	}
	scanMode := *secretScan
	if scanMode == "" {
		scanMode = resolution.Config.Privacy.ExportSecretScan
	}
	target := *out
	if target == "" {
		target = sessionID.String() + ".rplay"
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
	result, err := portable.ExportFile(ctx, portable.Dependencies{DB: runtime.DB, CAS: runtime.CAS}, portable.ExportOptions{
		SessionID:  sessionID,
		Path:       target,
		Force:      *force,
		SecretScan: scanMode,
	})
	if err != nil {
		fmt.Fprintf(stderr, "rappid: export failed: %v\n", err)
		return 1
	}
	if scanMode == "warn" {
		for _, finding := range result.Findings {
			fmt.Fprintf(stderr, "rappid: export warning: potential %s in %s\n", finding.Pattern, finding.Source)
		}
	}
	if *jsonOutput {
		ids := make([]string, 0, len(result.Sessions))
		for _, item := range result.Sessions {
			ids = append(ids, item.String())
		}
		payload := struct {
			Path           string   `json:"path"`
			Sessions       []string `json:"sessions"`
			States         int      `json:"states"`
			Objects        int      `json:"objects"`
			SecretFindings int      `json:"secret_findings"`
		}{Path: result.Path, Sessions: ids, States: result.States, Objects: result.Objects, SecretFindings: len(result.Findings)}
		if err := json.NewEncoder(stdout).Encode(payload); err != nil {
			fmt.Fprintf(stderr, "rappid: encode JSON result: %v\n", err)
			return 1
		}
		return 0
	}
	fmt.Fprintf(stdout, "exported %s (%d session(s), %d state(s), %d object(s))\n", result.Path, len(result.Sessions), result.States, result.Objects)
	return 0
}

func runImport(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("rappid replay import", flag.ContinueOnError)
	flags.SetOutput(stderr)
	dataDir := flags.String("data-dir", "", "Replay local data directory")
	jsonOutput := flags.Bool("json", false, "emit import result as JSON")
	flags.Usage = func() { printImportUsage(stderr) }
	if err := flags.Parse(args); err != nil {
		return 2
	}
	if flags.NArg() != 1 {
		printImportUsage(stderr)
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
	result, err := portable.ImportFile(ctx, portable.Dependencies{DB: runtime.DB, CAS: runtime.CAS}, flags.Arg(0))
	if err != nil {
		fmt.Fprintf(stderr, "rappid: import failed: %v\n", err)
		return 1
	}
	ids := make([]string, 0, len(result.Sessions))
	for _, item := range result.Sessions {
		ids = append(ids, item.String())
	}
	if *jsonOutput {
		payload := struct {
			Sessions []string `json:"sessions"`
			States   int      `json:"states"`
			Objects  int      `json:"objects"`
		}{Sessions: ids, States: result.States, Objects: result.Objects}
		if err := json.NewEncoder(stdout).Encode(payload); err != nil {
			fmt.Fprintf(stderr, "rappid: encode JSON result: %v\n", err)
			return 1
		}
		return 0
	}
	fmt.Fprintf(stdout, "imported %d session(s), %d state(s), %d object(s)\n", len(ids), result.States, result.Objects)
	return 0
}

func runArchiveVerify(path string, jsonOutput bool, stdout, stderr io.Writer) int {
	result, err := portable.VerifyFile(path)
	if err != nil {
		fmt.Fprintf(stderr, "rappid: archive verify failed: %v\n", err)
		return 1
	}
	if jsonOutput {
		if err := json.NewEncoder(stdout).Encode(result); err != nil {
			fmt.Fprintf(stderr, "rappid: encode JSON result: %v\n", err)
			return 1
		}
		return 0
	}
	fmt.Fprintf(stdout, "verified archive %s (%d session(s), %d state(s), %d event(s), %d artifact(s), %d object(s))\n",
		path, result.Sessions, result.States, result.Events, result.Artifacts, result.Objects)
	return 0
}

func printExportUsage(w io.Writer) {
	fmt.Fprintln(w, "Usage: rappid replay export [--data-dir DIR] [--out FILE] [--force] [--secret-scan block|warn|off] [--json] <session-id>")
}

func printImportUsage(w io.Writer) {
	fmt.Fprintln(w, "Usage: rappid replay import [--data-dir DIR] [--json] <archive.rplay>")
}
