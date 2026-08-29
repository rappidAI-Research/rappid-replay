package record

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sync"
	"syscall"
	"time"

	"github.com/rappidAI-Research/rappid-replay/internal/event"
	"github.com/rappidAI-Research/rappid-replay/internal/terminal"
)

const ptyDrainGrace = 250 * time.Millisecond

type commandExecution interface {
	PID() int
	Wait() error
	Finalize() error
}

type pipeExecution struct {
	command        *exec.Cmd
	stdoutRecorder *streamEventWriter
	stderrRecorder *streamEventWriter
}

func (e *pipeExecution) PID() int    { return e.command.Process.Pid }
func (e *pipeExecution) Wait() error { return e.command.Wait() }
func (e *pipeExecution) Finalize() error {
	return errors.Join(e.stdoutRecorder.Flush(), e.stderrRecorder.Flush())
}

type asyncFailure struct {
	mu  sync.Mutex
	err error
}

func (f *asyncFailure) set(err error) bool {
	if err == nil {
		return false
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return false
	}
	f.err = err
	return true
}

func (f *asyncFailure) get() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.err
}

type ptyExecution struct {
	session        *terminal.Session
	outputRecorder *streamEventWriter
	outputDone     chan error
	stop           chan struct{}
	stopOnce       sync.Once
	failure        asyncFailure
	sink           *eventSink
}

type ptyRunning struct {
	*ptyExecution
	pid int
}

func (e *ptyRunning) PID() int { return e.pid }

func (e *ptyExecution) stopPumps() {
	e.stopOnce.Do(func() { close(e.stop) })
}

func (e *ptyExecution) Wait() error {
	waitErr := e.session.Wait()
	e.stopPumps()
	return waitErr
}

func (e *ptyExecution) Finalize() error {
	e.stopPumps()
	var outputErr error
	select {
	case outputErr = <-e.outputDone:
	case <-time.After(ptyDrainGrace):
		_ = e.sink.append("terminal.drain", struct {
			Complete bool   `json:"complete"`
			Reason   string `json:"reason"`
		}{Complete: false, Reason: "post-exit-timeout"})
		_ = e.session.Close()
		outputErr = <-e.outputDone
	}
	if isExpectedPTYClosure(outputErr) {
		outputErr = nil
	}
	flushErr := e.outputRecorder.Flush()
	closeErr := e.session.Close()
	return errors.Join(e.failure.get(), outputErr, flushErr, closeErr)
}

func isExpectedPTYClosure(err error) bool {
	return err == nil || errors.Is(err, io.EOF) || errors.Is(err, io.ErrClosedPipe) || errors.Is(err, os.ErrClosed) || errors.Is(err, syscall.EIO)
}

func startRecordedExecution(
	ctx context.Context,
	cancel context.CancelFunc,
	sink *eventSink,
	options Options,
	workingDir string,
	recordedCommand []string,
	commandRedacted bool,
) (commandExecution, error) {
	if options.PTY {
		return startPTYExecution(ctx, cancel, sink, options, workingDir, recordedCommand, commandRedacted)
	}
	return startPipeExecution(ctx, sink, options, workingDir, recordedCommand, commandRedacted)
}

func startPipeExecution(
	ctx context.Context,
	sink *eventSink,
	options Options,
	workingDir string,
	recordedCommand []string,
	commandRedacted bool,
) (commandExecution, error) {
	command := exec.CommandContext(ctx, options.Command[0], options.Command[1:]...)
	command.Dir = workingDir
	command.Stdin = options.Stdin
	if options.Env != nil {
		command.Env = append([]string(nil), options.Env...)
	}

	streamGate := make(chan struct{})
	stdoutRecorder := &streamEventWriter{sink: sink, stream: "stdout", output: options.Stdout}
	stderrRecorder := &streamEventWriter{sink: sink, stream: "stderr", output: options.Stderr}
	command.Stdout = gatedWriter{ready: streamGate, writer: stdoutRecorder}
	command.Stderr = gatedWriter{ready: streamGate, writer: stderrRecorder}

	if err := command.Start(); err != nil {
		close(streamGate)
		return nil, fmt.Errorf("start recorded command: %w", err)
	}
	if err := appendProcessStarted(sink, command.Process.Pid, command.Path, workingDir, recordedCommand, commandRedacted, false); err != nil {
		close(streamGate)
		_ = command.Process.Kill()
		_ = command.Wait()
		return nil, err
	}
	close(streamGate)
	return &pipeExecution{command: command, stdoutRecorder: stdoutRecorder, stderrRecorder: stderrRecorder}, nil
}

func startPTYExecution(
	ctx context.Context,
	cancel context.CancelFunc,
	sink *eventSink,
	options Options,
	workingDir string,
	recordedCommand []string,
	commandRedacted bool,
) (commandExecution, error) {
	session, err := terminal.New()
	if err != nil {
		return nil, fmt.Errorf("start recorded command PTY: %w", err)
	}
	cleanup := true
	defer func() {
		if cleanup {
			_ = session.Close()
		}
	}()

	if options.InitialTerminalSize.Valid() {
		if err := session.Resize(options.InitialTerminalSize); err != nil {
			return nil, fmt.Errorf("set initial PTY size: %w", err)
		}
	}
	if err := sink.append("terminal.opened", struct {
		Mode        string `json:"mode"`
		Output      string `json:"output"`
		InputPolicy string `json:"input_policy"`
		Columns     int    `json:"columns,omitempty"`
		Rows        int    `json:"rows,omitempty"`
	}{
		Mode: "pty", Output: "combined", InputPolicy: options.TerminalInput,
		Columns: options.InitialTerminalSize.Columns, Rows: options.InitialTerminalSize.Rows,
	}); err != nil {
		return nil, err
	}

	process, err := session.Start(ctx, options.Command[0], options.Command[1:], workingDir, options.Env)
	if err != nil {
		return nil, fmt.Errorf("start recorded command: %w", err)
	}
	path := options.Command[0]
	if resolved, lookErr := exec.LookPath(options.Command[0]); lookErr == nil {
		path = resolved
	}
	if err := appendProcessStarted(sink, process.Pid, path, workingDir, recordedCommand, commandRedacted, true); err != nil {
		_ = process.Kill()
		_ = session.Wait()
		return nil, err
	}

	execution := &ptyExecution{
		session:        session,
		outputRecorder: &streamEventWriter{sink: sink, stream: "output", output: options.Stdout},
		outputDone:     make(chan error, 1),
		stop:           make(chan struct{}),
		sink:           sink,
	}
	cleanup = false

	go func() {
		_, copyErr := io.Copy(execution.outputRecorder, session)
		execution.outputDone <- copyErr
		if !isExpectedPTYClosure(copyErr) && execution.failure.set(fmt.Errorf("capture PTY output: %w", copyErr)) {
			cancel()
		}
	}()

	if options.Stdin != nil {
		go pumpPTYInput(execution, cancel, options.Stdin, options.TerminalInput)
	}
	if options.TerminalResize != nil {
		go pumpPTYResize(execution, cancel, options.TerminalResize, options.InitialTerminalSize)
	}

	return &ptyRunning{ptyExecution: execution, pid: process.Pid}, nil
}

func appendProcessStarted(
	sink *eventSink,
	pid int,
	path string,
	workingDir string,
	recordedCommand []string,
	commandRedacted bool,
	pty bool,
) error {
	return sink.appendTechnical("process.started", struct {
		PID     int      `json:"pid"`
		Path    string   `json:"path"`
		Command []string `json:"command"`
		CWD     string   `json:"cwd"`
		PTY     bool     `json:"pty"`
	}{PID: pid, Path: path, Command: recordedCommand, CWD: workingDir, PTY: pty}, commandRedacted)
}

func pumpPTYInput(execution *ptyExecution, cancel context.CancelFunc, source io.Reader, policy string) {
	buffer := make([]byte, 32<<10)
	for {
		n, readErr := source.Read(buffer)
		if n > 0 {
			written, writeErr := execution.session.Write(buffer[:n])
			if written > 0 {
				if err := persistPTYInput(execution.sink, buffer[:written], policy); err != nil {
					if execution.failure.set(err) {
						cancel()
					}
					return
				}
			}
			if writeErr != nil {
				if execution.failure.set(fmt.Errorf("write PTY input: %w", writeErr)) {
					cancel()
				}
				return
			}
			if written != n {
				if execution.failure.set(io.ErrShortWrite) {
					cancel()
				}
				return
			}
		}
		if readErr != nil {
			if !errors.Is(readErr, io.EOF) && execution.failure.set(fmt.Errorf("read PTY input: %w", readErr)) {
				cancel()
			}
			return
		}
		select {
		case <-execution.stop:
			return
		default:
		}
	}
}

func persistPTYInput(sink *eventSink, data []byte, policy string) error {
	switch policy {
	case "off":
		return nil
	case "metadata-only":
		return sink.append("terminal.stdin", struct {
			Bytes   int    `json:"bytes"`
			Capture string `json:"capture"`
		}{Bytes: len(data), Capture: "metadata-only"})
	case "full":
		persisted, redacted := sink.redactContent(data)
		return sink.appendWithPrivacy("terminal.stdin", struct {
			Encoding    string `json:"encoding"`
			DataB64     string `json:"data_b64"`
			Bytes       int    `json:"bytes"`
			StoredBytes int    `json:"stored_bytes"`
		}{
			Encoding: "base64", DataB64: base64.StdEncoding.EncodeToString(persisted),
			Bytes: len(data), StoredBytes: len(persisted),
		}, event.Privacy{Classification: "content", Redacted: redacted})
	default:
		return fmt.Errorf("unsupported terminal input policy %q", policy)
	}
}

func pumpPTYResize(execution *ptyExecution, cancel context.CancelFunc, changes <-chan terminal.Size, initial terminal.Size) {
	last := initial
	for {
		select {
		case <-execution.stop:
			return
		case size, ok := <-changes:
			if !ok {
				return
			}
			if !size.Valid() || size == last {
				continue
			}
			if err := execution.session.Resize(size); err != nil {
				if execution.failure.set(fmt.Errorf("resize PTY: %w", err)) {
					cancel()
				}
				return
			}
			if err := execution.sink.append("terminal.resized", struct {
				Columns int `json:"columns"`
				Rows    int `json:"rows"`
			}{Columns: size.Columns, Rows: size.Rows}); err != nil {
				if execution.failure.set(err) {
					cancel()
				}
				return
			}
			last = size
		}
	}
}
