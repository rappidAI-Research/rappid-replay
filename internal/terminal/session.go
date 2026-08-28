// Package terminal isolates the cross-platform pseudo-terminal implementation
// used by the Generic Recorder.
package terminal

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"

	ptylib "github.com/aymanbagabas/go-pty"
)

// Size is a terminal viewport in character cells.
type Size struct {
	Columns int
	Rows    int
}

func (s Size) Valid() bool {
	return s.Columns > 0 && s.Rows > 0
}

// Session owns one pseudo-terminal and at most one attached child command.
type Session struct {
	device      ptylib.Pty
	command     *ptylib.Cmd
	slaveClosed bool
}

// New opens a platform-native PTY (Unix PTY or Windows ConPTY).
func New() (*Session, error) {
	device, err := ptylib.New()
	if err != nil {
		return nil, fmt.Errorf("open pseudo-terminal: %w", err)
	}
	return &Session{device: device}, nil
}

// Start launches name attached to the pseudo-terminal. The parent's Unix slave
// descriptor is closed immediately after the child inherits it so the master
// can observe EOF once the child side disappears.
func (s *Session) Start(ctx context.Context, name string, args []string, dir string, env []string) (*os.Process, error) {
	if s == nil || s.device == nil {
		return nil, fmt.Errorf("pseudo-terminal session is not open")
	}
	if s.command != nil {
		return nil, fmt.Errorf("pseudo-terminal command already started")
	}
	command := s.device.CommandContext(ctx, name, args...)
	command.Dir = dir
	if env != nil {
		command.Env = append([]string(nil), env...)
	}
	if err := command.Start(); err != nil {
		return nil, err
	}
	s.command = command
	if unixPTY, ok := s.device.(ptylib.UnixPty); ok {
		if err := unixPTY.Slave().Close(); err != nil && !errors.Is(err, os.ErrClosed) {
			_ = command.Process.Kill()
			return nil, fmt.Errorf("close parent PTY slave: %w", err)
		}
		s.slaveClosed = true
	}
	return command.Process, nil
}

// Wait waits for the attached child and preserves exec.ExitError semantics from
// the underlying PTY implementation.
func (s *Session) Wait() error {
	if s == nil || s.command == nil {
		return fmt.Errorf("pseudo-terminal command is not started")
	}
	return s.command.Wait()
}

func (s *Session) ProcessState() *os.ProcessState {
	if s == nil || s.command == nil {
		return nil
	}
	return s.command.ProcessState
}

func (s *Session) Read(p []byte) (int, error) {
	if s == nil || s.device == nil {
		return 0, io.ErrClosedPipe
	}
	return s.device.Read(p)
}

func (s *Session) Write(p []byte) (int, error) {
	if s == nil || s.device == nil {
		return 0, io.ErrClosedPipe
	}
	return s.device.Write(p)
}

func (s *Session) Resize(size Size) error {
	if !size.Valid() {
		return fmt.Errorf("invalid terminal size %dx%d", size.Columns, size.Rows)
	}
	if s == nil || s.device == nil {
		return io.ErrClosedPipe
	}
	return s.device.Resize(size.Columns, size.Rows)
}

// Close releases the PTY without treating the deliberately pre-closed Unix
// slave descriptor as a cleanup failure.
func (s *Session) Close() error {
	if s == nil || s.device == nil {
		return nil
	}
	if unixPTY, ok := s.device.(ptylib.UnixPty); ok {
		var errs []error
		if err := unixPTY.Master().Close(); err != nil && !errors.Is(err, os.ErrClosed) {
			errs = append(errs, err)
		}
		if !s.slaveClosed {
			if err := unixPTY.Slave().Close(); err != nil && !errors.Is(err, os.ErrClosed) {
				errs = append(errs, err)
			}
		}
		s.device = nil
		return errors.Join(errs...)
	}
	err := s.device.Close()
	s.device = nil
	return err
}
