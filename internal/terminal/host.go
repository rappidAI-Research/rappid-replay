package terminal

import (
	"fmt"
	"os"

	"golang.org/x/term"
)

// IsTerminal reports whether file is attached to an interactive terminal.
func IsTerminal(file *os.File) bool {
	return file != nil && term.IsTerminal(int(file.Fd()))
}

// HostSize returns the current terminal viewport.
func HostSize(file *os.File) (Size, error) {
	if file == nil {
		return Size{}, fmt.Errorf("terminal file is required")
	}
	columns, rows, err := term.GetSize(int(file.Fd()))
	if err != nil {
		return Size{}, err
	}
	size := Size{Columns: columns, Rows: rows}
	if !size.Valid() {
		return Size{}, fmt.Errorf("terminal reported invalid size %dx%d", columns, rows)
	}
	return size, nil
}

// MakeRaw switches an interactive host terminal to raw mode and returns a
// restoration closure. Callers should defer the closure immediately.
func MakeRaw(file *os.File) (func() error, error) {
	if file == nil {
		return nil, fmt.Errorf("terminal file is required")
	}
	state, err := term.MakeRaw(int(file.Fd()))
	if err != nil {
		return nil, err
	}
	return func() error {
		return term.Restore(int(file.Fd()), state)
	}, nil
}
