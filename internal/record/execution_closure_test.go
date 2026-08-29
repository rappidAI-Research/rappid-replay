package record

import (
	"fmt"
	"io"
	"testing"
)

func TestExpectedPTYClosureAcceptsClosedPipe(t *testing.T) {
	for _, err := range []error{
		io.ErrClosedPipe,
		fmt.Errorf("capture PTY output: %w", io.ErrClosedPipe),
	} {
		if !isExpectedPTYClosure(err) {
			t.Fatalf("isExpectedPTYClosure(%v) = false, want true", err)
		}
	}
}
