//go:build linux

package process

import "testing"

func TestParseLinuxProcStatHandlesSpacesAndClosingParensInName(t *testing.T) {
	entry, ok := parseLinuxProcStat(123, "123 (worker name) child) S 42 0 0 0 0\n")
	if !ok {
		t.Fatal("parseLinuxProcStat() rejected valid stat")
	}
	if entry.PID != 123 || entry.PPID != 42 || entry.Name != "worker name) child" {
		t.Fatalf("parseLinuxProcStat() = %+v", entry)
	}
}

func TestParseLinuxProcStatRejectsMalformedInput(t *testing.T) {
	for _, input := range []string{"", "123 no-parens S 1", "123 (name) S not-a-pid"} {
		if _, ok := parseLinuxProcStat(123, input); ok {
			t.Fatalf("parseLinuxProcStat(%q) unexpectedly succeeded", input)
		}
	}
}
