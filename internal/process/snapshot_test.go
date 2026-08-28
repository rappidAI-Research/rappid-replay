package process

import "testing"

func TestDescendantsReturnsStableParentBeforeChildOrder(t *testing.T) {
	entries := []Entry{
		{PID: 40, PPID: 20, Name: "grandchild"},
		{PID: 30, PPID: 10, Name: "child-b"},
		{PID: 20, PPID: 10, Name: "child-a"},
		{PID: 50, PPID: 999, Name: "unrelated"},
		{PID: 60, PPID: 60, Name: "cycle"},
	}
	got := Descendants(entries, 10)
	want := []int{20, 30, 40}
	if len(got) != len(want) {
		t.Fatalf("Descendants() = %+v, want PIDs %v", got, want)
	}
	for index, pid := range want {
		if got[index].PID != pid {
			t.Fatalf("Descendants()[%d].PID = %d, want %d; all=%+v", index, got[index].PID, pid, got)
		}
	}
}

func TestDescendantsBreaksMalformedCycles(t *testing.T) {
	entries := []Entry{
		{PID: 2, PPID: 1},
		{PID: 3, PPID: 2},
		{PID: 2, PPID: 3},
	}
	got := Descendants(entries, 1)
	if len(got) != 2 || got[0].PID != 2 || got[1].PID != 3 {
		t.Fatalf("Descendants() = %+v, want [2 3] once each", got)
	}
}
