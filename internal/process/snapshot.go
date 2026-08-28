// Package process provides read-only process discovery for the Generic Recorder.
package process

import "sort"

// Entry is the minimum process metadata Replay needs to reconstruct parent-child
// relationships without persisting command lines that may contain secrets.
type Entry struct {
	PID  int    `json:"pid"`
	PPID int    `json:"ppid"`
	Name string `json:"name,omitempty"`
}

// Snapshot returns a best-effort point-in-time view of processes visible to the
// current user. Individual processes that disappear during enumeration may be
// omitted; a platform-level enumeration failure is returned.
func Snapshot() ([]Entry, error) {
	entries, err := snapshotPlatform()
	if err != nil {
		return nil, err
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].PID != entries[j].PID {
			return entries[i].PID < entries[j].PID
		}
		return entries[i].PPID < entries[j].PPID
	})
	return entries, nil
}

// Descendants returns the currently visible descendants of rootPID in stable
// parent-before-child order. The root process itself is not included.
func Descendants(entries []Entry, rootPID int) []Entry {
	if rootPID <= 0 {
		return nil
	}
	children := make(map[int][]Entry)
	for _, entry := range entries {
		if entry.PID <= 0 || entry.PID == entry.PPID {
			continue
		}
		children[entry.PPID] = append(children[entry.PPID], entry)
	}
	for parent := range children {
		sort.Slice(children[parent], func(i, j int) bool {
			return children[parent][i].PID < children[parent][j].PID
		})
	}

	visited := map[int]bool{rootPID: true}
	queue := []int{rootPID}
	var result []Entry
	for len(queue) > 0 {
		parent := queue[0]
		queue = queue[1:]
		for _, child := range children[parent] {
			if visited[child.PID] {
				continue
			}
			visited[child.PID] = true
			result = append(result, child)
			queue = append(queue, child.PID)
		}
	}
	return result
}
