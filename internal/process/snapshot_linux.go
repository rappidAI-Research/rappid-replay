//go:build linux

package process

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

func snapshotPlatform() ([]Entry, error) {
	directories, err := os.ReadDir("/proc")
	if err != nil {
		return nil, err
	}
	entries := make([]Entry, 0, len(directories))
	for _, directory := range directories {
		if !directory.IsDir() {
			continue
		}
		pid, err := strconv.Atoi(directory.Name())
		if err != nil || pid <= 0 {
			continue
		}
		content, err := os.ReadFile(filepath.Join("/proc", directory.Name(), "stat"))
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) || errors.Is(err, fs.ErrPermission) {
				continue
			}
			continue
		}
		entry, ok := parseLinuxProcStat(pid, string(content))
		if ok {
			entries = append(entries, entry)
		}
	}
	return entries, nil
}

func parseLinuxProcStat(pid int, stat string) (Entry, bool) {
	open := strings.IndexByte(stat, '(')
	close := strings.LastIndex(stat, ") ")
	if open < 0 || close <= open || close+2 >= len(stat) {
		return Entry{}, false
	}
	fields := strings.Fields(stat[close+2:])
	// Field 3 is process state and field 4 is PPID.
	if len(fields) < 2 {
		return Entry{}, false
	}
	ppid, err := strconv.Atoi(fields[1])
	if err != nil || ppid < 0 {
		return Entry{}, false
	}
	return Entry{PID: pid, PPID: ppid, Name: stat[open+1 : close]}, true
}
