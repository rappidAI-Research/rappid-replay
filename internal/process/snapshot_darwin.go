//go:build darwin

package process

import "golang.org/x/sys/unix"

func snapshotPlatform() ([]Entry, error) {
	processes, err := unix.SysctlKinfoProcSlice("kern.proc.all")
	if err != nil {
		return nil, err
	}
	entries := make([]Entry, 0, len(processes))
	for _, item := range processes {
		pid := int(item.Proc.P_pid)
		if pid <= 0 {
			continue
		}
		entries = append(entries, Entry{
			PID:  pid,
			PPID: int(item.Eproc.Ppid),
			Name: darwinProcessName(item.Proc.P_comm[:]),
		})
	}
	return entries, nil
}

func darwinProcessName(raw []byte) string {
	for index, value := range raw {
		if value == 0 {
			return string(raw[:index])
		}
	}
	return string(raw)
}
