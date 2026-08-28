//go:build windows

package process

import (
	"errors"
	"unsafe"

	"golang.org/x/sys/windows"
)

func snapshotPlatform() ([]Entry, error) {
	handle, err := windows.CreateToolhelp32Snapshot(windows.TH32CS_SNAPPROCESS, 0)
	if err != nil {
		return nil, err
	}
	defer windows.CloseHandle(handle)

	var item windows.ProcessEntry32
	item.Size = uint32(unsafe.Sizeof(item))
	if err := windows.Process32First(handle, &item); err != nil {
		if errors.Is(err, windows.ERROR_NO_MORE_FILES) {
			return nil, nil
		}
		return nil, err
	}

	var entries []Entry
	for {
		pid := int(item.ProcessID)
		if pid > 0 {
			entries = append(entries, Entry{
				PID:  pid,
				PPID: int(item.ParentProcessID),
				Name: windows.UTF16ToString(item.ExeFile[:]),
			})
		}
		if err := windows.Process32Next(handle, &item); err != nil {
			if errors.Is(err, windows.ERROR_NO_MORE_FILES) {
				break
			}
			return nil, err
		}
	}
	return entries, nil
}
