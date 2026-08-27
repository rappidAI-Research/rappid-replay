//go:build windows

package store

// Windows does not expose a portable directory-fsync primitive through the Go
// standard library. Replay still fsyncs every staging file before the atomic
// no-replace link commit. The directory helper is intentionally a no-op here;
// stronger volume-specific flushing can be added behind this platform boundary
// without changing the CAS format.
func syncDir(string) error { return nil }
