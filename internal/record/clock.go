package record

import (
	"sync"
	"time"
)

// runClock derives session-local monotonic timestamps from the monotonic
// component carried by time.Time values returned by time.Now. The lock also
// makes concurrent stdout/stderr observations receive a non-decreasing value.
type runClock struct {
	start time.Time
	mu    sync.Mutex
	last  uint64
}

func newRunClock(start time.Time) *runClock {
	return &runClock{start: start}
}

func (c *runClock) sample() (time.Time, uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now()
	elapsed := now.Sub(c.start)
	var ns uint64
	if elapsed > 0 {
		ns = uint64(elapsed.Nanoseconds())
	}
	if ns < c.last {
		ns = c.last
	}
	c.last = ns
	return now.UTC(), ns
}
