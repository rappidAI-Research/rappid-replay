package record

import (
	"context"
	"fmt"
	"sync"
	"time"

	processinfo "github.com/rappidAI-Research/rappid-replay/internal/process"
)

const processDiscoveryInterval = 100 * time.Millisecond

type processTreeResult struct {
	Discovered int
	Scans      int
	ScanErrors int
	Err        error
}

type processTreeMonitor struct {
	stop chan struct{}
	done chan processTreeResult
	once sync.Once
}

func startProcessTreeMonitor(ctx context.Context, sink *eventSink, rootPID int, cancelCommand context.CancelFunc) *processTreeMonitor {
	monitor := &processTreeMonitor{
		stop: make(chan struct{}),
		done: make(chan processTreeResult, 1),
	}
	go func() {
		monitor.done <- runProcessTreeMonitor(ctx, sink, rootPID, monitor.stop, cancelCommand)
	}()
	return monitor
}

func (m *processTreeMonitor) Stop() processTreeResult {
	m.once.Do(func() { close(m.stop) })
	return <-m.done
}

func runProcessTreeMonitor(
	ctx context.Context,
	sink *eventSink,
	rootPID int,
	stop <-chan struct{},
	cancelCommand context.CancelFunc,
) processTreeResult {
	result := processTreeResult{}
	seen := map[int]bool{rootPID: true}

	fatal := func(err error) processTreeResult {
		if cancelCommand != nil {
			cancelCommand()
		}
		result.Err = err
		return result
	}

	scan := func() error {
		entries, err := processinfo.Snapshot()
		result.Scans++
		if err != nil {
			result.ScanErrors++
			if appendErr := sink.append("process.discovery.error", struct {
				Error string `json:"error"`
			}{Error: err.Error()}); appendErr != nil {
				return appendErr
			}
			return nil
		}
		for _, child := range processinfo.Descendants(entries, rootPID) {
			if seen[child.PID] {
				continue
			}
			seen[child.PID] = true
			if err := sink.append("process.discovered", struct {
				PID       int    `json:"pid"`
				PPID      int    `json:"ppid"`
				Name      string `json:"name,omitempty"`
				Discovery string `json:"discovery"`
			}{PID: child.PID, PPID: child.PPID, Name: child.Name, Discovery: "sampled"}); err != nil {
				return err
			}
			result.Discovered++
		}
		return nil
	}

	if err := scan(); err != nil {
		return fatal(err)
	}

	ticker := time.NewTicker(processDiscoveryInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return result
		case <-stop:
			if err := scan(); err != nil {
				return fatal(err)
			}
			if err := sink.append("process.tree", struct {
				RootPID             int  `json:"root_pid"`
				Discovered          int  `json:"discovered"`
				Scans               int  `json:"scans"`
				ScanErrors          int  `json:"scan_errors"`
				Sampled             bool `json:"sampled"`
				Complete            bool `json:"complete"`
				IntervalMillis      int  `json:"interval_ms"`
				ShortLivedMayBeLost bool `json:"short_lived_may_be_lost"`
			}{
				RootPID: rootPID, Discovered: result.Discovered, Scans: result.Scans,
				ScanErrors: result.ScanErrors, Sampled: true, Complete: false,
				IntervalMillis: int(processDiscoveryInterval / time.Millisecond), ShortLivedMayBeLost: true,
			}); err != nil {
				return fatal(fmt.Errorf("persist process tree summary: %w", err))
			}
			return result
		case <-ticker.C:
			if err := scan(); err != nil {
				return fatal(err)
			}
		}
	}
}
