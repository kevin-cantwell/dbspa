package main

import (
	"fmt"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

// TimePoint is a single observation of process metrics at a point in time.
type TimePoint struct {
	ElapsedSec float64 `json:"elapsed_sec"`
	RSSMB      float64 `json:"rss_mb"`
	Records    int64   `json:"records"`
	RecPerSec  float64 `json:"rec_per_sec"`
}

// ProcessMonitor polls a subprocess's RSS and records throughput over time.
type ProcessMonitor struct {
	pid      int
	interval time.Duration
	start    time.Time

	mu      sync.Mutex
	points  []TimePoint
	records int64
	done    chan struct{}
}

func NewProcessMonitor(pid int, interval time.Duration) *ProcessMonitor {
	return &ProcessMonitor{
		pid:      pid,
		interval: interval,
		done:     make(chan struct{}),
	}
}

func (m *ProcessMonitor) Start() {
	m.start = time.Now()
	go m.poll()
}

func (m *ProcessMonitor) AddRecords(n int64) {
	m.mu.Lock()
	m.records += n
	m.mu.Unlock()
}

func (m *ProcessMonitor) Stop() []TimePoint {
	close(m.done)
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.points
}

func (m *ProcessMonitor) PeakRSSMB() float64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	var peak float64
	for _, p := range m.points {
		if p.RSSMB > peak {
			peak = p.RSSMB
		}
	}
	return peak
}

func (m *ProcessMonitor) FinalRSSMB() float64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.points) == 0 {
		return 0
	}
	return m.points[len(m.points)-1].RSSMB
}

func (m *ProcessMonitor) poll() {
	ticker := time.NewTicker(m.interval)
	defer ticker.Stop()

	var lastRecords int64
	var lastTime = m.start

	for {
		select {
		case <-m.done:
			return
		case now := <-ticker.C:
			rss := getRSSMB(m.pid)
			m.mu.Lock()
			records := m.records
			dt := now.Sub(lastTime).Seconds()
			var rps float64
			if dt > 0 {
				rps = float64(records-lastRecords) / dt
			}
			m.points = append(m.points, TimePoint{
				ElapsedSec: now.Sub(m.start).Seconds(),
				RSSMB:      rss,
				Records:    records,
				RecPerSec:  rps,
			})
			lastRecords = records
			lastTime = now
			m.mu.Unlock()
		}
	}
}

// getRSSMB returns the RSS of a process in MB.
func getRSSMB(pid int) float64 {
	var cmd *exec.Cmd
	if runtime.GOOS == "darwin" {
		// macOS: ps -o rss= returns KB
		cmd = exec.Command("ps", "-o", "rss=", "-p", fmt.Sprintf("%d", pid))
	} else {
		// Linux: ps -o rss= returns KB
		cmd = exec.Command("ps", "-o", "rss=", "-p", fmt.Sprintf("%d", pid))
	}
	out, err := cmd.Output()
	if err != nil {
		return 0
	}
	kb, err := strconv.ParseFloat(strings.TrimSpace(string(out)), 64)
	if err != nil {
		return 0
	}
	return kb / 1024.0
}
