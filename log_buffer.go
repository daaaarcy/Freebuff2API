package main

import (
	"strings"
	"sync"
)

const defaultDashboardLogLines = 500

type logBuffer struct {
	mu    sync.Mutex
	limit int
	lines []string
}

func newLogBuffer(limit int) *logBuffer {
	if limit <= 0 {
		limit = defaultDashboardLogLines
	}
	return &logBuffer{limit: limit}
}

func (b *logBuffer) Write(p []byte) (int, error) {
	if b == nil {
		return len(p), nil
	}
	for _, line := range strings.Split(string(p), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		b.append(line)
	}
	return len(p), nil
}

func (b *logBuffer) append(line string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.lines = append(b.lines, line)
	if overflow := len(b.lines) - b.limit; overflow > 0 {
		copy(b.lines, b.lines[overflow:])
		b.lines = b.lines[:b.limit]
	}
}

func (b *logBuffer) Lines() []string {
	if b == nil {
		return nil
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]string, len(b.lines))
	copy(out, b.lines)
	return out
}
