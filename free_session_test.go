package main

import (
	"testing"
	"time"
)

func TestReadySessionRejectsSessionsInsideSafetyWindow(t *testing.T) {
	pool := &tokenPool{
		sessions: map[string]*cachedSession{
			"deepseek/deepseek-v4-pro": {
				status:     sessionStatusActive,
				instanceID: "session-near-expiry",
				expiresAt:  time.Now().Add(90 * time.Second),
			},
		},
	}

	instanceID, ready := pool.readySessionLocked("deepseek/deepseek-v4-pro", time.Now())
	if ready {
		t.Fatalf("near-expiry session was ready with instance ID %q; want refresh", instanceID)
	}
}
