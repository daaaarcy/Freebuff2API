package main

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestReadySessionRejectsSessionsInsideSafetyWindow(t *testing.T) {
	pool := &tokenPool{
		session: &cachedSession{
			status:     sessionStatusActive,
			instanceID: "session-near-expiry",
			expiresAt:  time.Now().Add(90 * time.Second),
		},
	}

	instanceID, ready := pool.readySessionLocked("deepseek/deepseek-v4-pro", time.Now())
	if ready {
		t.Fatalf("near-expiry session was ready with instance ID %q; want refresh", instanceID)
	}
}

func TestEnsureSessionRejectsRefreshWhilePremiumSessionInflight(t *testing.T) {
	pool := &tokenPool{
		name: "token-1",
		session: &cachedSession{
			status:     sessionStatusActive,
			instanceID: "session-near-expiry",
			expiresAt:  time.Now().Add(90 * time.Second),
		},
		sessionInflight: 2,
	}

	instanceID, err := pool.ensureSession(context.Background(), "deepseek/deepseek-v4-pro")
	if instanceID != "" {
		t.Fatalf("instanceID = %q, want empty while refresh is blocked", instanceID)
	}
	var busyErr *sessionBusyError
	if !errors.As(err, &busyErr) {
		t.Fatalf("err = %v, want sessionBusyError", err)
	}
	if busyErr.Inflight != 2 {
		t.Fatalf("busy inflight = %d, want 2", busyErr.Inflight)
	}
}

func TestEnsureSessionRejectsMismatchedFlashWhilePremiumSessionInflight(t *testing.T) {
	pool := &tokenPool{
		name: "token-1",
		session: &cachedSession{
			status:     sessionStatusActive,
			instanceID: "session-pro",
			model:      "deepseek/deepseek-v4-pro",
			expiresAt:  time.Now().Add(time.Hour),
		},
		sessionInflight: 1,
	}

	instanceID, err := pool.ensureSession(context.Background(), "deepseek/deepseek-v4-flash")
	if instanceID != "" {
		t.Fatalf("instanceID = %q, want empty for mismatched in-flight pro session", instanceID)
	}
	var lockedErr *sessionModelLockedError
	if !errors.As(err, &lockedErr) {
		t.Fatalf("err = %v, want sessionModelLockedError", err)
	}
}

func TestEnsureSessionRejectsDifferentPremiumModelWhileSessionInflight(t *testing.T) {
	pool := &tokenPool{
		name: "token-1",
		session: &cachedSession{
			status:     sessionStatusActive,
			instanceID: "session-pro",
			model:      "deepseek/deepseek-v4-pro",
			expiresAt:  time.Now().Add(time.Hour),
		},
		sessionInflight: 1,
	}

	instanceID, err := pool.ensureSession(context.Background(), "moonshotai/kimi-k2.6")
	if instanceID != "" {
		t.Fatalf("instanceID = %q, want empty for mismatched premium in-flight session", instanceID)
	}
	var busyErr *sessionBusyError
	if !errors.As(err, &busyErr) {
		t.Fatalf("err = %v, want sessionBusyError", err)
	}
}

func TestReleaseOldSessionLeaseDoesNotDecrementReplacementSession(t *testing.T) {
	pool := &tokenPool{
		runs: map[string]*managedRun{},
		session: &cachedSession{
			status:     sessionStatusActive,
			instanceID: "new-session",
			expiresAt:  time.Now().Add(time.Hour),
		},
		sessionInflight: 1,
	}
	run := &managedRun{
		id:      "run-1",
		agentID: "base2-free-deepseek",
	}
	pool.runs[run.agentID] = run
	oldLease := &runLease{
		pool:              pool,
		run:               run,
		sessionInstanceID: "old-session",
	}

	pool.release(oldLease)

	if pool.sessionInflight != 1 {
		t.Fatalf("sessionInflight = %d, want replacement session count preserved", pool.sessionInflight)
	}
}
