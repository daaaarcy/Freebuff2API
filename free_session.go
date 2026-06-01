package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const (
	freeSessionPollInterval        = 5 * time.Second
	freeSessionRefreshSafetyWindow = 2 * time.Minute
)

type sessionStatus string

const (
	sessionStatusDisabled   sessionStatus = "disabled"
	sessionStatusNone       sessionStatus = "none"
	sessionStatusQueued     sessionStatus = "queued"
	sessionStatusActive     sessionStatus = "active"
	sessionStatusEnded      sessionStatus = "ended"
	sessionStatusSuperseded sessionStatus = "superseded"
)

type freeSessionResponse struct {
	Status                 string `json:"status"`
	InstanceID             string `json:"instanceId"`
	Position               int    `json:"position"`
	QueueDepth             int    `json:"queueDepth"`
	QueuedAt               string `json:"queuedAt"`
	ExpiresAt              string `json:"expiresAt"`
	RemainingMs            int64  `json:"remainingMs"`
	EstimatedWaitMs        int64  `json:"estimatedWaitMs"`
	GracePeriodRemainingMs int64  `json:"gracePeriodRemainingMs"`
	Message                string `json:"message"`
}

type cachedSession struct {
	status     sessionStatus
	instanceID string
	expiresAt  time.Time
	position   int
	queueDepth int
	pollAt     time.Time
	retryAfter time.Duration
}

func (p *tokenPool) ensureSession(ctx context.Context, model string) (string, error) {
	for {
		p.mu.Lock()
		if instanceID, ready := p.readySessionLocked(model, time.Now()); ready {
			p.mu.Unlock()
			return instanceID, nil
		}
		session := p.sessions[model]
		if waitingErr := waitingRoomErrorFromSession(p.name, session, time.Now()); waitingErr != nil {
			p.mu.Unlock()
			return "", waitingErr
		}
		if ch := p.sessionRefreshCh[model]; ch != nil {
			p.mu.Unlock()
			select {
			case <-ctx.Done():
				return "", ctx.Err()
			case <-ch:
				continue
			}
		}
		ch := make(chan struct{})
		p.sessionRefreshCh[model] = ch
		p.mu.Unlock()

		newSession, instanceID, err := p.refreshSession(ctx, model)

		p.mu.Lock()
		if newSession != nil {
			p.sessions[model] = newSession
		}
		if err != nil {
			delete(p.sessions, model)
			p.lastError = err.Error()
		} else if waitingErr := waitingRoomErrorFromSession(p.name, newSession, time.Now()); waitingErr != nil {
			p.lastError = waitingErr.Error()
		} else {
			p.lastError = ""
		}
		close(p.sessionRefreshCh[model])
		delete(p.sessionRefreshCh, model)
		p.mu.Unlock()

		if err == nil {
			if waitingErr := waitingRoomErrorFromSession(p.name, newSession, time.Now()); waitingErr != nil {
				return "", waitingErr
			}
		}
		return instanceID, err
	}
}

func (p *tokenPool) readySessionLocked(model string, now time.Time) (string, bool) {
	session := p.sessions[model]
	if session == nil {
		return "", false
	}
	switch session.status {
	case sessionStatusDisabled:
		return "", true
	case sessionStatusActive:
		if session.instanceID == "" {
			return "", false
		}
		if session.expiresAt.IsZero() || now.Before(session.expiresAt.Add(-freeSessionRefreshSafetyWindow)) {
			return session.instanceID, true
		}
	}
	return "", false
}

func (p *tokenPool) refreshSession(ctx context.Context, model string) (*cachedSession, string, error) {
	// End existing session before creating a new one for a different model.
	// The upstream binds sessions to models; creating without ending reuses the old binding.
	if err := p.client.EndSession(ctx, p.token); err != nil {
		p.logger.Printf("%s: end session before refresh (continuing): %v", p.name, err)
	}

	// Pass the model so the upstream creates a session for the correct model
	state, err := p.client.CreateOrRefreshSession(ctx, p.token, model)
	if err != nil {
		return nil, "", fmt.Errorf("start free session: %w", err)
	}

	for {
		switch sessionStatus(strings.TrimSpace(state.Status)) {
		case sessionStatusDisabled:
			return &cachedSession{status: sessionStatusDisabled}, "", nil
		case sessionStatusActive:
			instanceID := strings.TrimSpace(state.InstanceID)
			if instanceID == "" {
				return nil, "", fmt.Errorf("free session active response missing instanceId")
			}
			expiresAt, err := parseOptionalTime(state.ExpiresAt)
			if err != nil {
				return nil, "", fmt.Errorf("parse free session expiry: %w", err)
			}
			return &cachedSession{
				status:     sessionStatusActive,
				instanceID: instanceID,
				expiresAt:  expiresAt,
			}, instanceID, nil
		case sessionStatusQueued:
			instanceID := strings.TrimSpace(state.InstanceID)
			if instanceID == "" {
				return nil, "", fmt.Errorf("free session queued response missing instanceId")
			}
			p.logQueuePosition(state)
			delay := queuedPollDelay(state)
			return &cachedSession{
				status:     sessionStatusQueued,
				instanceID: instanceID,
				position:   maxInt(state.Position, 1),
				queueDepth: maxInt(state.QueueDepth, maxInt(state.Position, 1)),
				pollAt:     time.Now().Add(delay),
				retryAfter: delay,
			}, "", nil
		case sessionStatusNone, sessionStatusEnded, sessionStatusSuperseded:
			state, err = p.client.CreateOrRefreshSession(ctx, p.token, model)
			if err != nil {
				return nil, "", fmt.Errorf("refresh free session: %w", err)
			}
		default:
			return nil, "", fmt.Errorf("unexpected free session status %q", state.Status)
		}
	}
}

func (p *tokenPool) invalidateSession(model, reason string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.sessions, model)
	if reason != "" {
		p.lastError = reason
	}
}

func (p *tokenPool) currentSessionInstanceID() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, session := range p.sessions {
		if session != nil {
			return session.instanceID
		}
	}
	return ""
}

func waitingRoomErrorFromSession(token string, session *cachedSession, now time.Time) *waitingRoomError {
	if session == nil || session.status != sessionStatusQueued {
		return nil
	}
	if !session.pollAt.IsZero() && now.Before(session.pollAt) {
		return &waitingRoomError{
			Token:      token,
			Position:   session.position,
			QueueDepth: session.queueDepth,
			RetryAfter: time.Until(session.pollAt),
		}
	}
	return nil
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func (p *tokenPool) logQueuePosition(state freeSessionResponse) {
	var parts []string

	if state.QueueDepth > 0 {
		parts = append(parts, fmt.Sprintf("position %d/%d", state.Position, state.QueueDepth))
	} else if state.Position > 0 {
		parts = append(parts, fmt.Sprintf("position %d", state.Position))
	}

	if state.EstimatedWaitMs > 0 {
		parts = append(parts, "~"+formatWaitDuration(time.Duration(state.EstimatedWaitMs)*time.Millisecond)+" remaining")
	}

	if state.QueuedAt != "" {
		if queuedAt, err := time.Parse(time.RFC3339, state.QueuedAt); err == nil {
			parts = append(parts, "elapsed "+formatElapsedDuration(time.Since(queuedAt)))
		}
	}

	if len(parts) > 0 {
		p.logger.Printf("%s: waiting room: %s", p.name, strings.Join(parts, ", "))
	} else {
		p.logger.Printf("%s: waiting room: queued", p.name)
	}
}

func formatWaitDuration(d time.Duration) string {
	d = d.Round(time.Minute)
	if d < time.Minute {
		return "< 1 min"
	}
	hours := int(d.Hours())
	minutes := int(d.Minutes()) % 60
	if hours > 0 {
		return fmt.Sprintf("%dh %dm", hours, minutes)
	}
	return fmt.Sprintf("%d min", minutes)
}

func formatElapsedDuration(d time.Duration) string {
	d = d.Round(time.Second)
	minutes := int(d.Minutes())
	seconds := int(d.Seconds()) % 60
	if minutes > 0 {
		return fmt.Sprintf("%dm %02ds", minutes, seconds)
	}
	return fmt.Sprintf("%ds", seconds)
}

func (p *tokenPool) endSession(ctx context.Context) error {
	p.mu.Lock()
	// End all active sessions
	var activeSessions []*cachedSession
	for _, session := range p.sessions {
		if session != nil && session.status != sessionStatusDisabled && session.instanceID != "" {
			activeSessions = append(activeSessions, session)
		}
	}
	p.sessions = make(map[string]*cachedSession)
	p.mu.Unlock()

	if len(activeSessions) == 0 {
		return nil
	}
	if err := p.client.EndSession(ctx, p.token); err != nil {
		return fmt.Errorf("end free session: %w", err)
	}
	return nil
}

func (c *UpstreamClient) CreateOrRefreshSession(ctx context.Context, authToken, model string) (freeSessionResponse, error) {
	return c.doSessionRequestWithModel(ctx, http.MethodPost, authToken, "", model)
}

func (c *UpstreamClient) GetSession(ctx context.Context, authToken, instanceID string) (freeSessionResponse, error) {
	return c.doSessionRequest(ctx, http.MethodGet, authToken, instanceID)
}

func (c *UpstreamClient) EndSession(ctx context.Context, authToken string) error {
	requestURL, err := url.JoinPath(c.baseURL, "/api/v1/freebuff/session")
	if err != nil {
		return fmt.Errorf("build free session url: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, requestURL, nil)
	if err != nil {
		return fmt.Errorf("create free session delete request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+authToken)
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", c.userAgent)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("send free session delete request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("free session delete failed with status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return nil
}

func (c *UpstreamClient) doSessionRequest(ctx context.Context, method, authToken, instanceID string) (freeSessionResponse, error) {
	return c.doSessionRequestWithModel(ctx, method, authToken, instanceID, "")
}

func (c *UpstreamClient) doSessionRequestWithModel(ctx context.Context, method, authToken, instanceID, model string) (freeSessionResponse, error) {
	requestURL, err := url.JoinPath(c.baseURL, "/api/v1/freebuff/session")
	if err != nil {
		return freeSessionResponse{}, fmt.Errorf("build free session url: %w", err)
	}

	var body io.Reader
	if method == http.MethodPost {
		body = bytes.NewReader([]byte("{}"))
	}

	req, err := http.NewRequestWithContext(ctx, method, requestURL, body)
	if err != nil {
		return freeSessionResponse{}, fmt.Errorf("create free session request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+authToken)
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", c.userAgent)
	if method == http.MethodPost {
		req.Header.Set("Content-Type", "application/json")
		if model != "" {
			req.Header.Set("x-freebuff-model", model)
		}
	}
	if instanceID != "" {
		req.Header.Set("x-freebuff-instance-id", instanceID)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return freeSessionResponse{}, fmt.Errorf("send free session request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return freeSessionResponse{Status: string(sessionStatusDisabled)}, nil
	}

	responseBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return freeSessionResponse{}, fmt.Errorf("read free session response: %w", err)
	}
	if resp.StatusCode == http.StatusTooManyRequests {
		return freeSessionResponse{}, parseRateLimitError(responseBody)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return freeSessionResponse{}, fmt.Errorf("free session request failed with status %d: %s", resp.StatusCode, strings.TrimSpace(string(responseBody)))
	}

	var parsed freeSessionResponse
	if err := json.Unmarshal(responseBody, &parsed); err != nil {
		return freeSessionResponse{}, fmt.Errorf("decode free session response: %w", err)
	}
	if strings.TrimSpace(parsed.Status) == "" {
		return freeSessionResponse{}, fmt.Errorf("free session response missing status")
	}
	return parsed, nil
}

func parseRateLimitError(body []byte) *rateLimitError {
	var parsed struct {
		Model        string  `json:"model"`
		ResetAt      string  `json:"resetAt"`
		RetryAfterMs int64   `json:"retryAfterMs"`
		Limit        float64 `json:"limit"`
		RecentCount  float64 `json:"recentCount"`
	}
	if err := json.Unmarshal(body, &parsed); err != nil {
		return &rateLimitError{}
	}
	rl := &rateLimitError{
		Model:  parsed.Model,
		Limit:  parsed.Limit,
		Recent: parsed.RecentCount,
	}
	if parsed.ResetAt != "" {
		rl.ResetAt, _ = time.Parse(time.RFC3339, parsed.ResetAt)
	}
	if parsed.RetryAfterMs > 0 {
		rl.RetryAfter = time.Duration(parsed.RetryAfterMs) * time.Millisecond
	}
	return rl
}

func queuedPollDelay(state freeSessionResponse) time.Duration {
	if state.EstimatedWaitMs <= 0 {
		return freeSessionPollInterval
	}
	delay := time.Duration(state.EstimatedWaitMs) * time.Millisecond
	if delay < time.Second {
		return time.Second
	}
	if delay > freeSessionPollInterval {
		return freeSessionPollInterval
	}
	return delay
}

func parseOptionalTime(value string) (time.Time, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return time.Time{}, nil
	}
	return time.Parse(time.RFC3339, value)
}

func sleepWithContext(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		return nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
