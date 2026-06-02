package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"
)

type RunManager struct {
	cfg    Config
	logger *log.Logger
	pools  []*tokenPool

	stopCh chan struct{}
	wg     sync.WaitGroup
}

type tokenPool struct {
	name   string
	token  string
	cfg    Config
	client *UpstreamClient
	logger *log.Logger

	mu                   sync.Mutex
	runs                 map[string]*managedRun // agentID -> current run
	draining             []*managedRun
	session              *cachedSession
	sessionRefreshCh     chan struct{}
	sessionInflight      int
	sessionStartedCounts map[string]int
	lastError            string
	cooldownUntil        time.Time
	modelCooldowns       map[string]modelCooldown
}

type modelCooldown struct {
	Until  time.Time
	Reason string
}

type managedRun struct {
	id           string
	agentID      string
	startedAt    time.Time
	inflight     int
	requestCount int
	finishing    bool
}

type runLease struct {
	pool              *tokenPool
	run               *managedRun
	sessionInstanceID string
}

type tokenSnapshot struct {
	Name                 string                  `json:"name"`
	Runs                 []runSnapshot           `json:"runs"`
	DrainingRuns         int                     `json:"draining_runs"`
	SessionStatus        string                  `json:"session_status,omitempty"`
	SessionModel         string                  `json:"session_model,omitempty"`
	SessionPremium       bool                    `json:"session_premium"`
	SessionInstanceID    string                  `json:"session_instance_id,omitempty"`
	SessionExpiresAt     time.Time               `json:"session_expires_at,omitempty"`
	SessionPosition      int                     `json:"session_position,omitempty"`
	SessionQueueDepth    int                     `json:"session_queue_depth,omitempty"`
	SessionPollAt        time.Time               `json:"session_poll_at,omitempty"`
	SessionInflight      int                     `json:"session_inflight,omitempty"`
	SessionStartedCounts map[string]int          `json:"session_started_counts,omitempty"`
	CooldownUntil        time.Time               `json:"cooldown_until,omitempty"`
	ModelCooldowns       []modelCooldownSnapshot `json:"model_cooldowns,omitempty"`
	LastError            string                  `json:"last_error,omitempty"`
}

type modelCooldownSnapshot struct {
	Model  string    `json:"model"`
	Until  time.Time `json:"until"`
	Reason string    `json:"reason,omitempty"`
}

type runSnapshot struct {
	AgentID      string    `json:"agent_id"`
	RunID        string    `json:"run_id"`
	StartedAt    time.Time `json:"started_at"`
	Inflight     int       `json:"inflight"`
	RequestCount int       `json:"request_count"`
}

type waitingRoomError struct {
	Token      string
	Position   int
	QueueDepth int
	RetryAfter time.Duration
}

type rateLimitError struct {
	Model      string
	ResetAt    time.Time
	RetryAfter time.Duration
	Limit      float64
	Recent     float64
}

type cooldownError struct {
	Until  time.Time
	Model  string
	Reason string
}

func (e *cooldownError) Error() string {
	if strings.TrimSpace(e.Model) != "" {
		return fmt.Sprintf("model %s cooling down until %s", e.Model, e.Until.Format(time.RFC3339))
	}
	return fmt.Sprintf("token cooling down until %s", e.Until.Format(time.RFC3339))
}

func (e *rateLimitError) Error() string {
	msg := fmt.Sprintf("rate limited for %s", e.Model)
	if e.Limit > 0 {
		msg += fmt.Sprintf(" (%.0f/%.0f used)", e.Recent, e.Limit)
	}
	if !e.ResetAt.IsZero() {
		msg += fmt.Sprintf(", resets at %s", e.ResetAt.Format(time.RFC3339))
	}
	return msg
}

func (e *waitingRoomError) Error() string {
	if e == nil {
		return "freebuff waiting room queued"
	}

	message := "freebuff waiting room queued"
	if e.Token != "" {
		message += " for " + e.Token
	}
	if e.Position > 0 {
		if e.QueueDepth >= e.Position {
			message += fmt.Sprintf(" (position %d/%d)", e.Position, e.QueueDepth)
		} else {
			message += fmt.Sprintf(" (position %d)", e.Position)
		}
	}
	if e.RetryAfter > 0 {
		message += fmt.Sprintf(", retry in about %s", e.RetryAfter.Round(time.Second))
	}
	return message
}

func NewRunManager(cfg Config, client *UpstreamClient, logger *log.Logger) *RunManager {
	pools := make([]*tokenPool, 0, len(cfg.AuthTokens))
	for index, token := range cfg.AuthTokens {
		pools = append(pools, &tokenPool{
			name:                 fmt.Sprintf("token-%d", index+1),
			token:                token,
			cfg:                  cfg,
			client:               client,
			runs:                 make(map[string]*managedRun),
			sessionStartedCounts: make(map[string]int),
			modelCooldowns:       make(map[string]modelCooldown),
			logger:               logger,
		})
	}

	return &RunManager{
		cfg:    cfg,
		logger: logger,
		pools:  pools,
		stopCh: make(chan struct{}),
	}
}

func (m *RunManager) Start(ctx context.Context, agentIDs []string) {
	// Pre-warm runs for all free agents in background.
	// The server is already listening; if a request arrives before
	// pre-warming finishes, acquire() will lazily create the run.
	go m.prewarm(agentIDs)

	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		ticker := time.NewTicker(time.Minute)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				maintainCtx, cancel := context.WithTimeout(context.Background(), m.cfg.RequestTimeout)
				for _, pool := range m.pools {
					if err := pool.maintain(maintainCtx); err != nil {
						m.logger.Printf("%s: maintenance failed: %v", pool.name, err)
					}
				}
				cancel()
			case <-m.stopCh:
				return
			}
		}
	}()
}

func (m *RunManager) prewarm(agentIDs []string) {
	ctx, cancel := context.WithTimeout(context.Background(), m.cfg.RequestTimeout)
	defer cancel()

	// Only prewarm the first pool. Backup pools activate on demand
	// so we don't burn sessions on tokens that may not be needed.
	pool := m.pools[0]
	for _, agentID := range agentIDs {
		if err := pool.rotateAgent(ctx, agentID); err != nil {
			m.logger.Printf("%s: prewarm %s failed: %v", pool.name, agentID, err)
		} else {
			m.logger.Printf("%s: prewarmed %s", pool.name, agentID)
		}
	}
}

func (m *RunManager) Close(ctx context.Context) {
	close(m.stopCh)
	m.wg.Wait()
	for _, pool := range m.pools {
		if err := pool.shutdown(ctx); err != nil {
			m.logger.Printf("%s: shutdown failed: %v", pool.name, err)
		}
	}
}

func (m *RunManager) Acquire(ctx context.Context, agentID, model string) (*runLease, string, error) {
	if len(m.pools) == 0 {
		return nil, "", errors.New("no auth tokens configured")
	}

	// Failover: try pools in order, fall through on rate limit or cooldown.
	var errs []string
	var waiting []*waitingRoomError
	var rateLimits []*rateLimitError
	var cooldowns []*cooldownError
	for _, pool := range m.pools {
		lease, sessionInstanceID, err := pool.acquire(ctx, agentID, model)
		if err == nil {
			return lease, sessionInstanceID, nil
		}
		var waitingErr *waitingRoomError
		if errors.As(err, &waitingErr) {
			waiting = append(waiting, waitingErr)
			errs = append(errs, fmt.Sprintf("%s: %v", pool.name, err))
			continue
		}
		var cooldownErr *cooldownError
		if errors.As(err, &cooldownErr) {
			cooldowns = append(cooldowns, cooldownErr)
			errs = append(errs, fmt.Sprintf("%s: %v", pool.name, err))
			continue
		}
		var rateErr *rateLimitError
		if errors.As(err, &rateErr) {
			rateLimits = append(rateLimits, rateErr)
			errs = append(errs, fmt.Sprintf("%s: %v", pool.name, err))
			continue
		}
		// Cooldown or other transient errors — also try next pool.
		errs = append(errs, fmt.Sprintf("%s: %v", pool.name, err))
	}

	transitionErrors := len(waiting) + len(rateLimits) + len(cooldowns)
	if len(waiting) > 0 && transitionErrors == len(m.pools) {
		return nil, "", bestWaitingRoomError(waiting)
	}

	if len(rateLimits) > 0 && transitionErrors == len(m.pools) {
		return nil, "", bestRateLimitError(rateLimits)
	}

	if len(cooldowns) > 0 && transitionErrors == len(m.pools) {
		if cooldown := bestModelCooldownError(cooldowns); cooldown != nil {
			return nil, "", cooldown
		}
	}

	return nil, "", fmt.Errorf("unable to acquire run from any token (%s)", strings.Join(errs, "; "))
}

func (m *RunManager) Release(lease *runLease) {
	if lease == nil || lease.pool == nil || lease.run == nil {
		return
	}
	lease.pool.release(lease)
}

func (m *RunManager) Invalidate(lease *runLease, reason string) {
	if lease == nil || lease.pool == nil || lease.run == nil {
		return
	}
	lease.pool.invalidate(lease.run, reason)
}

func (m *RunManager) Cooldown(lease *runLease, duration time.Duration, reason string) {
	if lease == nil || lease.pool == nil {
		return
	}
	lease.pool.markCooldown(duration, reason)
}

func (m *RunManager) CooldownModel(lease *runLease, model string, duration time.Duration, reason string) {
	if lease == nil || lease.pool == nil {
		return
	}
	lease.pool.markModelCooldown(model, duration, reason)
}

func (m *RunManager) Snapshots() []tokenSnapshot {
	snapshots := make([]tokenSnapshot, 0, len(m.pools))
	for _, pool := range m.pools {
		snapshots = append(snapshots, pool.snapshot())
	}
	return snapshots
}

func (p *tokenPool) acquire(ctx context.Context, agentID, model string) (*runLease, string, error) {
	p.mu.Lock()
	now := time.Now()
	if now.Before(p.cooldownUntil) {
		cooldownUntil := p.cooldownUntil
		p.mu.Unlock()
		return nil, "", &cooldownError{Until: cooldownUntil}
	}
	if cooldown, ok := p.modelCooldownLocked(model, now); ok {
		p.mu.Unlock()
		return nil, "", &cooldownError{Until: cooldown.Until, Model: strings.TrimSpace(model), Reason: cooldown.Reason}
	}
	run := p.runs[agentID]
	needsRotate := run == nil || time.Since(run.startedAt) >= p.cfg.RotationInterval
	p.mu.Unlock()

	if needsRotate {
		if err := p.rotateAgent(ctx, agentID); err != nil {
			return nil, "", err
		}
	}

	var sessionInstanceID string
	if p.cfg.RequiresFreeSession(model) {
		var err error
		sessionInstanceID, err = p.ensureSession(ctx, model)
		if err != nil {
			var rateErr *rateLimitError
			if errors.As(err, &rateErr) && rateErr.RetryAfter > 0 {
				p.markModelCooldown(model, rateErr.RetryAfter, fmt.Sprintf("rate limited for %s", model))
			}
			return nil, "", err
		}
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	run = p.runs[agentID]
	if run == nil {
		return nil, "", errors.New("run missing after rotation")
	}
	run.inflight++
	run.requestCount++
	if sessionInstanceID != "" {
		p.sessionInflight++
	}
	return &runLease{pool: p, run: run, sessionInstanceID: sessionInstanceID}, sessionInstanceID, nil
}

func bestWaitingRoomError(waiting []*waitingRoomError) *waitingRoomError {
	if len(waiting) == 0 {
		return nil
	}
	best := waiting[0]
	for _, candidate := range waiting[1:] {
		if candidate == nil {
			continue
		}
		if best == nil {
			best = candidate
			continue
		}
		if candidate.Position > 0 && (best.Position == 0 || candidate.Position < best.Position) {
			best = candidate
			continue
		}
		if candidate.Position == best.Position && candidate.RetryAfter > 0 && (best.RetryAfter == 0 || candidate.RetryAfter < best.RetryAfter) {
			best = candidate
		}
	}
	return best
}

func bestModelCooldownError(cooldowns []*cooldownError) *cooldownError {
	var best *cooldownError
	for _, candidate := range cooldowns {
		if candidate == nil || strings.TrimSpace(candidate.Model) == "" {
			continue
		}
		if best == nil || candidate.Until.Before(best.Until) {
			copy := *candidate
			best = &copy
		}
	}
	return best
}

func bestRateLimitError(rateLimits []*rateLimitError) *rateLimitError {
	if len(rateLimits) == 0 {
		return nil
	}
	best := rateLimits[0]
	for _, candidate := range rateLimits[1:] {
		if candidate == nil {
			continue
		}
		if best == nil {
			best = candidate
			continue
		}
		if !candidate.ResetAt.IsZero() && (best.ResetAt.IsZero() || candidate.ResetAt.Before(best.ResetAt)) {
			best = candidate
			continue
		}
		if candidate.ResetAt.Equal(best.ResetAt) && candidate.RetryAfter > 0 && (best.RetryAfter == 0 || candidate.RetryAfter < best.RetryAfter) {
			best = candidate
		}
	}
	return best
}

func (p *tokenPool) maintain(ctx context.Context) error {
	p.mu.Lock()
	hasSession := p.session != nil
	sessionModel := ""
	if p.session != nil {
		sessionModel = p.session.model
	}
	p.mu.Unlock()
	if hasSession {
		if _, err := p.ensureSession(ctx, sessionModel); err != nil {
			var busyErr *sessionBusyError
			if !errors.As(err, &busyErr) {
				p.logger.Printf("%s: refresh free session failed: %v", p.name, err)
			}
		}
	}

	p.mu.Lock()
	var toRotate []string
	for agentID, run := range p.runs {
		if time.Since(run.startedAt) >= p.cfg.RotationInterval {
			toRotate = append(toRotate, agentID)
		}
	}
	draining := append([]*managedRun(nil), p.draining...)
	p.mu.Unlock()

	for _, agentID := range toRotate {
		if err := p.rotateAgent(ctx, agentID); err != nil {
			p.logger.Printf("%s: rotate agent %s failed: %v", p.name, agentID, err)
		}
	}

	for _, run := range draining {
		if err := p.finishIfReady(run); err != nil {
			p.logger.Printf("%s: finish draining run %s failed: %v", p.name, run.id, err)
		}
	}
	return nil
}

func (p *tokenPool) shutdown(ctx context.Context) error {
	p.mu.Lock()
	var allRuns []*managedRun
	for _, run := range p.runs {
		allRuns = append(allRuns, run)
	}
	allRuns = append(allRuns, p.draining...)
	p.runs = make(map[string]*managedRun)
	p.draining = nil
	p.mu.Unlock()

	var errs []string
	for _, run := range allRuns {
		if err := p.client.FinishRun(ctx, p.token, run.id, run.requestCount); err != nil {
			errs = append(errs, err.Error())
		}
	}
	if err := p.endSession(ctx); err != nil {
		errs = append(errs, err.Error())
	}
	if len(errs) > 0 {
		return errors.New(strings.Join(errs, "; "))
	}
	return nil
}

func (p *tokenPool) rotateAgent(ctx context.Context, agentID string) error {
	p.mu.Lock()
	if now := time.Now(); now.Before(p.cooldownUntil) {
		cooldownUntil := p.cooldownUntil
		p.mu.Unlock()
		return &cooldownError{Until: cooldownUntil}
	}
	p.mu.Unlock()

	runID, err := p.client.StartRun(ctx, p.token, agentID)
	if err != nil {
		p.mu.Lock()
		p.lastError = err.Error()
		p.mu.Unlock()
		return err
	}

	p.mu.Lock()
	oldRun := p.runs[agentID]
	p.runs[agentID] = &managedRun{
		id:        runID,
		agentID:   agentID,
		startedAt: time.Now(),
	}
	p.lastError = ""
	if oldRun != nil {
		p.draining = append(p.draining, oldRun)
	}
	p.mu.Unlock()

	if oldRun != nil {
		go func(run *managedRun) {
			if err := p.finishIfReady(run); err != nil {
				p.logger.Printf("%s: finish rotated run %s (agent %s) failed: %v", p.name, run.id, run.agentID, err)
			}
		}(oldRun)
	}
	return nil
}

func (p *tokenPool) release(lease *runLease) {
	if lease == nil || lease.run == nil {
		return
	}

	p.mu.Lock()
	run := lease.run
	if run.inflight > 0 {
		run.inflight--
	}
	if lease.sessionInstanceID != "" &&
		p.session != nil &&
		p.session.instanceID == lease.sessionInstanceID &&
		p.sessionInflight > 0 {
		p.sessionInflight--
	}
	p.mu.Unlock()

	if err := p.finishIfReady(run); err != nil {
		p.logger.Printf("%s: finish released run %s failed: %v", p.name, run.id, err)
	}
}

func (p *tokenPool) finishIfReady(run *managedRun) error {
	p.mu.Lock()
	if run == nil || run.inflight > 0 || run.finishing {
		p.mu.Unlock()
		return nil
	}
	// Only finish if this run is no longer the current run for its agent
	if current, ok := p.runs[run.agentID]; ok && current == run {
		p.mu.Unlock()
		return nil
	}
	run.finishing = true
	p.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), p.cfg.RequestTimeout)
	defer cancel()

	if err := p.client.FinishRun(ctx, p.token, run.id, run.requestCount); err != nil {
		p.mu.Lock()
		run.finishing = false
		p.lastError = err.Error()
		p.mu.Unlock()
		return err
	}

	p.mu.Lock()
	filtered := p.draining[:0]
	for _, drainingRun := range p.draining {
		if drainingRun != run {
			filtered = append(filtered, drainingRun)
		}
	}
	p.draining = filtered
	p.mu.Unlock()
	return nil
}

func (p *tokenPool) invalidate(run *managedRun, reason string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Remove from current runs if it matches
	if current, ok := p.runs[run.agentID]; ok && current == run {
		delete(p.runs, run.agentID)
	}

	filtered := p.draining[:0]
	for _, drainingRun := range p.draining {
		if drainingRun != run {
			filtered = append(filtered, drainingRun)
		}
	}
	p.draining = filtered
	if reason != "" {
		p.lastError = reason
	}
}

func (p *tokenPool) markCooldown(duration time.Duration, reason string) {
	if duration <= 0 {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.cooldownUntil = time.Now().Add(duration)
	if reason != "" {
		p.lastError = reason
	}
}

func (p *tokenPool) markModelCooldown(model string, duration time.Duration, reason string) {
	model = strings.TrimSpace(model)
	if model == "" || duration <= 0 {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.modelCooldowns == nil {
		p.modelCooldowns = make(map[string]modelCooldown)
	}
	p.modelCooldowns[model] = modelCooldown{
		Until:  time.Now().Add(duration),
		Reason: reason,
	}
	if reason != "" {
		p.lastError = reason
	}
}

func (p *tokenPool) modelCooldownLocked(model string, now time.Time) (modelCooldown, bool) {
	model = strings.TrimSpace(model)
	if model == "" || len(p.modelCooldowns) == 0 {
		return modelCooldown{}, false
	}
	cooldown, ok := p.modelCooldowns[model]
	if !ok {
		return modelCooldown{}, false
	}
	if cooldown.Until.IsZero() || now.Before(cooldown.Until) {
		return cooldown, true
	}
	delete(p.modelCooldowns, model)
	return modelCooldown{}, false
}

func (p *tokenPool) snapshot() tokenSnapshot {
	p.mu.Lock()
	defer p.mu.Unlock()

	snapshot := tokenSnapshot{
		Name:          p.name,
		DrainingRuns:  len(p.draining),
		CooldownUntil: p.cooldownUntil,
		LastError:     p.lastError,
	}
	now := time.Now()
	for model, cooldown := range p.modelCooldowns {
		if !cooldown.Until.IsZero() && now.After(cooldown.Until) {
			delete(p.modelCooldowns, model)
			continue
		}
		snapshot.ModelCooldowns = append(snapshot.ModelCooldowns, modelCooldownSnapshot{
			Model:  model,
			Until:  cooldown.Until,
			Reason: cooldown.Reason,
		})
	}
	if p.session != nil {
		snapshot.SessionStatus = string(p.session.status)
		snapshot.SessionModel = p.session.model
		snapshot.SessionPremium = p.cfg.RequiresPremiumSession(p.session.model)
		snapshot.SessionInstanceID = p.session.instanceID
		snapshot.SessionExpiresAt = p.session.expiresAt
		snapshot.SessionPosition = p.session.position
		snapshot.SessionQueueDepth = p.session.queueDepth
		snapshot.SessionPollAt = p.session.pollAt
		snapshot.SessionInflight = p.sessionInflight
	}
	if len(p.sessionStartedCounts) > 0 {
		snapshot.SessionStartedCounts = make(map[string]int, len(p.sessionStartedCounts))
		for model, count := range p.sessionStartedCounts {
			snapshot.SessionStartedCounts[model] = count
		}
	}
	for agentID, run := range p.runs {
		snapshot.Runs = append(snapshot.Runs, runSnapshot{
			AgentID:      agentID,
			RunID:        run.id,
			StartedAt:    run.startedAt,
			Inflight:     run.inflight,
			RequestCount: run.requestCount,
		})
	}
	return snapshot
}
