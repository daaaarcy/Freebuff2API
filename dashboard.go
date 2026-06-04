package main

import (
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"
)

//go:embed web/dashboard/index.html web/dashboard/style.css web/dashboard/app.js
var dashboardAssets embed.FS

const dashboardSessionQuotaLimit24h = 5

type dashboardState struct {
	OK          bool                  `json:"ok"`
	RefreshedAt time.Time             `json:"refreshed_at"`
	Service     dashboardServiceState `json:"service"`
	Config      dashboardConfigState  `json:"config"`
	Models      []dashboardModelState `json:"models"`
	Tokens      []dashboardTokenState `json:"tokens"`
	Logs        []string              `json:"logs,omitempty"`
}

type dashboardServiceState struct {
	StartedAt               time.Time `json:"started_at"`
	UptimeSec               int       `json:"uptime_sec"`
	SessionTransitionPeriod string    `json:"session_transition_period"`
	TokenCount              int       `json:"token_count"`
	ActiveSessions          int       `json:"active_sessions"`
	TransitioningSessions   int       `json:"transitioning_sessions"`
	CoolingTokens           int       `json:"cooling_tokens"`
	TotalRuns               int       `json:"total_runs"`
	TotalInflight           int       `json:"total_inflight"`
	TotalRequests           int       `json:"total_requests"`
}

type dashboardConfigState struct {
	ListenAddr              string                   `json:"listen_addr"`
	UpstreamBaseURL         string                   `json:"upstream_base_url"`
	UserAgent               string                   `json:"user_agent"`
	AuthTokenCount          int                      `json:"auth_token_count"`
	AuthTokens              []dashboardTokenIdentity `json:"auth_tokens"`
	APIKeyAuthEnabled       bool                     `json:"api_key_auth_enabled"`
	APIKeyCount             int                      `json:"api_key_count"`
	HTTPProxyConfigured     bool                     `json:"http_proxy_configured"`
	RotationInterval        string                   `json:"rotation_interval"`
	RequestTimeout          string                   `json:"request_timeout"`
	SessionTransitionPeriod string                   `json:"session_transition_period"`
	SessionRequiredModels   []string                 `json:"session_required_models"`
	PremiumSessionModels    []string                 `json:"premium_session_models"`
}

type dashboardTokenIdentity struct {
	Name        string `json:"name"`
	Fingerprint string `json:"fingerprint"`
}

type dashboardModelState struct {
	ID              string `json:"id"`
	AgentID         string `json:"agent_id,omitempty"`
	SessionRequired bool   `json:"session_required"`
	Premium         bool   `json:"premium"`
}

type dashboardTokenState struct {
	Name                        string                  `json:"name"`
	Fingerprint                 string                  `json:"fingerprint"`
	State                       string                  `json:"state"`
	RunCount                    int                     `json:"run_count"`
	TotalRequests               int                     `json:"total_requests"`
	Inflight                    int                     `json:"inflight"`
	DrainingRuns                int                     `json:"draining_runs"`
	CooldownUntil               time.Time               `json:"cooldown_until,omitempty"`
	Cooling                     bool                    `json:"cooling"`
	ModelCooldowns              []modelCooldownSnapshot `json:"model_cooldowns,omitempty"`
	LastError                   string                  `json:"last_error,omitempty"`
	Session                     dashboardSessionState   `json:"session"`
	Runs                        []dashboardRunState     `json:"runs"`
	StartedCounts               map[string]int          `json:"session_started_counts,omitempty"`
	SessionStartsLast24h        int                     `json:"session_starts_last_24h"`
	SessionStartsLast24hByModel map[string]int          `json:"session_starts_last_24h_by_model,omitempty"`
	SessionQuotaLimit24h        int                     `json:"session_quota_limit_24h"`
	SessionSlot24h              int                     `json:"session_slot_24h"`
	SessionSlotsRemaining24h    int                     `json:"session_slots_remaining_24h"`
	SessionUsageLabel           string                  `json:"session_usage_label"`
}

type dashboardSessionState struct {
	Status         string    `json:"status,omitempty"`
	Model          string    `json:"model,omitempty"`
	Premium        bool      `json:"premium"`
	InstanceID     string    `json:"instance_id,omitempty"`
	ExpiresAt      time.Time `json:"expires_at,omitempty"`
	RemainingMs    int64     `json:"remaining_ms,omitempty"`
	Transitioning  bool      `json:"transitioning"`
	TransitionMode string    `json:"transition_mode,omitempty"`
	Position       int       `json:"position,omitempty"`
	QueueDepth     int       `json:"queue_depth,omitempty"`
	PollAt         time.Time `json:"poll_at,omitempty"`
	Inflight       int       `json:"inflight,omitempty"`
}

type dashboardRunState struct {
	AgentID      string    `json:"agent_id"`
	RunID        string    `json:"run_id"`
	StartedAt    time.Time `json:"started_at"`
	AgeSec       int       `json:"age_sec"`
	Inflight     int       `json:"inflight"`
	RequestCount int       `json:"request_count"`
}

func (s *Server) handleDashboard(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/dashboard", "/dashboard/":
		if r.Method != http.MethodGet {
			writeOpenAIError(w, http.StatusMethodNotAllowed, "method not allowed", "invalid_request_error", "")
			return
		}
		serveDashboardAsset(w, r, "web/dashboard/index.html", "text/html; charset=utf-8")
	case "/dashboard/style.css":
		if r.Method != http.MethodGet {
			writeOpenAIError(w, http.StatusMethodNotAllowed, "method not allowed", "invalid_request_error", "")
			return
		}
		serveDashboardAsset(w, r, "web/dashboard/style.css", "text/css; charset=utf-8")
	case "/dashboard/app.js":
		if r.Method != http.MethodGet {
			writeOpenAIError(w, http.StatusMethodNotAllowed, "method not allowed", "invalid_request_error", "")
			return
		}
		serveDashboardAsset(w, r, "web/dashboard/app.js", "application/javascript; charset=utf-8")
	case "/dashboard/api/state":
		s.handleDashboardState(w, r)
	default:
		http.NotFound(w, r)
	}
}

func serveDashboardAsset(w http.ResponseWriter, r *http.Request, assetPath, contentType string) {
	content, err := dashboardAssets.ReadFile(assetPath)
	if err != nil {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", contentType)
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(content)
}

func (s *Server) handleDashboardState(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeOpenAIError(w, http.StatusMethodNotAllowed, "method not allowed", "invalid_request_error", "")
		return
	}
	writeJSON(w, http.StatusOK, s.dashboardState())
}

func (s *Server) dashboardState() dashboardState {
	now := time.Now()
	snapshots := s.runs.Snapshots()
	models := s.registry.Models()

	state := dashboardState{
		OK:          true,
		RefreshedAt: now.UTC(),
		Service: dashboardServiceState{
			StartedAt:               s.started.UTC(),
			UptimeSec:               int(now.Sub(s.started).Seconds()),
			SessionTransitionPeriod: s.cfg.SessionTransitionPeriod.String(),
			TokenCount:              len(snapshots),
		},
		Config: dashboardConfigState{
			ListenAddr:              s.cfg.ListenAddr,
			UpstreamBaseURL:         s.cfg.UpstreamBaseURL,
			UserAgent:               s.cfg.UserAgent,
			AuthTokenCount:          len(s.cfg.AuthTokens),
			AuthTokens:              dashboardTokenIdentities(s.cfg.AuthTokens),
			APIKeyAuthEnabled:       len(s.cfg.APIKeys) > 0,
			APIKeyCount:             len(s.cfg.APIKeys),
			HTTPProxyConfigured:     strings.TrimSpace(s.cfg.HTTPProxy) != "",
			RotationInterval:        s.cfg.RotationInterval.String(),
			RequestTimeout:          s.cfg.RequestTimeout.String(),
			SessionTransitionPeriod: s.cfg.SessionTransitionPeriod.String(),
			SessionRequiredModels:   append([]string(nil), s.cfg.SessionRequiredModels...),
			PremiumSessionModels:    append([]string(nil), s.cfg.PremiumSessionModels...),
		},
		Models: dashboardModels(models, s.cfg, s.registry),
		Tokens: dashboardTokens(snapshots, s.cfg.AuthTokens, now),
		Logs:   s.logs.Lines(),
	}

	for _, token := range state.Tokens {
		if token.Session.Status == string(sessionStatusActive) {
			state.Service.ActiveSessions++
		}
		if token.Session.Transitioning {
			state.Service.TransitioningSessions++
		}
		if token.Cooling || len(token.ModelCooldowns) > 0 {
			state.Service.CoolingTokens++
		}
		state.Service.TotalRuns += token.RunCount
		state.Service.TotalInflight += token.Inflight
		state.Service.TotalRequests += token.TotalRequests
	}
	return state
}

func dashboardTokenIdentities(tokens []string) []dashboardTokenIdentity {
	identities := make([]dashboardTokenIdentity, 0, len(tokens))
	for index, token := range tokens {
		identities = append(identities, dashboardTokenIdentity{
			Name:        tokenName(index),
			Fingerprint: tokenFingerprint(token),
		})
	}
	return identities
}

func dashboardModels(models []string, cfg Config, registry *ModelRegistry) []dashboardModelState {
	output := make([]dashboardModelState, 0, len(models))
	for _, model := range models {
		agentID := ""
		if registry != nil {
			agentID, _ = registry.AgentForModel(model)
		}
		output = append(output, dashboardModelState{
			ID:              model,
			AgentID:         agentID,
			SessionRequired: cfg.RequiresFreeSession(model),
			Premium:         cfg.RequiresPremiumSession(model),
		})
	}
	sort.Slice(output, func(i, j int) bool {
		return output[i].ID < output[j].ID
	})
	return output
}

func dashboardTokens(snapshots []tokenSnapshot, authTokens []string, now time.Time) []dashboardTokenState {
	output := make([]dashboardTokenState, 0, len(snapshots))
	for index, snapshot := range snapshots {
		token := dashboardTokenState{
			Name:                        snapshot.Name,
			Fingerprint:                 fingerprintForTokenIndex(authTokens, index),
			RunCount:                    len(snapshot.Runs),
			DrainingRuns:                snapshot.DrainingRuns,
			CooldownUntil:               snapshot.CooldownUntil,
			Cooling:                     !snapshot.CooldownUntil.IsZero() && now.Before(snapshot.CooldownUntil),
			ModelCooldowns:              append([]modelCooldownSnapshot(nil), snapshot.ModelCooldowns...),
			LastError:                   snapshot.LastError,
			StartedCounts:               copyStringIntMap(snapshot.SessionStartedCounts),
			SessionStartsLast24h:        snapshot.SessionStartsLast24h,
			SessionStartsLast24hByModel: copyStringIntMap(snapshot.SessionStartsLast24hByModel),
			SessionQuotaLimit24h:        dashboardSessionQuotaLimit24h,
			SessionSlot24h:              snapshot.SessionStartsLast24h,
			SessionSlotsRemaining24h:    maxInt(dashboardSessionQuotaLimit24h-snapshot.SessionStartsLast24h, 0),
			SessionUsageLabel:           sessionUsageLabel(snapshot.SessionStartsLast24h, dashboardSessionQuotaLimit24h),
			Session: dashboardSessionState{
				Status:         snapshot.SessionStatus,
				Model:          snapshot.SessionModel,
				Premium:        snapshot.SessionPremium,
				InstanceID:     snapshot.SessionInstanceID,
				ExpiresAt:      snapshot.SessionExpiresAt,
				RemainingMs:    snapshot.SessionRemainingMs,
				Transitioning:  snapshot.SessionTransitioning,
				TransitionMode: snapshot.SessionTransitionMode,
				Position:       snapshot.SessionPosition,
				QueueDepth:     snapshot.SessionQueueDepth,
				PollAt:         snapshot.SessionPollAt,
				Inflight:       snapshot.SessionInflight,
			},
		}
		for _, run := range snapshot.Runs {
			token.TotalRequests += run.RequestCount
			token.Inflight += run.Inflight
			token.Runs = append(token.Runs, dashboardRunState{
				AgentID:      run.AgentID,
				RunID:        shortenID(run.RunID),
				StartedAt:    run.StartedAt,
				AgeSec:       positiveSeconds(now.Sub(run.StartedAt)),
				Inflight:     run.Inflight,
				RequestCount: run.RequestCount,
			})
		}
		sort.Slice(token.Runs, func(i, j int) bool {
			return token.Runs[i].AgentID < token.Runs[j].AgentID
		})
		token.State = dashboardTokenStateName(token)
		output = append(output, token)
	}
	return output
}

func sessionUsageLabel(used, limit int) string {
	return strconv.Itoa(used) + " / " + strconv.Itoa(limit)
}

func dashboardTokenStateName(token dashboardTokenState) string {
	switch {
	case token.LastError != "":
		return "error"
	case token.Cooling || len(token.ModelCooldowns) > 0:
		return "cooling"
	case token.Session.Transitioning && token.Session.TransitionMode != "":
		return token.Session.TransitionMode + "-transition"
	case token.Session.Status == string(sessionStatusActive):
		return "active"
	case token.Session.Status == string(sessionStatusQueued):
		return "queued"
	case token.RunCount > 0:
		return "warm"
	default:
		return "idle"
	}
}

func fingerprintForTokenIndex(tokens []string, index int) string {
	if index < 0 || index >= len(tokens) {
		return ""
	}
	return tokenFingerprint(tokens[index])
}

func tokenFingerprint(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])[:8]
}

func tokenName(index int) string {
	return "token-" + strconv.Itoa(index+1)
}

func shortenID(value string) string {
	value = strings.TrimSpace(value)
	if len(value) <= 12 {
		return value
	}
	return value[:8] + "..." + value[len(value)-4:]
}

func positiveSeconds(duration time.Duration) int {
	if duration <= 0 {
		return 0
	}
	return int(duration.Seconds())
}

func copyStringIntMap(input map[string]int) map[string]int {
	if len(input) == 0 {
		return nil
	}
	output := make(map[string]int, len(input))
	for key, value := range input {
		output[key] = value
	}
	return output
}
