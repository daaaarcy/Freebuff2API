package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func newDashboardTestServer() *Server {
	cfg := Config{
		ListenAddr:              ":9993",
		UpstreamBaseURL:         "https://www.codebuff.com",
		AuthTokens:              []string{"secret-token-one", "secret-token-two"},
		RotationInterval:        6 * time.Hour,
		RequestTimeout:          30 * time.Minute,
		SessionTransitionPeriod: 10 * time.Minute,
		UserAgent:               "freebuff-dashboard-test",
		APIKeys:                 []string{"client-secret-key"},
		HTTPProxy:               "http://proxy.example:8080",
		SessionRequiredModels:   []string{"deepseek/deepseek-v4-pro", "deepseek/deepseek-v4-flash"},
		PremiumSessionModels:    []string{"deepseek/deepseek-v4-pro"},
	}
	registry := &ModelRegistry{
		agentModels: map[string][]string{
			"base2-free-deepseek":       {"deepseek/deepseek-v4-pro"},
			"base2-free-deepseek-flash": {"deepseek/deepseek-v4-flash"},
		},
		modelToAgent: map[string]string{
			"deepseek/deepseek-v4-pro":   "base2-free-deepseek",
			"deepseek/deepseek-v4-flash": "base2-free-deepseek-flash",
		},
		allModels: []string{"deepseek/deepseek-v4-pro", "deepseek/deepseek-v4-flash"},
	}
	server := NewServer(cfg, nil, registry)
	server.runs.pools[0].session = &cachedSession{
		status:     sessionStatusActive,
		instanceID: "freebuff-instance-secret",
		agentID:    "base2-free-deepseek",
		model:      "deepseek/deepseek-v4-pro",
		expiresAt:  time.Now().Add(9 * time.Minute),
	}
	server.runs.pools[0].sessionStartedCounts = map[string]int{"deepseek/deepseek-v4-pro": 2}
	server.runs.pools[0].sessionStartEvents = []sessionStartEvent{
		{model: "deepseek/deepseek-v4-pro", startedAt: time.Now().Add(-23 * time.Hour)},
		{model: "deepseek/deepseek-v4-pro", startedAt: time.Now().Add(-25 * time.Hour)},
	}
	server.runs.pools[0].runs["base2-free-deepseek"] = &managedRun{
		id:           "run-secret-id",
		agentID:      "base2-free-deepseek",
		startedAt:    time.Now().Add(-15 * time.Minute),
		inflight:     1,
		requestCount: 3,
	}
	server.runs.pools[1].markModelCooldown("deepseek/deepseek-v4-pro", time.Hour, "rate limited for deepseek/deepseek-v4-pro")
	return server
}

func dashboardRequest(server *Server, method, path string, apiKey string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, nil)
	if apiKey != "" {
		req.Header.Set("x-api-key", apiKey)
	}
	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, req)
	return recorder
}

func TestDashboardRoutesServeStaticAssets(t *testing.T) {
	server := newDashboardTestServer()

	for _, tc := range []struct {
		path        string
		contentType string
		contains    string
	}{
		{path: "/dashboard/", contentType: "text/html", contains: "Freebuff2API"},
		{path: "/dashboard/style.css", contentType: "text/css", contains: "dashboard-shell"},
		{path: "/dashboard/app.js", contentType: "javascript", contains: "dashboard/api/state"},
	} {
		recorder := dashboardRequest(server, http.MethodGet, tc.path, "client-secret-key")
		if recorder.Code != http.StatusOK {
			t.Fatalf("%s status = %d, body = %s, want 200", tc.path, recorder.Code, recorder.Body.String())
		}
		if got := recorder.Header().Get("Content-Type"); !strings.Contains(got, tc.contentType) {
			t.Fatalf("%s content type = %q, want %q", tc.path, got, tc.contentType)
		}
		if !strings.Contains(recorder.Body.String(), tc.contains) {
			t.Fatalf("%s body missing %q", tc.path, tc.contains)
		}
	}
}

func TestDashboardStateRedactsSecretsAndSummarizesRuntime(t *testing.T) {
	server := newDashboardTestServer()

	recorder := dashboardRequest(server, http.MethodGet, "/dashboard/api/state", "client-secret-key")
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s, want 200", recorder.Code, recorder.Body.String())
	}

	body := recorder.Body.String()
	for _, secret := range []string{"secret-token-one", "secret-token-two", "client-secret-key", "proxy.example"} {
		if strings.Contains(body, secret) {
			t.Fatalf("dashboard state leaked secret %q in body: %s", secret, body)
		}
	}
	if !strings.Contains(body, "freebuff-instance-secret") {
		t.Fatalf("dashboard state missing active session instance ID")
	}

	var state map[string]any
	if err := json.Unmarshal(recorder.Body.Bytes(), &state); err != nil {
		t.Fatalf("decode state: %v", err)
	}
	config := state["config"].(map[string]any)
	if got, want := int(config["auth_token_count"].(float64)), 2; got != want {
		t.Fatalf("auth token count = %d, want %d", got, want)
	}
	if got := config["api_key_auth_enabled"].(bool); !got {
		t.Fatalf("api_key_auth_enabled = false, want true")
	}
	if got := config["http_proxy_configured"].(bool); !got {
		t.Fatalf("http_proxy_configured = false, want true")
	}
	tokens := state["tokens"].([]any)
	if got, want := len(tokens), 2; got != want {
		t.Fatalf("tokens length = %d, want %d", got, want)
	}
	firstToken := tokens[0].(map[string]any)
	if got, want := firstToken["fingerprint"].(string), tokenFingerprintForTest("secret-token-one"); got != want {
		t.Fatalf("fingerprint = %q, want %q", got, want)
	}
	if got, want := int(firstToken["session_starts_last_24h"].(float64)), 1; got != want {
		t.Fatalf("session starts last 24h = %d, want %d", got, want)
	}
	if got, want := int(firstToken["session_quota_limit_24h"].(float64)), 5; got != want {
		t.Fatalf("session quota limit 24h = %d, want %d", got, want)
	}
	if got, want := int(firstToken["session_slot_24h"].(float64)), 1; got != want {
		t.Fatalf("session slot 24h = %d, want %d", got, want)
	}
	if got, want := int(firstToken["session_slots_remaining_24h"].(float64)), 4; got != want {
		t.Fatalf("session slots remaining 24h = %d, want %d", got, want)
	}
	if got, want := firstToken["session_usage_label"].(string), "1 / 5"; got != want {
		t.Fatalf("session usage label = %q, want %q", got, want)
	}
	startsByModel := firstToken["session_starts_last_24h_by_model"].(map[string]any)
	if got, want := int(startsByModel["deepseek/deepseek-v4-pro"].(float64)), 1; got != want {
		t.Fatalf("session starts last 24h for pro = %d, want %d", got, want)
	}
	session := firstToken["session"].(map[string]any)
	if got, want := session["instance_id"].(string), "freebuff-instance-secret"; got != want {
		t.Fatalf("session instance ID = %q, want %q", got, want)
	}
	if got := session["transitioning"].(bool); !got {
		t.Fatalf("transitioning = false, want true for expiring premium session")
	}
	if got, want := session["transition_mode"].(string), "proactive"; got != want {
		t.Fatalf("transition mode = %q, want %q", got, want)
	}
	runs := firstToken["runs"].([]any)
	if got, want := len(runs), 1; got != want {
		t.Fatalf("runs length = %d, want %d", got, want)
	}
}

func TestDashboardStateIncludesRecentProcessLogs(t *testing.T) {
	server := newDashboardTestServer()
	server.logs = newLogBuffer(2)
	_, _ = server.logs.Write([]byte("first log line\n"))
	_, _ = server.logs.Write([]byte("second log line\nthird log line\n"))

	recorder := dashboardRequest(server, http.MethodGet, "/dashboard/api/state", "client-secret-key")
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s, want 200", recorder.Code, recorder.Body.String())
	}

	var state map[string]any
	if err := json.Unmarshal(recorder.Body.Bytes(), &state); err != nil {
		t.Fatalf("decode state: %v", err)
	}
	logs := state["logs"].([]any)
	if got, want := len(logs), 2; got != want {
		t.Fatalf("logs length = %d, want %d", got, want)
	}
	if got, want := logs[0].(string), "second log line"; got != want {
		t.Fatalf("first retained log = %q, want %q", got, want)
	}
	if got, want := logs[1].(string), "third log line"; got != want {
		t.Fatalf("second retained log = %q, want %q", got, want)
	}
}

func TestDashboardRoutesRequireAPIKeyWhenConfigured(t *testing.T) {
	server := newDashboardTestServer()

	recorder := dashboardRequest(server, http.MethodGet, "/dashboard/", "")
	if recorder.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated status = %d, want 401", recorder.Code)
	}

	recorder = dashboardRequest(server, http.MethodGet, "/dashboard/api/state", "client-secret-key")
	if recorder.Code != http.StatusOK {
		t.Fatalf("authenticated status = %d, body = %s, want 200", recorder.Code, recorder.Body.String())
	}
}

func TestRootRedirectsToDashboardAndUnknownPathsStayNotFound(t *testing.T) {
	server := newDashboardTestServer()
	server.cfg.APIKeys = nil

	recorder := dashboardRequest(server, http.MethodGet, "/", "")
	if recorder.Code != http.StatusFound {
		t.Fatalf("root status = %d, body = %s, want 302", recorder.Code, recorder.Body.String())
	}
	if got, want := recorder.Header().Get("Location"), "/dashboard/"; got != want {
		t.Fatalf("root location = %q, want %q", got, want)
	}

	recorder = dashboardRequest(server, http.MethodGet, "/missing", "")
	if recorder.Code != http.StatusNotFound {
		t.Fatalf("missing status = %d, body = %s, want 404", recorder.Code, recorder.Body.String())
	}
}

func tokenFingerprintForTest(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])[:8]
}
