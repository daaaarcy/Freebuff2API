package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

const transitionTestModel = "deepseek/deepseek-v4-pro"
const transitionTestAgent = "base2-free-deepseek"
const transitionFlashModel = "deepseek/deepseek-v4-flash"
const transitionFlashAgent = "base2-free-deepseek-flash"
const transitionKimiModel = "moonshotai/kimi-k2.6"
const transitionKimiAgent = "base2-free-kimi"

type transitionUpstream struct {
	t *testing.T

	mu                 sync.Mutex
	starts             map[string]int
	sessions           map[string][]freeSessionResponse
	sessionErrors      map[string]upstreamError
	modelSessionErrors map[string]upstreamError
	chatStatuses       map[string][]upstreamError
	chatDrops          map[string]int
	chatTokens         []string
	chatSessionIDs     []string
	chatPayloads       []map[string]any
	sessionRequests    []string
}

type upstreamError struct {
	status int
	body   string
}

func newTransitionUpstream(t *testing.T) (*transitionUpstream, *httptest.Server) {
	t.Helper()
	upstream := &transitionUpstream{
		t:                  t,
		starts:             make(map[string]int),
		sessions:           make(map[string][]freeSessionResponse),
		sessionErrors:      make(map[string]upstreamError),
		modelSessionErrors: make(map[string]upstreamError),
		chatStatuses:       make(map[string][]upstreamError),
		chatDrops:          make(map[string]int),
	}
	server := httptest.NewServer(http.HandlerFunc(upstream.handle))
	t.Cleanup(server.Close)
	return upstream, server
}

func (u *transitionUpstream) handle(w http.ResponseWriter, r *http.Request) {
	token := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	switch r.URL.Path {
	case "/api/v1/agent-runs":
		u.handleAgentRuns(w, r, token)
	case "/api/v1/freebuff/session":
		u.handleSession(w, r, token)
	case "/api/v1/chat/completions":
		u.handleChat(w, r, token)
	default:
		http.NotFound(w, r)
	}
}

func (u *transitionUpstream) handleAgentRuns(w http.ResponseWriter, r *http.Request, token string) {
	var payload struct {
		Action string `json:"action"`
	}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		u.t.Errorf("decode agent run request: %v", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if payload.Action == "FINISH" {
		writeJSON(w, http.StatusOK, map[string]any{"ok": true})
		return
	}

	u.mu.Lock()
	u.starts[token]++
	runID := token + "-run-" + time.Now().Format("150405.000000000")
	u.mu.Unlock()

	writeJSON(w, http.StatusOK, map[string]any{"runId": runID})
}

func (u *transitionUpstream) handleSession(w http.ResponseWriter, r *http.Request, token string) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusOK, freeSessionResponse{Status: string(sessionStatusEnded)})
		return
	}

	u.mu.Lock()
	u.sessionRequests = append(u.sessionRequests, token)
	requestedModel := requestedSessionModel(u.t, r)
	if configured, ok := u.modelSessionErrors[token+"|"+requestedModel]; ok {
		u.mu.Unlock()
		w.WriteHeader(configured.status)
		_, _ = w.Write([]byte(configured.body))
		return
	}
	if configured, ok := u.sessionErrors[token]; ok {
		u.mu.Unlock()
		w.WriteHeader(configured.status)
		_, _ = w.Write([]byte(configured.body))
		return
	}
	responses := u.sessions[token]
	var response freeSessionResponse
	if len(responses) > 0 {
		response = responses[0]
		u.sessions[token] = responses[1:]
	} else {
		response = activeSessionResponse(token)
	}
	u.mu.Unlock()

	writeJSON(w, http.StatusOK, response)
}

func requestedSessionModel(t *testing.T, r *http.Request) string {
	t.Helper()
	return strings.TrimSpace(r.Header.Get("x-freebuff-model"))
}

func (u *transitionUpstream) handleChat(w http.ResponseWriter, r *http.Request, token string) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		u.t.Errorf("read chat body: %v", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err != nil {
		u.t.Errorf("decode chat body: %v", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	metadata := mapValue(payload["codebuff_metadata"])

	u.mu.Lock()
	u.chatTokens = append(u.chatTokens, token)
	u.chatSessionIDs = append(u.chatSessionIDs, stringValue(metadata["freebuff_instance_id"]))
	u.chatPayloads = append(u.chatPayloads, payload)
	if u.chatDrops[token] > 0 {
		u.chatDrops[token]--
		u.mu.Unlock()
		dropConnection(u.t, w)
		return
	}
	if responses := u.chatStatuses[token]; len(responses) > 0 {
		response := responses[0]
		u.chatStatuses[token] = responses[1:]
		u.mu.Unlock()
		w.WriteHeader(response.status)
		_, _ = w.Write([]byte(response.body))
		return
	}
	u.mu.Unlock()

	writeJSON(w, http.StatusOK, map[string]any{
		"id":      "chatcmpl-test",
		"object":  "chat.completion",
		"choices": []any{},
	})
}

func (u *transitionUpstream) chatTokenSequence() []string {
	u.mu.Lock()
	defer u.mu.Unlock()
	out := make([]string, len(u.chatTokens))
	copy(out, u.chatTokens)
	return out
}

func (u *transitionUpstream) chatSessionSequence() []string {
	u.mu.Lock()
	defer u.mu.Unlock()
	out := make([]string, len(u.chatSessionIDs))
	copy(out, u.chatSessionIDs)
	return out
}

func (u *transitionUpstream) chatPayloadSequence() []map[string]any {
	u.mu.Lock()
	defer u.mu.Unlock()
	out := make([]map[string]any, len(u.chatPayloads))
	copy(out, u.chatPayloads)
	return out
}

func (u *transitionUpstream) sessionRequestSequence() []string {
	u.mu.Lock()
	defer u.mu.Unlock()
	out := make([]string, len(u.sessionRequests))
	copy(out, u.sessionRequests)
	return out
}

func dropConnection(t *testing.T, w http.ResponseWriter) {
	t.Helper()
	hijacker, ok := w.(http.Hijacker)
	if !ok {
		t.Fatalf("response writer does not support hijacking")
	}
	conn, _, err := hijacker.Hijack()
	if err != nil {
		t.Fatalf("hijack response writer: %v", err)
	}
	_ = conn.(*net.TCPConn).SetLinger(0)
	_ = conn.Close()
}

func activeSessionResponse(token string) freeSessionResponse {
	return freeSessionResponse{
		Status:     string(sessionStatusActive),
		InstanceID: token + "-session",
		ExpiresAt:  time.Now().Add(time.Hour).UTC().Format(time.RFC3339),
	}
}

func newTransitionServer(baseURL string, tokens []string) *Server {
	cfg := Config{
		UpstreamBaseURL:  baseURL,
		AuthTokens:       tokens,
		RotationInterval: 6 * time.Hour,
		RequestTimeout:   5 * time.Second,
		UserAgent:        "freebuff-transition-test",
	}
	logger := log.New(io.Discard, "", 0)
	client := NewUpstreamClient(cfg)
	registry := &ModelRegistry{
		agentModels: map[string][]string{
			transitionTestAgent:  {transitionTestModel},
			transitionFlashAgent: {transitionFlashModel},
			transitionKimiAgent:  {transitionKimiModel},
		},
		modelToAgent: map[string]string{
			transitionTestModel:  transitionTestAgent,
			transitionFlashModel: transitionFlashAgent,
			transitionKimiModel:  transitionKimiAgent,
		},
		allModels: []string{transitionTestModel, transitionFlashModel, transitionKimiModel},
	}
	return &Server{
		cfg:      cfg,
		logger:   logger,
		client:   client,
		runs:     NewRunManager(cfg, client, logger),
		registry: registry,
		started:  time.Now(),
	}
}

func performChatRequest(t *testing.T, server *Server) *httptest.ResponseRecorder {
	t.Helper()
	return performChatRequestBody(t, server, `{"model":"deepseek/deepseek-v4-pro","messages":[{"role":"user","content":"hi"}]}`)
}

func performChatRequestBody(t *testing.T, server *Server, body string) *httptest.ResponseRecorder {
	t.Helper()
	requestBody := bytes.NewBufferString(body)
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", requestBody)
	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, req)
	return recorder
}

func TestProxyUsesSharedFreeSessionForFlashModel(t *testing.T) {
	upstream, upstreamServer := newTransitionUpstream(t)
	server := newTransitionServer(upstreamServer.URL, []string{"token-1"})

	recorder := performChatRequestBody(t, server, `{"model":"deepseek/deepseek-v4-flash","messages":[{"role":"user","content":"hi"}]}`)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s, want 200", recorder.Code, recorder.Body.String())
	}
	if got, want := strings.Join(upstream.sessionRequestSequence(), ","), "token-1"; got != want {
		t.Fatalf("session requests = %s, want one shared free session", got)
	}
	if got, want := strings.Join(upstream.chatSessionSequence(), ","), "token-1-session"; got != want {
		t.Fatalf("chat session IDs = %q, want shared session ID", got)
	}
}

func TestProxyRetriesSessionInvalidResponseForFlashModel(t *testing.T) {
	upstream, upstreamServer := newTransitionUpstream(t)
	upstream.chatStatuses["token-1"] = []upstreamError{{
		status: http.StatusUpgradeRequired,
		body:   `{"error":"freebuff_update_required","message":"This version of freebuff is out of date."}`,
	}}
	server := newTransitionServer(upstreamServer.URL, []string{"token-1"})

	recorder := performChatRequestBody(t, server, `{"model":"deepseek/deepseek-v4-flash","messages":[{"role":"user","content":"hi"}]}`)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s, want 200", recorder.Code, recorder.Body.String())
	}
	if got, want := strings.Join(upstream.sessionRequestSequence(), ","), "token-1,token-1"; got != want {
		t.Fatalf("session requests = %s, want initial and refreshed free sessions", got)
	}
	if got, want := len(upstream.chatTokenSequence()), 2; got != want {
		t.Fatalf("chat attempts = %d, want %d", got, want)
	}
}

func TestProxySharesOneSessionAcrossSamePremiumModelRequests(t *testing.T) {
	upstream, upstreamServer := newTransitionUpstream(t)
	server := newTransitionServer(upstreamServer.URL, []string{"token-1"})

	first := performChatRequestBody(t, server, `{"model":"deepseek/deepseek-v4-pro","messages":[{"role":"user","content":"hi"}]}`)
	if first.Code != http.StatusOK {
		t.Fatalf("first status = %d, body = %s, want 200", first.Code, first.Body.String())
	}
	second := performChatRequestBody(t, server, `{"model":"deepseek/deepseek-v4-pro","messages":[{"role":"user","content":"hi again"}]}`)
	if second.Code != http.StatusOK {
		t.Fatalf("second status = %d, body = %s, want 200", second.Code, second.Body.String())
	}

	if got, want := strings.Join(upstream.sessionRequestSequence(), ","), "token-1"; got != want {
		t.Fatalf("session requests = %s, want one shared premium session", got)
	}
	if got, want := strings.Join(upstream.chatSessionSequence(), ","), "token-1-session,token-1-session"; got != want {
		t.Fatalf("chat session IDs = %s, want one shared premium session ID", got)
	}
}

func TestFlashReusesExistingPremiumSessionWithoutStartingNewSession(t *testing.T) {
	upstream, upstreamServer := newTransitionUpstream(t)
	server := newTransitionServer(upstreamServer.URL, []string{"token-1"})

	pro := performChatRequestBody(t, server, `{"model":"deepseek/deepseek-v4-pro","messages":[{"role":"user","content":"hi"}]}`)
	if pro.Code != http.StatusOK {
		t.Fatalf("pro status = %d, body = %s, want 200", pro.Code, pro.Body.String())
	}
	flash := performChatRequestBody(t, server, `{"model":"deepseek/deepseek-v4-flash","messages":[{"role":"user","content":"hi"}]}`)
	if flash.Code != http.StatusOK {
		t.Fatalf("flash status = %d, body = %s, want 200", flash.Code, flash.Body.String())
	}

	if got, want := strings.Join(upstream.sessionRequestSequence(), ","), "token-1"; got != want {
		t.Fatalf("session requests = %s, want flash to reuse existing pro session", got)
	}
	if got, want := strings.Join(upstream.chatSessionSequence(), ","), "token-1-session,token-1-session"; got != want {
		t.Fatalf("chat session IDs = %s, want shared pro session for flash", got)
	}

	snapshots := server.runs.Snapshots()
	if got := snapshots[0].SessionModel; got != transitionTestModel {
		t.Fatalf("session model = %q, want original pro model", got)
	}
	if got := snapshots[0].SessionStartedCounts[transitionTestModel]; got != 1 {
		t.Fatalf("pro session starts = %d, want 1", got)
	}
	if got := snapshots[0].SessionStartedCounts[transitionFlashModel]; got != 0 {
		t.Fatalf("flash session starts = %d, want 0 when reusing pro session", got)
	}
}

func TestFlashReusesPremiumSessionWhilePremiumRequestInflight(t *testing.T) {
	upstream, upstreamServer := newTransitionUpstream(t)
	server := newTransitionServer(upstreamServer.URL, []string{"token-1", "token-2"})

	proLease, _, err := server.runs.Acquire(context.Background(), transitionTestAgent, transitionTestModel)
	if err != nil {
		t.Fatalf("acquire pro lease: %v", err)
	}
	defer server.runs.Release(proLease)

	flash := performChatRequestBody(t, server, `{"model":"deepseek/deepseek-v4-flash","messages":[{"role":"user","content":"hi"}]}`)
	if flash.Code != http.StatusOK {
		t.Fatalf("flash status = %d, body = %s, want 200", flash.Code, flash.Body.String())
	}
	if got, want := strings.Join(upstream.sessionRequestSequence(), ","), "token-1"; got != want {
		t.Fatalf("session requests = %s, want flash to reuse in-flight pro session", got)
	}
	if got, want := strings.Join(upstream.chatTokenSequence(), ","), "token-1"; got != want {
		t.Fatalf("chat tokens = %s, want flash to stay on token-1", got)
	}
	if got, want := strings.Join(upstream.chatSessionSequence(), ","), "token-1-session"; got != want {
		t.Fatalf("chat session IDs = %s, want pro session reused for flash", got)
	}
}

func TestAcquireReturnsBestWaitingRoomWhenAllTokensQueued(t *testing.T) {
	upstream, upstreamServer := newTransitionUpstream(t)
	upstream.sessions["token-1"] = []freeSessionResponse{{
		Status:          string(sessionStatusQueued),
		InstanceID:      "token-1-queued",
		Position:        3,
		QueueDepth:      5,
		EstimatedWaitMs: 5_000,
	}}
	upstream.sessions["token-2"] = []freeSessionResponse{{
		Status:          string(sessionStatusQueued),
		InstanceID:      "token-2-queued",
		Position:        1,
		QueueDepth:      5,
		EstimatedWaitMs: 1_000,
	}}
	server := newTransitionServer(upstreamServer.URL, []string{"token-1", "token-2"})

	lease, _, err := server.runs.Acquire(context.Background(), transitionTestAgent, transitionTestModel)
	if lease != nil {
		t.Fatalf("lease = %#v, want nil", lease)
	}
	var waitingErr *waitingRoomError
	if !errors.As(err, &waitingErr) {
		t.Fatalf("err = %v, want waitingRoomError", err)
	}
	if waitingErr.Token != "token-2" || waitingErr.Position != 1 {
		t.Fatalf("waiting error = %#v, want token-2 at position 1", waitingErr)
	}
}

func TestAcquireReturnsWaitingRoomWhenOtherTokensCoolingDown(t *testing.T) {
	upstream, upstreamServer := newTransitionUpstream(t)
	upstream.sessions["token-2"] = []freeSessionResponse{{
		Status:          string(sessionStatusQueued),
		InstanceID:      "token-2-queued",
		Position:        2,
		QueueDepth:      4,
		EstimatedWaitMs: 2_000,
	}}
	server := newTransitionServer(upstreamServer.URL, []string{"token-1", "token-2"})
	server.runs.pools[0].markCooldown(time.Hour, "test cooldown")

	lease, _, err := server.runs.Acquire(context.Background(), transitionTestAgent, transitionTestModel)
	if lease != nil {
		t.Fatalf("lease = %#v, want nil", lease)
	}
	var waitingErr *waitingRoomError
	if !errors.As(err, &waitingErr) {
		t.Fatalf("err = %v, want waitingRoomError", err)
	}
	if waitingErr.Token != "token-2" || waitingErr.Position != 2 {
		t.Fatalf("waiting error = %#v, want token-2 at position 2", waitingErr)
	}
}

func TestAcquireReturnsEarliestRateLimitWhenAllTokensLimited(t *testing.T) {
	earlier := time.Now().Add(30 * time.Minute).UTC().Format(time.RFC3339)
	later := time.Now().Add(time.Hour).UTC().Format(time.RFC3339)
	upstream, upstreamServer := newTransitionUpstream(t)
	upstream.sessionErrors["token-1"] = upstreamError{
		status: http.StatusTooManyRequests,
		body:   `{"model":"deepseek/deepseek-v4-pro","resetAt":"` + earlier + `","retryAfterMs":1800000,"limit":5,"recentCount":5}`,
	}
	upstream.sessionErrors["token-2"] = upstreamError{
		status: http.StatusTooManyRequests,
		body:   `{"model":"deepseek/deepseek-v4-pro","resetAt":"` + later + `","retryAfterMs":3600000,"limit":5,"recentCount":5}`,
	}
	server := newTransitionServer(upstreamServer.URL, []string{"token-1", "token-2"})

	lease, _, err := server.runs.Acquire(context.Background(), transitionTestAgent, transitionTestModel)
	if lease != nil {
		t.Fatalf("lease = %#v, want nil", lease)
	}
	var rateErr *rateLimitError
	if !errors.As(err, &rateErr) {
		t.Fatalf("err = %v, want rateLimitError", err)
	}
	if got, want := rateErr.RetryAfter, 30*time.Minute; got != want {
		t.Fatalf("retry after = %v, want %v", got, want)
	}
}

func TestAcquireRateLimitCoolsOnlyRequestedModel(t *testing.T) {
	resetAt := time.Now().Add(time.Hour).UTC().Format(time.RFC3339)
	upstream, upstreamServer := newTransitionUpstream(t)
	upstream.modelSessionErrors["token-1|"+transitionFlashModel] = upstreamError{
		status: http.StatusTooManyRequests,
		body:   `{"model":"` + transitionFlashModel + `","resetAt":"` + resetAt + `","retryAfterMs":3600000,"limit":5,"recentCount":5}`,
	}
	server := newTransitionServer(upstreamServer.URL, []string{"token-1"})

	flashLease, _, flashErr := server.runs.Acquire(context.Background(), transitionFlashAgent, transitionFlashModel)
	if flashLease != nil {
		t.Fatalf("flash lease = %#v, want nil", flashLease)
	}
	var rateErr *rateLimitError
	if !errors.As(flashErr, &rateErr) {
		t.Fatalf("flash err = %v, want rateLimitError", flashErr)
	}

	proLease, _, proErr := server.runs.Acquire(context.Background(), transitionTestAgent, transitionTestModel)
	if proErr != nil {
		t.Fatalf("pro acquire after flash limit: %v", proErr)
	}
	if proLease == nil || proLease.pool.name != "token-1" {
		t.Fatalf("pro lease = %#v, want token-1 lease", proLease)
	}
	server.runs.Release(proLease)

	snapshots := server.runs.Snapshots()
	if len(snapshots) != 1 {
		t.Fatalf("snapshots len = %d, want 1", len(snapshots))
	}
	if !snapshots[0].CooldownUntil.IsZero() {
		t.Fatalf("token cooldown = %s, want no token-wide cooldown", snapshots[0].CooldownUntil)
	}
	if len(snapshots[0].ModelCooldowns) != 1 {
		t.Fatalf("model cooldowns = %#v, want one flash cooldown", snapshots[0].ModelCooldowns)
	}
	if got := snapshots[0].ModelCooldowns[0].Model; got != transitionFlashModel {
		t.Fatalf("model cooldown = %q, want %q", got, transitionFlashModel)
	}
}

func TestProxyRetriesUnauthorizedChatOnNextToken(t *testing.T) {
	upstream, upstreamServer := newTransitionUpstream(t)
	upstream.chatStatuses["token-1"] = []upstreamError{{
		status: http.StatusUnauthorized,
		body:   `{"error":"auth token expired"}`,
	}}
	server := newTransitionServer(upstreamServer.URL, []string{"token-1", "token-2"})

	recorder := performChatRequest(t, server)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s, want 200", recorder.Code, recorder.Body.String())
	}
	if got, want := strings.Join(upstream.chatTokenSequence(), ","), "token-1,token-2"; got != want {
		t.Fatalf("chat tokens = %s, want %s", got, want)
	}
	if got, want := strings.Join(upstream.chatSessionSequence(), ","), "token-1-session,token-2-session"; got != want {
		t.Fatalf("chat session IDs = %s, want %s", got, want)
	}
}

func TestProxyRetriesRateLimitedChatOnNextToken(t *testing.T) {
	upstream, upstreamServer := newTransitionUpstream(t)
	upstream.chatStatuses["token-1"] = []upstreamError{{
		status: http.StatusTooManyRequests,
		body:   `{"model":"deepseek/deepseek-v4-pro","resetAt":"` + time.Now().Add(time.Hour).UTC().Format(time.RFC3339) + `","retryAfterMs":3600000,"limit":5,"recentCount":5}`,
	}}
	server := newTransitionServer(upstreamServer.URL, []string{"token-1", "token-2"})

	recorder := performChatRequest(t, server)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s, want 200", recorder.Code, recorder.Body.String())
	}
	if got, want := strings.Join(upstream.chatTokenSequence(), ","), "token-1,token-2"; got != want {
		t.Fatalf("chat tokens = %s, want %s", got, want)
	}
}

func TestProxyChatRateLimitCoolsOnlyRequestedModel(t *testing.T) {
	upstream, upstreamServer := newTransitionUpstream(t)
	upstream.chatStatuses["token-1"] = []upstreamError{{
		status: http.StatusTooManyRequests,
		body:   `{"model":"` + transitionFlashModel + `","resetAt":"` + time.Now().Add(time.Hour).UTC().Format(time.RFC3339) + `","retryAfterMs":3600000,"limit":5,"recentCount":5}`,
	}}
	server := newTransitionServer(upstreamServer.URL, []string{"token-1"})

	flash := performChatRequestBody(t, server, `{"model":"deepseek/deepseek-v4-flash","messages":[{"role":"user","content":"hi"}]}`)
	if flash.Code != http.StatusTooManyRequests {
		t.Fatalf("flash status = %d, body = %s, want 429", flash.Code, flash.Body.String())
	}

	pro := performChatRequestBody(t, server, `{"model":"deepseek/deepseek-v4-pro","messages":[{"role":"user","content":"hi"}]}`)
	if pro.Code != http.StatusOK {
		t.Fatalf("pro status = %d, body = %s, want 200", pro.Code, pro.Body.String())
	}
	if got, want := strings.Join(upstream.chatTokenSequence(), ","), "token-1,token-1"; got != want {
		t.Fatalf("chat tokens = %s, want %s", got, want)
	}
}

func TestInjectUpstreamMetadataTranslatesJSONSchemaResponseFormat(t *testing.T) {
	server := newTransitionServer("http://example.invalid", []string{"token-1"})
	payload := map[string]any{
		"model": transitionTestModel,
		"messages": []any{
			map[string]any{"role": "user", "content": "return the ticker"},
		},
		"response_format": map[string]any{
			"type": "json_schema",
			"json_schema": map[string]any{
				"name": "ticker_response",
				"schema": map[string]any{
					"type":     "object",
					"required": []any{"ticker"},
					"properties": map[string]any{
						"ticker": map[string]any{"type": "string"},
					},
				},
			},
		},
	}

	body, err := server.injectUpstreamMetadata(payload, transitionTestModel, "run-1", "session-1")
	if err != nil {
		t.Fatalf("inject metadata: %v", err)
	}

	var forwarded map[string]any
	if err := json.Unmarshal(body, &forwarded); err != nil {
		t.Fatalf("decode forwarded body: %v", err)
	}
	responseFormat := mapValue(forwarded["response_format"])
	if got := stringValue(responseFormat["type"]); got != "json_object" {
		t.Fatalf("forwarded response_format.type = %q, want json_object", got)
	}
	if _, exists := responseFormat["json_schema"]; exists {
		t.Fatalf("forwarded response_format json_schema = %#v, want stripped for upstream compatibility", responseFormat["json_schema"])
	}
	messages := sliceValue(forwarded["messages"])
	if len(messages) != 2 {
		t.Fatalf("messages len = %d, want 2", len(messages))
	}
	systemMessage := mapValue(messages[0])
	if got := stringValue(systemMessage["role"]); got != "system" {
		t.Fatalf("first message role = %q, want system", got)
	}
	content := stringValue(systemMessage["content"])
	if !strings.Contains(content, "valid JSON object") || !strings.Contains(content, `"ticker"`) {
		t.Fatalf("system instruction = %q, want JSON object instruction containing schema", content)
	}
	if originalMessages := sliceValue(payload["messages"]); stringValue(mapValue(originalMessages[0])["role"]) != "user" {
		t.Fatalf("original payload messages mutated: %#v", payload["messages"])
	}
	if _, stillExists := mapValue(payload["response_format"])["json_schema"]; !stillExists {
		t.Fatalf("original response_format mutated: %#v", payload["response_format"])
	}
}

func TestInjectUpstreamMetadataPreservesJSONObjectResponseFormat(t *testing.T) {
	server := newTransitionServer("http://example.invalid", []string{"token-1"})
	payload := map[string]any{
		"model":           transitionTestModel,
		"messages":        []any{map[string]any{"role": "user", "content": "return json"}},
		"response_format": map[string]any{"type": "json_object"},
	}

	body, err := server.injectUpstreamMetadata(payload, transitionTestModel, "run-1", "session-1")
	if err != nil {
		t.Fatalf("inject metadata: %v", err)
	}

	var forwarded map[string]any
	if err := json.Unmarshal(body, &forwarded); err != nil {
		t.Fatalf("decode forwarded body: %v", err)
	}
	responseFormat := mapValue(forwarded["response_format"])
	if got := stringValue(responseFormat["type"]); got != "json_object" {
		t.Fatalf("forwarded response_format.type = %q, want json_object", got)
	}
	if len(responseFormat) != 1 {
		t.Fatalf("forwarded response_format = %#v, want only type=json_object", responseFormat)
	}
	systemMessage := mapValue(sliceValue(forwarded["messages"])[0])
	content := stringValue(systemMessage["content"])
	if !strings.Contains(content, "valid JSON object") || strings.Contains(content, "schema") {
		t.Fatalf("system instruction = %q, want plain JSON object instruction", content)
	}
}

func TestProxyRejectsUnsupportedResponseFormat(t *testing.T) {
	_, upstreamServer := newTransitionUpstream(t)
	server := newTransitionServer(upstreamServer.URL, []string{"token-1"})

	recorder := performChatRequestBody(t, server, `{"model":"deepseek/deepseek-v4-pro","messages":[{"role":"user","content":"hi"}],"response_format":{"type":"xml"}}`)
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, body = %s, want 400", recorder.Code, recorder.Body.String())
	}
	if !strings.Contains(recorder.Body.String(), "unsupported response_format type") {
		t.Fatalf("body = %s, want unsupported response_format message", recorder.Body.String())
	}
}

func TestProxyRetriesClosedUpstreamChatConnection(t *testing.T) {
	upstream, upstreamServer := newTransitionUpstream(t)
	upstream.chatDrops["token-1"] = 1
	server := newTransitionServer(upstreamServer.URL, []string{"token-1"})

	recorder := performChatRequest(t, server)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s, want 200", recorder.Code, recorder.Body.String())
	}
	if got, want := strings.Join(upstream.chatTokenSequence(), ","), "token-1,token-1"; got != want {
		t.Fatalf("chat tokens = %s, want %s", got, want)
	}
}

func TestProxyRetriesUpstreamBadGatewayStatus(t *testing.T) {
	upstream, upstreamServer := newTransitionUpstream(t)
	upstream.chatStatuses["token-1"] = []upstreamError{{
		status: http.StatusBadGateway,
		body:   `<!DOCTYPE html><html><body><h1>Bad Gateway</h1></body></html>`,
	}}
	server := newTransitionServer(upstreamServer.URL, []string{"token-1"})

	recorder := performChatRequest(t, server)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s, want 200", recorder.Code, recorder.Body.String())
	}
	if got, want := strings.Join(upstream.chatTokenSequence(), ","), "token-1,token-1"; got != want {
		t.Fatalf("chat tokens = %s, want %s", got, want)
	}
}

func TestProxyReturnsConciseBadGatewayAfterTransientRetriesExhausted(t *testing.T) {
	upstream, upstreamServer := newTransitionUpstream(t)
	upstream.chatStatuses["token-1"] = []upstreamError{
		{status: http.StatusBadGateway, body: `<!DOCTYPE html><html><body><h1>Bad Gateway</h1></body></html>`},
		{status: http.StatusBadGateway, body: `<!DOCTYPE html><html><body><h1>Bad Gateway</h1></body></html>`},
	}
	server := newTransitionServer(upstreamServer.URL, []string{"token-1"})

	recorder := performChatRequest(t, server)
	if recorder.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, body = %s, want 502", recorder.Code, recorder.Body.String())
	}
	if strings.Contains(recorder.Body.String(), "<html>") || strings.Contains(recorder.Body.String(), "<!DOCTYPE html>") {
		t.Fatalf("body = %s, want concise JSON error without raw HTML", recorder.Body.String())
	}
	if !strings.Contains(recorder.Body.String(), "upstream temporary failure") {
		t.Fatalf("body = %s, want upstream temporary failure message", recorder.Body.String())
	}
}

func TestProxyRetriesInvalidJSONObjectResponse(t *testing.T) {
	upstream, upstreamServer := newTransitionUpstream(t)
	upstream.chatStatuses["token-1"] = []upstreamError{
		{status: http.StatusOK, body: `{"id":"chatcmpl-test","object":"chat.completion","choices":[{"index":0,"message":{"role":"assistant","content":"not json"},"finish_reason":"stop"}]}`},
		{status: http.StatusOK, body: `{"id":"chatcmpl-test","object":"chat.completion","choices":[{"index":0,"message":{"role":"assistant","content":"{\"ok\":true}"},"finish_reason":"stop"}]}`},
	}
	server := newTransitionServer(upstreamServer.URL, []string{"token-1"})

	recorder := performChatRequestBody(t, server, `{"model":"deepseek/deepseek-v4-pro","messages":[{"role":"user","content":"return JSON"}],"response_format":{"type":"json_object"}}`)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s, want 200", recorder.Code, recorder.Body.String())
	}
	content := openAIResponseContent(t, recorder.Body.Bytes())
	if content != `{"ok":true}` {
		t.Fatalf("content = %q, want valid JSON object", content)
	}
	if got, want := len(upstream.chatPayloadSequence()), 2; got != want {
		t.Fatalf("chat requests = %d, want %d", got, want)
	}
}

func TestProxyRetriesInvalidJSONSchemaResponse(t *testing.T) {
	upstream, upstreamServer := newTransitionUpstream(t)
	upstream.chatStatuses["token-1"] = []upstreamError{
		{status: http.StatusOK, body: `{"id":"chatcmpl-test","object":"chat.completion","choices":[{"index":0,"message":{"role":"assistant","content":"{\"ticker\":123}"},"finish_reason":"stop"}]}`},
		{status: http.StatusOK, body: `{"id":"chatcmpl-test","object":"chat.completion","choices":[{"index":0,"message":{"role":"assistant","content":"{\"ticker\":\"AAPL\"}"},"finish_reason":"stop"}]}`},
	}
	server := newTransitionServer(upstreamServer.URL, []string{"token-1"})

	recorder := performChatRequestBody(t, server, `{"model":"deepseek/deepseek-v4-pro","messages":[{"role":"user","content":"return a ticker"}],"response_format":{"type":"json_schema","json_schema":{"name":"ticker_response","strict":true,"schema":{"type":"object","additionalProperties":false,"required":["ticker"],"properties":{"ticker":{"type":"string"}}}}}}`)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s, want 200", recorder.Code, recorder.Body.String())
	}
	content := openAIResponseContent(t, recorder.Body.Bytes())
	if content != `{"ticker":"AAPL"}` {
		t.Fatalf("content = %q, want schema-valid object", content)
	}
	payloads := upstream.chatPayloadSequence()
	if got, want := len(payloads), 2; got != want {
		t.Fatalf("chat requests = %d, want %d", got, want)
	}
	messages := sliceValue(payloads[1]["messages"])
	firstSystem := mapValue(messages[0])
	if !strings.Contains(stringValue(firstSystem["content"]), "previous response") {
		t.Fatalf("retry system instruction = %q, want correction instruction", stringValue(firstSystem["content"]))
	}
}

func TestProxyReturnsValidationErrorAfterStructuredOutputRetryFails(t *testing.T) {
	upstream, upstreamServer := newTransitionUpstream(t)
	upstream.chatStatuses["token-1"] = []upstreamError{
		{status: http.StatusOK, body: `{"id":"chatcmpl-test","object":"chat.completion","choices":[{"index":0,"message":{"role":"assistant","content":"{\"ticker\":123}"},"finish_reason":"stop"}]}`},
		{status: http.StatusOK, body: `{"id":"chatcmpl-test","object":"chat.completion","choices":[{"index":0,"message":{"role":"assistant","content":"{\"ticker\":456}"},"finish_reason":"stop"}]}`},
	}
	server := newTransitionServer(upstreamServer.URL, []string{"token-1"})

	recorder := performChatRequestBody(t, server, `{"model":"deepseek/deepseek-v4-pro","messages":[{"role":"user","content":"return a ticker"}],"response_format":{"type":"json_schema","json_schema":{"name":"ticker_response","strict":true,"schema":{"type":"object","additionalProperties":false,"required":["ticker"],"properties":{"ticker":{"type":"string"}}}}}}`)
	if recorder.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, body = %s, want 502", recorder.Code, recorder.Body.String())
	}
	if !strings.Contains(recorder.Body.String(), "upstream_response_validation_failed") {
		t.Fatalf("body = %s, want validation failure code", recorder.Body.String())
	}
	if got, want := len(upstream.chatPayloadSequence()), 2; got != want {
		t.Fatalf("chat requests = %d, want %d", got, want)
	}
}

func openAIResponseContent(t *testing.T, body []byte) string {
	t.Helper()
	var payload struct {
		Choices []struct {
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
		} `json:"choices"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(payload.Choices) == 0 {
		t.Fatalf("response has no choices: %s", string(body))
	}
	return payload.Choices[0].Message.Content
}
