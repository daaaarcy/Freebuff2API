package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

const transitionTestModel = "deepseek/deepseek-v4-pro"
const transitionTestAgent = "base2-free-deepseek"

type transitionUpstream struct {
	t *testing.T

	mu              sync.Mutex
	starts          map[string]int
	sessions        map[string][]freeSessionResponse
	sessionErrors   map[string]upstreamError
	chatStatuses    map[string][]upstreamError
	chatTokens      []string
	chatSessionIDs  []string
	sessionRequests []string
}

type upstreamError struct {
	status int
	body   string
}

func newTransitionUpstream(t *testing.T) (*transitionUpstream, *httptest.Server) {
	t.Helper()
	upstream := &transitionUpstream{
		t:             t,
		starts:        make(map[string]int),
		sessions:      make(map[string][]freeSessionResponse),
		sessionErrors: make(map[string]upstreamError),
		chatStatuses:  make(map[string][]upstreamError),
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

func (u *transitionUpstream) handleChat(w http.ResponseWriter, r *http.Request, token string) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		u.t.Errorf("read chat body: %v", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	var payload struct {
		CodebuffMetadata struct {
			FreebuffInstanceID string `json:"freebuff_instance_id"`
		} `json:"codebuff_metadata"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		u.t.Errorf("decode chat body: %v", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	u.mu.Lock()
	u.chatTokens = append(u.chatTokens, token)
	u.chatSessionIDs = append(u.chatSessionIDs, payload.CodebuffMetadata.FreebuffInstanceID)
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
		agentModels:  map[string][]string{transitionTestAgent: {transitionTestModel}},
		modelToAgent: map[string]string{transitionTestModel: transitionTestAgent},
		allModels:    []string{transitionTestModel},
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
	body := bytes.NewBufferString(`{"model":"deepseek/deepseek-v4-pro","messages":[{"role":"user","content":"hi"}]}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", body)
	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, req)
	return recorder
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
