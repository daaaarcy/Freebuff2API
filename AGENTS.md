# Freebuff2API — Agent Guide

OpenAI- and Anthropic-compatible proxy for Codebuff's free-tier models. Translates standard API requests into Codebuff's backend format, managing sessions, runs, and multi-token failover transparently.

## Quick Reference

- **Language:** Go 1.23+, single `package main` at repo root
- **Upstream:** `https://www.codebuff.com`
- **Port:** `:9993` (deployment), `:8080` (default config)
- **Config:** `config.json` (gitignored), env vars override all fields
- **License:** MIT
- **Upstream fork:** `origin` = Quorinex/upstream, `fork` = daaaarcy/Freebuff2API — push to `fork`

## Architecture

```
Client (OpenAI / Claude format)
  │
  ▼
Server (server.go) ── API key auth middleware
  ├── /healthz                 → RunManager.Snapshots()
  ├── /v1/models               → ModelRegistry.Models()
  ├── /v1/chat/completions     → proxyChatRequest()
  ├── /v1/messages             → Claude→OpenAI convert → proxyChatRequest()
  └── /v1/messages/count_tokens → tiktoken estimation
  │
  ▼
proxyChatRequest
  ├── ModelRegistry.AgentForModel() → resolve agentID
  ├── RunManager.Acquire()          → select pool, rotate run, ensure session, return instance_id
  ├── injectUpstreamMetadata        → run_id, cost_mode, client_id, returned instance_id
  ├── UpstreamClient.ChatCompletions() → POST /api/v1/chat/completions
  └── Handle response: stream/retry on session/run invalidity, 401, 429
  │
  ▼
Upstream (www.codebuff.com)
  ├── POST /api/v1/agent-runs         (start/finish runs)
  ├── POST /api/v1/chat/completions   (LLM inference)
  └── POST/GET/DELETE /api/v1/freebuff/session (session lifecycle)
```

## Source Files

| File | Purpose |
|---|---|
| `main.go` | Entry point, config loading, HTTP server lifecycle, graceful shutdown |
| `config.go` | `Config` struct, JSON + env var loading, validation, user-agent generation |
| `models.go` | `ModelRegistry` — fetches agent→model mappings from upstream GitHub TS files, refreshes every 6h |
| `upstream.go` | `UpstreamClient` — HTTP methods for StartRun, FinishRun, ChatCompletions against Codebuff |
| `free_session.go` | Free session lifecycle (create/poll/end/invalidate), waiting room, rate limit parsing |
| `run_manager.go` | `RunManager` + `tokenPool` — run rotation, cooldown, failover, prewarming, maintenance |
| `server.go` | HTTP handlers, middleware, proxy logic, tool schema normalization, error formatting |
| `anthropic.go` | Claude↔OpenAI bidirectional format conversion (streaming + non-streaming) |
| `token_count.go` | Token counting via tiktoken for `/v1/messages/count_tokens` |
| `*_test.go` | Regression tests for session expiry, waiting-room/rate-limit aggregation, and auth-token failover |

## Configuration

`config.json` (auto-detected in CWD, or pass `-config` flag):

```json
{
  "LISTEN_ADDR": ":9993",
  "UPSTREAM_BASE_URL": "https://www.codebuff.com",
  "AUTH_TOKENS": ["token1", "token2"],
  "ROTATION_INTERVAL": "6h",
  "REQUEST_TIMEOUT": "15m",
  "API_KEYS": ["optional-client-facing-key"],
  "HTTP_PROXY": "",
  "SESSION_REQUIRED_MODELS": ["deepseek/deepseek-v4-pro", "deepseek/deepseek-v4-flash", "minimax/minimax-m2.7", "moonshotai/kimi-k2.6"],
  "PREMIUM_SESSION_MODELS": ["deepseek/deepseek-v4-pro", "moonshotai/kimi-k2.6"]
}
```

All fields overrideable by env vars of the same name. `AUTH_TOKENS`, `API_KEYS`, `SESSION_REQUIRED_MODELS`, and `PREMIUM_SESSION_MODELS` accept comma-separated values.

## Token Pool & Failover Strategy

One `tokenPool` per `AUTH_TOKEN`. **Sequential failover, not round-robin:**

1. **Only pool 0 (token-1) prewarms** on startup — creates runs for all 16 agents
2. **Backup pools stay idle** — no runs, no premium sessions, until needed
3. **Requests always try pool 0 first** — if it succeeds, done
4. **On session acquisition 429** — that token/model pair enters cooldown until `resetAt`/`Retry-After`, try next pool
5. **On chat 429** — that token/model pair is cooled down, session is invalidated, request retries through next pool before returning an error
6. **On chat 401** — the active token is cooled down token-wide because auth was rejected
7. **Backups activate lazily** — first request on a backup pool creates its run and premium session on demand

Current Freebuff models share one active session per token. Premium quota cooldowns are tracked per token/model because premium sessions are limited per model, while auth failures remain token-wide.

When every token is unavailable, `RunManager.Acquire()` preserves the most useful client-facing transition error:
- all available tokens queued → best `waitingRoomError` by queue position/retry delay
- all available tokens rate-limited → best `rateLimitError` by earliest reset/retry delay
- all available tokens cooling for the requested model → `cooldownError` surfaced as 429 with `Retry-After`
- cooldown-only or mixed unknown failures → generic 502 with per-token details in logs/error text

## Key Error Types

| Type | HTTP Status | When |
|---|---|---|
| `waitingRoomError` | 503 | Session is queued in upstream waiting room |
| `rateLimitError` | 429 | Upstream daily quota exhausted (includes model, usage, resetAt) |
| Generic | 502 | All pools exhausted or upstream unreachable |

Rate limit responses include `Retry-After` header and structured message:
```
rate limited for deepseek/deepseek-v4-pro (6/5 used), resets at 2026-05-29T07:00:00Z
```

## Model Registry

Fetches two TypeScript source files from GitHub every 6 hours:
- `free-agents.ts` — agent→model Set mappings
- `freebuff-models.ts` — model constant definitions

Root agents (preferred for serving):
- `base2-free`, `base2-free-kimi`, `base2-free-deepseek`, `base2-free-deepseek-flash`

Non-root agents (code-reviewer-*, file-picker, etc.) are excluded from the public model list — they require an active root ancestor run.

Falls back to `hardcodedFallback` map if upstream fetch fails.

## Session Management

Only models listed in `SESSION_REQUIRED_MODELS` use `/api/v1/freebuff/session`. Defaults are the current public Freebuff model IDs: `deepseek/deepseek-v4-pro`, `deepseek/deepseek-v4-flash`, `minimax/minimax-m2.7`, and `moonshotai/kimi-k2.6`.

Only models listed in `PREMIUM_SESSION_MODELS` are counted as premium session models. Defaults: `deepseek/deepseek-v4-pro` and `moonshotai/kimi-k2.6`.

Freebuff sessions are **per-pool shared sessions** (`session *cachedSession`), not per-model sessions.

Lifecycle: `ensureSession()` → check shared cache → `refreshSession()` → POST `/api/v1/freebuff/session` → poll until active → cache with expiry.

For session-required models, `RunManager.Acquire()` calls `ensureSession()` before leasing the run and returns the selected session instance ID to `proxyChatRequest`; handlers should not call `ensureSession()` a second time for the same request.

Active sessions are treated as stale inside the final 2 minutes before `expiresAt` (`freeSessionRefreshSafetyWindow`) so normal requests refresh before hitting upstream `session_expired`. If the session has in-flight requests, refresh is deferred and the request fails over to another token instead of ending the active session.

If a premium model requests a different active premium session and that session has no in-flight requests, the old session is ended before starting the requested model session. If it has in-flight requests, the request fails over instead of interrupting the long-running stream.

`deepseek/deepseek-v4-flash` is session-required but not premium-counted. If a token already has an active session, flash may reuse that session instance ID even when it was started for a premium model; it should not tear down a premium session just to run flash.

Session starts are logged with token, model, premium flag, instance ID, expiry, and per-model start count. `/healthz` exposes `session_model`, `session_premium`, and `session_started_counts` for each token.

## Upstream Request Injection

Before forwarding to `/api/v1/chat/completions`, the payload gets:
- `model`: requested model name
- `codebuff_metadata.run_id`: active run ID
- `codebuff_metadata.cost_mode`: `"free"`
- `codebuff_metadata.client_id`: random 13-char base-36 ID
- `codebuff_metadata.freebuff_instance_id`: session instance ID

Tool schemas are normalized: `$ref` resolution, nullable simplification, enum dedup, max depth 12.

## Claude Messages Compatibility

`/v1/messages` converts Claude format ↔ OpenAI format bidirectionally:
- Request: system blocks, content parts (text/image/tool_use/tool_result/thinking), tools, tool_choice
- Response: thinking→reasoning_content, tool_calls→tool_use, finish_reason→stop_reason
- Streaming: proper Claude SSE events (message_start, content_block_delta, etc.)

## Deployment

**Systemd** (production):
```bash
sudo systemctl restart freebuff2api
journalctl -u freebuff2api -f
```

**Docker:**
```bash
docker build -t freebuff2api .
docker run -p 8080:8080 -v ./config.json:/app/config.json freebuff2api
```

## Health Check

`GET /healthz` returns:
```json
{
  "ok": true,
  "started_at": "...",
  "token_state": [
    {
      "name": "token-1",
      "runs": [...],
      "draining_runs": 0,
      "session_status": "active",
      "cooldown_until": "...",
      "last_error": "..."
    }
  ]
}
```

Use this to diagnose 502s — check `last_error` and `cooldown_until` on each pool.

## Build & Test

```bash
go build -o freebuff2api .          # build
go vet ./...                        # lint
go test -count=1 ./...              # tests
./freebuff2api -config config.json  # run locally
```

## Common Gotchas

- **429 rate limits** are per-model, per-token, daily quota (resets midnight Pacific / ~3pm SGT)
- **Sessions expire** after 1h — cached sessions refresh 2 minutes before expiry, but long streams can still cross the upstream expiry boundary after response bytes are already sent
- **Prewarm only warms pool 0** — backup pools have no runs until first failover, expect slight latency on first backup request
- **Invalid tokens** (logged out) cause 404 on StartRun — remove from config or re-login
- **Model registry** fetches from GitHub — if fetch fails, hardcoded fallback is used (may be stale)
- **`config.json` is gitignored** — don't commit tokens
