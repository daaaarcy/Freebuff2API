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
  ├── RunManager.Acquire()          → select pool, rotate run, ensure session
  ├── injectUpstreamMetadata        → run_id, cost_mode, client_id, instance_id
  ├── UpstreamClient.ChatCompletions() → POST /api/v1/chat/completions
  └── Handle response: stream/retry on session/run invalidity
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
  "HTTP_PROXY": ""
}
```

All fields overrideable by env vars of the same name. `AUTH_TOKENS` and `API_KEYS` accept comma-separated values.

## Token Pool & Failover Strategy

One `tokenPool` per `AUTH_TOKEN`. **Sequential failover, not round-robin:**

1. **Only pool 0 (token-1) prewarms** on startup — creates runs for all 16 agents
2. **Backup pools stay idle** — no runs, no sessions, until needed
3. **Requests always try pool 0 first** — if it succeeds, done
4. **On rate limit (429)** — pool enters cooldown until `resetAt`, try next pool
5. **On cooldown (401, etc.)** — skip pool, try next
6. **Backups activate lazily** — first request on a backup pool creates its run and session on demand

This ensures at most one token has an active premium session at any time, preserving the 1h session quota across tokens.

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

Sessions are **per-pool, per-model** (`sessions map[model]*cachedSession`).

Lifecycle: `ensureSession()` → check cache → `refreshSession()` → POST `/api/v1/freebuff/session` → poll until active → cache with expiry.

Model switching requires ending the current session first (upstream binds sessions to models).

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
./freebuff2api -config config.json  # run locally
```

No test files exist in this project.

## Common Gotchas

- **429 rate limits** are per-model, per-token, daily quota (resets midnight Pacific / ~3pm SGT)
- **Sessions expire** after 1h — the maintenance loop refreshes them, but model switching requires explicit session end
- **Prewarm only warms pool 0** — backup pools have no runs until first failover, expect slight latency on first backup request
- **Invalid tokens** (logged out) cause 404 on StartRun — remove from config or re-login
- **Model registry** fetches from GitHub — if fetch fails, hardcoded fallback is used (may be stale)
- **`config.json` is gitignored** — don't commit tokens
