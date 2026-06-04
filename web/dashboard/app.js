const stateUrl = "/dashboard/api/state";
const refreshMs = 5000;
let paused = false;
let timer = null;
let lastState = null;

const els = {
  subtitle: document.querySelector("#subtitle"),
  refresh: document.querySelector("#refresh-button"),
  pause: document.querySelector("#pause-button"),
  refreshStatus: document.querySelector("#refresh-status"),
  metrics: {
    service: document.querySelector("#metric-service"),
    tokens: document.querySelector("#metric-tokens"),
    sessions: document.querySelector("#metric-sessions"),
    runs: document.querySelector("#metric-runs"),
    inflight: document.querySelector("#metric-inflight"),
    cooling: document.querySelector("#metric-cooling"),
    sessionStarts: document.querySelector("#metric-session-starts"),
  },
  tokenBoard: document.querySelector("#token-board"),
  timeline: document.querySelector("#timeline"),
  transitionWindow: document.querySelector("#transition-window"),
  models: document.querySelector("#models"),
  config: document.querySelector("#config-list"),
  sessions: document.querySelector("#sessions-body"),
  runs: document.querySelector("#runs-body"),
  logCount: document.querySelector("#log-count"),
  logs: document.querySelector("#log-lines"),
  raw: document.querySelector("#raw-json"),
};

els.refresh.addEventListener("click", () => loadState());
els.pause.addEventListener("click", () => {
  paused = !paused;
  els.pause.textContent = paused ? "Resume" : "Pause";
  setRefreshStatus(paused ? "Polling paused" : "Polling active");
});

function schedule() {
  clearTimeout(timer);
  timer = setTimeout(async () => {
    if (!paused) {
      await loadState();
    }
    schedule();
  }, refreshMs);
}

async function loadState() {
  try {
    const response = await fetch(stateUrl, { cache: "no-store" });
    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`);
    }
    lastState = await response.json();
    render(lastState);
    setRefreshStatus(`Updated ${formatTime(lastState.refreshed_at)}`);
  } catch (error) {
    setRefreshStatus(`Refresh failed: ${error.message}`);
  }
}

function render(state) {
  const service = state.service || {};
  els.subtitle.textContent = `${state.config?.upstream_base_url || "upstream unknown"} · refreshed ${formatTime(state.refreshed_at)}`;
  els.metrics.service.textContent = state.ok ? "OK" : "Degraded";
  els.metrics.tokens.textContent = String(service.token_count ?? 0);
  els.metrics.sessions.textContent = `${service.active_sessions ?? 0} active`;
  els.metrics.runs.textContent = String(service.total_runs ?? 0);
  els.metrics.inflight.textContent = String(service.total_inflight ?? 0);
  els.metrics.cooling.textContent = String(service.cooling_tokens ?? 0);
  els.metrics.sessionStarts.textContent = String(totalSessionStarts24h(state.tokens || []));
  els.transitionWindow.textContent = `Window ${service.session_transition_period || "--"}`;

  renderTokens(state.tokens || []);
  renderTimeline(state.tokens || [], service.session_transition_period);
  renderSessions(state.tokens || []);
  renderModels(state.models || []);
  renderConfig(state.config || {});
  renderRuns(state.tokens || []);
  renderLogs(state.logs || []);
  els.raw.textContent = JSON.stringify(state, null, 2);
}

function renderTokens(tokens) {
  els.tokenBoard.innerHTML = "";
  for (const token of tokens) {
    const card = document.createElement("article");
    card.className = `token-card ${token.state || "idle"}`;
    const session = token.session || {};
    const cooldowns = token.model_cooldowns || [];
    card.innerHTML = `
      <div class="token-title">
        <strong>${escapeHtml(token.name)}</strong>
        <span class="${badgeClass(token.state)}">${escapeHtml(token.state || "idle")}</span>
      </div>
      <div class="token-meta">
        <div class="token-facts"><span>Fingerprint</span><strong>${escapeHtml(token.fingerprint || "--")}</strong></div>
        <div class="token-facts"><span>Runs</span><strong>${token.run_count || 0}</strong></div>
        <div class="token-facts"><span>Requests</span><strong>${token.total_requests || 0}</strong></div>
        <div class="token-facts"><span>Inflight</span><strong>${token.inflight || 0}</strong></div>
        <div class="token-facts"><span>Session slot</span><strong>${escapeHtml(token.session_usage_label || "0 / 5")}</strong></div>
        <div class="token-facts"><span>Remaining slots</span><strong>${token.session_slots_remaining_24h ?? "--"}</strong></div>
      </div>
      <div class="session-meta">
        <div><span>Session</span> ${escapeHtml(session.status || "none")}</div>
        <div><span>Instance</span> ${escapeHtml(session.instance_id || "--")}</div>
        <div><span>Model</span> ${escapeHtml(session.model || "--")}</div>
        <div><span>Premium</span> ${session.premium ? "yes" : "no"}</div>
        <div><span>Remaining</span> ${formatDuration(session.remaining_ms)}</div>
        <div><span>Transition</span> ${session.transitioning ? escapeHtml(session.transition_mode || "yes") : "no"}</div>
        ${formatStartsByModel(token.session_starts_last_24h_by_model)}
        ${cooldowns.length ? `<div><span>Cooldowns</span> ${cooldowns.map((c) => escapeHtml(c.model)).join(", ")}</div>` : ""}
        ${token.last_error ? `<div><span>Error</span> ${escapeHtml(token.last_error)}</div>` : ""}
      </div>
    `;
    els.tokenBoard.appendChild(card);
  }
}

function renderTimeline(tokens, transitionPeriod) {
  els.timeline.innerHTML = "";
  for (const token of tokens) {
    const session = token.session || {};
    const remaining = Number(session.remaining_ms || 0);
    const oneHourMs = 60 * 60 * 1000;
    const pct = Math.max(0, Math.min(100, Math.round((remaining / oneHourMs) * 100)));
    const lane = document.createElement("div");
    lane.className = "timeline-lane";
    lane.innerHTML = `
      <strong>${escapeHtml(token.name)}</strong>
      <div class="bar"><span class="bar-fill ${session.transitioning ? "transition" : ""}" style="width: ${pct}%"></span></div>
      <span class="muted">${formatDuration(remaining)}</span>
    `;
    els.timeline.appendChild(lane);
  }
  if (!tokens.length) {
    els.timeline.textContent = `No token sessions. Transition window ${transitionPeriod || "--"}`;
  }
}

function renderSessions(tokens) {
  const rows = [];
  for (const token of tokens) {
    const session = token.session || {};
    if (!session.status && !session.instance_id) continue;
    rows.push({ token, session });
  }
  els.sessions.innerHTML = rows.map(({ token, session }) => `
    <tr>
      <td>${escapeHtml(token.name)}</td>
      <td>${escapeHtml(session.instance_id || "--")}</td>
      <td>${escapeHtml(session.model || "--")}</td>
      <td>${session.premium ? "yes" : "no"}</td>
      <td>${formatDuration(session.remaining_ms)}</td>
      <td>${session.transitioning ? escapeHtml(session.transition_mode || "yes") : "no"}</td>
      <td>${escapeHtml(token.session_usage_label || "0 / 5")}</td>
    </tr>
  `).join("");
  if (!rows.length) {
    els.sessions.innerHTML = `<tr><td colspan="7" class="muted">No active or queued session</td></tr>`;
  }
}

function renderLogs(logs) {
  els.logCount.textContent = `${logs.length} lines`;
  els.logs.textContent = logs.length ? logs.join("\n") : "No Freebuff2API logs captured since process start.";
}

function renderModels(models) {
  els.models.innerHTML = "";
  for (const model of models) {
    const badge = document.createElement("span");
    badge.className = `badge ${model.premium ? "warn" : model.session_required ? "good" : ""}`;
    badge.title = model.agent_id ? `Agent ${model.agent_id}` : "";
    badge.textContent = `${model.id}${model.agent_id ? " · " + model.agent_id : ""}${model.premium ? " · premium" : model.session_required ? " · session" : ""}`;
    els.models.appendChild(badge);
  }
}

function renderConfig(config) {
  const rows = [
    ["Listen", config.listen_addr],
    ["Upstream", config.upstream_base_url],
    ["User agent", config.user_agent],
    ["Auth tokens", config.auth_token_count],
    ["API auth", config.api_key_auth_enabled ? `enabled (${config.api_key_count})` : "disabled"],
    ["HTTP proxy", config.http_proxy_configured ? "configured" : "not configured"],
    ["Rotation", config.rotation_interval],
    ["Request timeout", config.request_timeout],
    ["Transition", config.session_transition_period],
    ["Session models", (config.session_required_models || []).join(", ")],
    ["Premium models", (config.premium_session_models || []).join(", ")],
  ];
  els.config.innerHTML = rows.map(([label, value]) => `<dt>${escapeHtml(label)}</dt><dd>${escapeHtml(String(value ?? "--"))}</dd>`).join("");
}

function renderRuns(tokens) {
  const rows = [];
  for (const token of tokens) {
    for (const run of token.runs || []) {
      rows.push({ token: token.name, ...run });
    }
  }
  rows.sort((a, b) => String(a.token).localeCompare(String(b.token)) || String(a.agent_id).localeCompare(String(b.agent_id)));
  els.runs.innerHTML = rows.map((run) => `
    <tr>
      <td>${escapeHtml(run.token)}</td>
      <td>${escapeHtml(run.agent_id)}</td>
      <td>${escapeHtml(run.run_id)}</td>
      <td>${formatSeconds(run.age_sec)}</td>
      <td>${run.inflight || 0}</td>
      <td>${run.request_count || 0}</td>
    </tr>
  `).join("");
}

function totalSessionStarts24h(tokens) {
  return tokens.reduce((sum, token) => sum + Number(token.session_starts_last_24h || 0), 0);
}

function formatStartsByModel(byModel) {
  if (!byModel || !Object.keys(byModel).length) return "";
  const text = Object.entries(byModel)
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([model, count]) => `${model}: ${count}`)
    .join(", ");
  return `<div><span>Starts by model</span> ${escapeHtml(text)}</div>`;
}

function setRefreshStatus(text) {
  els.refreshStatus.textContent = text;
}

function badgeClass(state) {
  if (state === "active" || state === "warm") return "badge good";
  if (state === "error") return "badge bad";
  if (state === "cooling" || state === "queued" || String(state).includes("transition")) return "badge warn";
  return "badge";
}

function formatDuration(ms) {
  if (!ms || ms <= 0) return "--";
  return formatSeconds(Math.round(ms / 1000));
}

function formatSeconds(total) {
  if (!total || total <= 0) return "--";
  const hours = Math.floor(total / 3600);
  const minutes = Math.floor((total % 3600) / 60);
  const seconds = Math.floor(total % 60);
  if (hours > 0) return `${hours}h ${minutes}m`;
  if (minutes > 0) return `${minutes}m ${seconds}s`;
  return `${seconds}s`;
}

function formatTime(value) {
  if (!value) return "--";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "--";
  return date.toLocaleTimeString();
}

function escapeHtml(value) {
  return String(value ?? "").replace(/[&<>"']/g, (char) => ({
    "&": "&amp;",
    "<": "&lt;",
    ">": "&gt;",
    '"': "&quot;",
    "'": "&#39;",
  })[char]);
}

loadState();
schedule();
