const API = {
  dashboard:
    "/metrics/ops-dashboard?lookback_hours=24&recent_run_limit=20&replay_limit=300&event_lookback_days=30&sync_alerts_from_audit=true",
  alerts: "/alerts/notifications?only_unacked=true&limit=30&sync_limit=500",
  connectors: "/events/connectors/overview?limit=200",
  connectorSla: "/events/connectors/sla?include_disabled=true",
  connectorSlaStates: "/events/connectors/sla/states?open_only=true&limit=300",
  connectorSlaSummary: "/events/connectors/sla/states/summary",
  coverage: "/events/ops/coverage?lookback_days=30",
  nlpActiveRuleset: "/events/nlp/rulesets/active?include_rules=false",
  nlpDriftMonitor: "/events/nlp/drift/monitor?limit=30",
  nlpDriftSnapshots: "/events/nlp/drift/snapshots?limit=20",
};

const state = {
  connectorOverview: null,
  connectorSla: null,
  connectorSlaStates: [],
  connectorSlaSummary: null,
  failureRows: [],
  selectedFailureId: null,
};

function esc(v) {
  return String(v ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;");
}

function fmtTs(v) {
  if (!v) return "-";
  const d = new Date(v);
  if (Number.isNaN(d.getTime())) return esc(v);
  return d.toLocaleString();
}

function fmtNum(v) {
  const n = Number(v ?? 0);
  if (!Number.isFinite(n)) return "-";
  return n.toLocaleString();
}

function fmtSigned(v, digits = 4) {
  if (v === null || v === undefined) return "-";
  const n = Number(v);
  if (!Number.isFinite(n)) return "-";
  return `${n >= 0 ? "+" : ""}${n.toFixed(digits)}`;
}

function statusChip(raw) {
  const text = String(raw || "UNKNOWN").toUpperCase();
  let cls = "status-warn";
  if (text.includes("SUCCESS") || text.includes("REPLAYED") || text.includes("OK") || text === "INFO") cls = "status-ok";
  if (text.includes("FAILED") || text.includes("CRITICAL") || text.includes("DEAD") || text.includes("ERROR")) cls = "status-danger";
  return `<span class="status-chip ${cls}">${esc(text)}</span>`;
}

async function fetchJSON(url) {
  const res = await fetch(url, { headers: { Accept: "application/json" } });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`${url} -> HTTP ${res.status}: ${text.slice(0, 260)}`);
  }
  return res.json();
}

async function postJSON(url, body) {
  const res = await fetch(url, {
    method: "POST",
    headers: {
      Accept: "application/json",
      "Content-Type": "application/json",
    },
    body: JSON.stringify(body),
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`${url} -> HTTP ${res.status}: ${text.slice(0, 300)}`);
  }
  return res.json();
}

async function postEmpty(url) {
  const res = await fetch(url, {
    method: "POST",
    headers: { Accept: "application/json" },
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`${url} -> HTTP ${res.status}: ${text.slice(0, 300)}`);
  }
  return res.json();
}

function setText(id, value) {
  const el = document.getElementById(id);
  if (el) el.textContent = String(value);
}

function showError(text) {
  const el = document.getElementById("errorBanner");
  if (!el) return;
  if (!text) {
    el.classList.add("hidden");
    el.textContent = "";
    return;
  }
  el.textContent = text;
  el.classList.remove("hidden");
}

function setWorkbenchResult(text) {
  setText("wbActionResult", text || "-");
}

function getSlaStatusByConnector(name) {
  const statuses = (state.connectorSla && state.connectorSla.statuses) || [];
  return statuses.find((x) => x.connector_name === name) || null;
}

function renderCoverageChart(daily) {
  const host = document.getElementById("coverageChart");
  if (!host) return;
  const rows = Array.isArray(daily) ? daily.slice(-30) : [];
  if (!rows.length) {
    host.innerHTML = `<p class="muted">No event data in lookback window.</p>`;
    return;
  }
  const maxTotal = Math.max(...rows.map((x) => Number(x.total_events || 0)), 1);
  host.innerHTML = rows
    .map((r) => {
      const t = Number(r.total_events || 0);
      const pos = Number(r.positive_events || 0);
      const neg = Number(r.negative_events || 0);
      const h = Math.max(4, Math.round((t / maxTotal) * 120));
      const sum = Math.max(t, 1);
      const hp = Math.round((pos / sum) * h);
      const hn = Math.round((neg / sum) * h);
      const hu = Math.max(0, h - hp - hn);
      const label = String(r.trade_date || "").slice(5);
      return `<div class="bar-col" title="${esc(r.trade_date)} total=${t}">
        <div class="bar-stack" style="height:${h}px">
          <div class="bar-neu" style="height:${hu}px"></div>
          <div class="bar-neg" style="height:${hn}px"></div>
          <div class="bar-pos" style="height:${hp}px"></div>
        </div>
        <div class="bar-label">${esc(label)}</div>
      </div>`;
    })
    .join("");
}

function renderConnectorRows(items) {
  const host = document.getElementById("connectorRows");
  if (!host) return;
  if (!Array.isArray(items) || !items.length) {
    host.innerHTML = `<tr><td colspan="8" class="muted">No connector configured.</td></tr>`;
    return;
  }
  host.innerHTML = items
    .map((x) => {
      const sla = getSlaStatusByConnector(x.connector_name);
      return `<tr>
        <td>${esc(x.connector_name)}</td>
        <td>${esc(x.source_name)}</td>
        <td>${fmtTs(x.last_run_at)}</td>
        <td>${statusChip(x.last_run_status || "IDLE")}</td>
        <td>${fmtNum(sla && sla.freshness_minutes)}</td>
        <td>${fmtNum(x.pending_failures)}</td>
        <td>${fmtNum(x.dead_failures)}</td>
        <td>${fmtTs(x.checkpoint_publish_time)}</td>
      </tr>`;
    })
    .join("");
}

function renderConnectorSlaRows(rows) {
  const host = document.getElementById("connectorSlaRows");
  if (!host) return;
  if (!Array.isArray(rows) || !rows.length) {
    host.innerHTML = `<tr><td colspan="8" class="muted">No connector SLA breach.</td></tr>`;
    return;
  }
  host.innerHTML = rows
    .map(
      (x) => `<tr>
      <td>${esc(x.connector_name)}</td>
      <td>${esc(x.breach_type)}</td>
      <td>${esc(x.stage)}</td>
      <td>${statusChip(x.severity)}</td>
      <td>${fmtNum(x.freshness_minutes)}</td>
      <td>${fmtNum(x.pending_failures)}</td>
      <td>${fmtNum(x.dead_failures)}</td>
      <td>${esc(x.message)}</td>
    </tr>`
    )
    .join("");
}

function renderConnectorSlaStateRows(rows) {
  const host = document.getElementById("connectorSlaStateRows");
  if (!host) return;
  if (!Array.isArray(rows) || !rows.length) {
    host.innerHTML = `<tr><td colspan="8" class="muted">No open SLA state.</td></tr>`;
    return;
  }
  host.innerHTML = rows
    .map((x) => {
      const level = Number(x.escalation_level || 0);
      const levelCls = level >= 2 ? "status-danger" : level >= 1 ? "status-warn" : "status-ok";
      const levelText = level > 0 ? `L${level} ${esc(x.escalation_reason || "")}` : "L0";
      return `<tr>
        <td>${esc(x.connector_name)}</td>
        <td>${esc(x.breach_type)}</td>
        <td>${esc(x.stage)}</td>
        <td>${statusChip(x.severity)}</td>
        <td>${fmtNum(x.repeat_count)}</td>
        <td><span class="status-chip ${levelCls}">${levelText}</span></td>
        <td>${fmtTs(x.last_seen_at)}</td>
        <td>${fmtTs(x.last_emitted_at)}</td>
      </tr>`;
    })
    .join("");
}
function renderRunRows(rows) {
  const host = document.getElementById("runRows");
  if (!host) return;
  if (!Array.isArray(rows) || !rows.length) {
    host.innerHTML = `<tr><td colspan="4" class="muted">No recent runs.</td></tr>`;
    return;
  }
  host.innerHTML = rows
    .map(
      (r) => `<tr>
      <td>${esc(r.run_id).slice(0, 10)}...</td>
      <td>${fmtNum(r.job_id)}</td>
      <td>${statusChip(r.status)}</td>
      <td>${fmtTs(r.started_at)}</td>
    </tr>`
    )
    .join("");
}

function renderSLARows(rows) {
  const host = document.getElementById("slaRows");
  if (!host) return;
  if (!Array.isArray(rows) || !rows.length) {
    host.innerHTML = `<tr><td colspan="4" class="muted">No scheduler SLA breach.</td></tr>`;
    return;
  }
  host.innerHTML = rows
    .map(
      (r) => `<tr>
      <td>${esc(r.job_name)}</td>
      <td>${esc(r.breach_type)}</td>
      <td>${statusChip(r.severity)}</td>
      <td>${esc(r.message)}</td>
    </tr>`
    )
    .join("");
}

function renderAlertRows(rows) {
  const host = document.getElementById("alertRows");
  if (!host) return;
  if (!Array.isArray(rows) || !rows.length) {
    host.innerHTML = `<tr><td colspan="4" class="muted">No unacked alerts.</td></tr>`;
    return;
  }
  host.innerHTML = rows
    .map(
      (x) => `<tr>
      <td>${fmtTs(x.created_at)}</td>
      <td>${statusChip(x.severity)}</td>
      <td>${esc(x.source)}</td>
      <td>${esc(x.message)}</td>
    </tr>`
    )
    .join("");
}

function renderNlpSnapshots(rows) {
  const host = document.getElementById("nlpSnapshotRows");
  if (!host) return;
  if (!Array.isArray(rows) || !rows.length) {
    host.innerHTML = `<tr><td colspan="8" class="muted">No drift snapshots.</td></tr>`;
    return;
  }
  host.innerHTML = rows
    .map((x) => {
      const warning = Array.isArray(x.alerts) ? x.alerts.filter((a) => a.severity === "WARNING").length : 0;
      const critical = Array.isArray(x.alerts) ? x.alerts.filter((a) => a.severity === "CRITICAL").length : 0;
      let alertLevel = "INFO";
      if (critical > 0) alertLevel = "CRITICAL";
      else if (warning > 0) alertLevel = "WARNING";
      return `<tr>
        <td>${fmtTs(x.created_at)}</td>
        <td>${esc(x.ruleset_version)}</td>
        <td>${fmtSigned(x.hit_rate_delta)}</td>
        <td>${fmtSigned(x.score_p50_delta)}</td>
        <td>${fmtSigned(x.contribution_delta)}</td>
        <td>${fmtSigned(x.feedback_polarity_accuracy_delta)}</td>
        <td>${fmtSigned(x.feedback_event_type_accuracy_delta)}</td>
        <td>${statusChip(alertLevel)} W:${warning} C:${critical}</td>
      </tr>`;
    })
    .join("");
}

function renderNlpMonitor(activeRuleset, monitor) {
  setText(
    "nlpActiveRuleset",
    activeRuleset && activeRuleset.version ? `${activeRuleset.version} (rules=${fmtNum(activeRuleset.rule_count)})` : "-"
  );
  if (!monitor) {
    setText("nlpLatestRisk", "-");
    setText("nlpLatestSnapshot", "-");
    setText("nlpTrendHitRate", "-");
    setText("nlpTrendScore", "-");
    setText("nlpTrendContribution", "-");
    setText("nlpTrendFeedbackPolarity", "-");
    return;
  }
  setText("nlpLatestRisk", monitor.latest_risk_level || "-");
  setText("nlpLatestSnapshot", monitor.latest_snapshot_id || "-");
  setText("nlpTrendHitRate", fmtSigned(monitor.hit_rate_delta_trend));
  setText("nlpTrendScore", fmtSigned(monitor.score_p50_delta_trend));
  setText("nlpTrendContribution", fmtSigned(monitor.contribution_delta_trend));
  setText("nlpTrendFeedbackPolarity", fmtSigned(monitor.feedback_polarity_accuracy_delta_trend));
}

function connectorFromSelect() {
  return String(document.getElementById("wbConnectorSelect")?.value || "").trim();
}

function statusFilterFromSelect() {
  return String(document.getElementById("wbStatusFilter")?.value || "PENDING").trim();
}

function errorKeywordFromInput() {
  return String(document.getElementById("wbErrorKeyword")?.value || "").trim();
}

function ensureWorkbenchConnectorOptions() {
  const select = document.getElementById("wbConnectorSelect");
  if (!select) return;
  const connectors = ((state.connectorOverview && state.connectorOverview.connectors) || []).map((x) => x.connector_name);
  const current = select.value;
  select.innerHTML = connectors.map((name) => `<option value="${esc(name)}">${esc(name)}</option>`).join("");
  if (current && connectors.includes(current)) {
    select.value = current;
  }
}

function clearWorkbenchEditor() {
  state.selectedFailureId = null;
  const rawEditor = document.getElementById("wbRawEditor");
  const eventEditor = document.getElementById("wbEventEditor");
  if (rawEditor) rawEditor.value = "{}";
  if (eventEditor) eventEditor.value = "{}";
  setText("wbSelectedMeta", "Pick one row to edit payload.");
}

function renderWorkbenchRows(rows) {
  const host = document.getElementById("wbFailureRows");
  if (!host) return;
  if (!Array.isArray(rows) || !rows.length) {
    host.innerHTML = `<tr><td colspan="7" class="muted">No failure rows in current filter.</td></tr>`;
    return;
  }
  host.innerHTML = rows
    .map(
      (x) => `<tr>
      <td><input type="checkbox" class="wb-check" data-id="${x.id}" /></td>
      <td>${fmtNum(x.id)}</td>
      <td>${statusChip(x.status)}</td>
      <td>${fmtNum(x.retry_count)}</td>
      <td>${fmtTs(x.next_retry_at)}</td>
      <td>${esc(x.last_error || "")}</td>
      <td><button type="button" class="wb-edit secondary" data-id="${x.id}">Edit</button></td>
    </tr>`
    )
    .join("");
}

function renderReplayResultRows(items) {
  const host = document.getElementById("wbReplayResultRows");
  if (!host) return;
  if (!Array.isArray(items) || !items.length) {
    host.innerHTML = `<tr><td colspan="3" class="muted">No replay result yet.</td></tr>`;
    return;
  }
  host.innerHTML = items
    .map(
      (x) => `<tr>
      <td>${fmtNum(x.failure_id)}</td>
      <td>${statusChip(x.status)}</td>
      <td>${esc(x.message || "")}</td>
    </tr>`
    )
    .join("");
}

function selectFailureForEdit(failureId) {
  const row = state.failureRows.find((x) => Number(x.id) === Number(failureId));
  if (!row) return;
  state.selectedFailureId = Number(row.id);

  const payload = row.payload || {};
  const raw = payload.raw_record && typeof payload.raw_record === "object" ? payload.raw_record : {};
  const event = payload.event && typeof payload.event === "object" ? payload.event : {};

  const rawEditor = document.getElementById("wbRawEditor");
  const eventEditor = document.getElementById("wbEventEditor");
  if (rawEditor) rawEditor.value = JSON.stringify(raw, null, 2);
  if (eventEditor) eventEditor.value = JSON.stringify(event, null, 2);

  setText(
    "wbSelectedMeta",
    `Editing failure_id=${row.id}, status=${row.status}, retry_count=${row.retry_count}, connector=${row.connector_name}`
  );
}
async function loadWorkbenchFailures() {
  const connector = connectorFromSelect();
  if (!connector) {
    state.failureRows = [];
    renderWorkbenchRows([]);
    clearWorkbenchEditor();
    return;
  }
  const status = statusFilterFromSelect();
  const keyword = errorKeywordFromInput();

  const params = new URLSearchParams();
  params.set("connector_name", connector);
  params.set("limit", "300");
  if (status !== "ALL") params.set("status", status);
  if (keyword) params.set("error_keyword", keyword);

  const rows = await fetchJSON(`/events/connectors/failures?${params.toString()}`);
  state.failureRows = rows;
  renderWorkbenchRows(rows);

  if (!rows.some((x) => Number(x.id) === Number(state.selectedFailureId))) {
    clearWorkbenchEditor();
  }
}

function parseJsonEditor(inputId, label) {
  const text = String(document.getElementById(inputId)?.value || "{}").trim();
  if (!text) return {};
  try {
    const parsed = JSON.parse(text);
    if (parsed && typeof parsed === "object" && !Array.isArray(parsed)) return parsed;
    throw new Error(`${label} must be a JSON object`);
  } catch (err) {
    throw new Error(`${label} parse failed: ${err.message}`);
  }
}

async function saveWorkbenchRepair() {
  if (!state.selectedFailureId) {
    throw new Error("Pick one failure row first.");
  }
  const connector = connectorFromSelect();
  if (!connector) {
    throw new Error("Connector is empty.");
  }

  const body = {
    connector_name: connector,
    failure_id: Number(state.selectedFailureId),
    patch_raw_record: parseJsonEditor("wbRawEditor", "raw_record"),
    patch_event: parseJsonEditor("wbEventEditor", "event"),
    reset_retry_count: Boolean(document.getElementById("wbResetRetry")?.checked),
    triggered_by: "ops_dashboard",
    note: String(document.getElementById("wbRepairNote")?.value || "").trim(),
  };

  const result = await postJSON("/events/connectors/failures/repair", body);
  setWorkbenchResult(`Repair saved. failure_id=${result.failure_id}, updated=${result.updated}`);
  await loadWorkbenchFailures();
}

function collectSelectedFailureIds() {
  return Array.from(document.querySelectorAll(".wb-check:checked"))
    .map((el) => Number(el.getAttribute("data-id") || "0"))
    .filter((x) => Number.isFinite(x) && x > 0);
}

async function replaySelectedFailures() {
  const connector = connectorFromSelect();
  if (!connector) {
    throw new Error("Connector is empty.");
  }
  const ids = collectSelectedFailureIds();
  if (!ids.length) {
    throw new Error("Pick at least one failure row.");
  }

  const result = await postJSON("/events/connectors/replay/manual", {
    connector_name: connector,
    failure_ids: ids,
    triggered_by: "ops_dashboard",
  });
  setWorkbenchResult(
    `Manual replay done: picked=${result.picked}, replayed=${result.replayed}, failed=${result.failed}, dead=${result.dead}`
  );
  renderReplayResultRows(result.items || []);
  await loadDashboard();
  await loadWorkbenchFailures();
}

async function repairReplaySelectedFailures() {
  const connector = connectorFromSelect();
  if (!connector) {
    throw new Error("Connector is empty.");
  }
  const ids = collectSelectedFailureIds();
  if (!ids.length) {
    throw new Error("Pick at least one failure row.");
  }

  const patchRaw = parseJsonEditor("wbRawEditor", "raw_record");
  const patchEvent = parseJsonEditor("wbEventEditor", "event");
  const note = String(document.getElementById("wbRepairNote")?.value || "").trim();
  const resetRetry = Boolean(document.getElementById("wbResetRetry")?.checked);

  const result = await postJSON("/events/connectors/replay/repair", {
    connector_name: connector,
    triggered_by: "ops_dashboard",
    items: ids.map((id) => ({
      failure_id: id,
      patch_raw_record: patchRaw,
      patch_event: patchEvent,
      reset_retry_count: resetRetry,
      note,
    })),
  });

  setWorkbenchResult(
    `Repair+Replay done: repaired=${result.repaired}, picked=${result.picked}, replayed=${result.replayed}, failed=${result.failed}, dead=${result.dead}`
  );
  renderReplayResultRows(result.items || []);
  await loadDashboard();
  await loadWorkbenchFailures();
}

async function syncSlaAlerts() {
  const params = new URLSearchParams({
    lookback_days: "30",
    cooldown_seconds: "900",
    warning_repeat_escalate: "3",
    critical_repeat_escalate: "2",
  });
  const result = await postEmpty(`/events/connectors/sla/sync-alerts?${params.toString()}`);
  setWorkbenchResult(
    `SLA sync: emitted=${result.emitted}, skipped=${result.skipped}, recovered=${result.recovered}, escalated=${result.escalated}`
  );
  await loadDashboard();
}
async function loadDashboard() {
  showError("");

  const [summary, alerts, connectorOverview, coverage, connectorSla, connectorSlaStates, connectorSlaSummary] = await Promise.all([
    fetchJSON(API.dashboard),
    fetchJSON(API.alerts),
    fetchJSON(API.connectors),
    fetchJSON(API.coverage),
    fetchJSON(API.connectorSla),
    fetchJSON(API.connectorSlaStates),
    fetchJSON(API.connectorSlaSummary),
  ]);

  const nlpResult = await Promise.allSettled([
    fetchJSON(API.nlpActiveRuleset),
    fetchJSON(API.nlpDriftMonitor),
    fetchJSON(API.nlpDriftSnapshots),
  ]);
  const activeRuleset = nlpResult[0].status === "fulfilled" ? nlpResult[0].value : null;
  const driftMonitor = nlpResult[1].status === "fulfilled" ? nlpResult[1].value : null;
  const driftSnapshots = nlpResult[2].status === "fulfilled" ? nlpResult[2].value : [];

  state.connectorOverview = connectorOverview;
  state.connectorSla = connectorSla;
  state.connectorSlaStates = connectorSlaStates || [];
  state.connectorSlaSummary = connectorSlaSummary || null;

  const jobs = summary.jobs || {};
  const sla = summary.sla || {};
  const alertStats = summary.alerts || {};
  const eventStats = summary.event || {};

  setText("kpiRuns24h", fmtNum(jobs.runs_last_24h));
  setText("kpiJobSlaBreaches", fmtNum((sla.breaches || []).length));
  setText("kpiConnectorSlaCritical", fmtNum(eventStats.connector_sla_critical || connectorSla.critical_count || 0));
  setText("kpiConnectorSlaOpen", fmtNum(connectorSlaSummary && connectorSlaSummary.open_states));
  setText(
    "kpiConnectorSlaEscalatedOpen",
    fmtNum(connectorSlaSummary && connectorSlaSummary.escalated_open_states)
  );
  setText("kpiCriticalAlerts", fmtNum(alertStats.unacked_critical));
  setText("kpiEventSymbols", fmtNum(coverage.symbols_covered));

  const summaryText = connectorSlaSummary
    ? `open=${connectorSlaSummary.open_states} | escalated_open=${connectorSlaSummary.escalated_open_states} | by_severity=${JSON.stringify(connectorSlaSummary.open_by_severity || {})}`
    : "-";
  setText("connectorSlaSummaryMeta", summaryText);

  setText("coverageMeta", `${coverage.lookback_days}d | generated ${fmtTs(coverage.generated_at)}`);
  setText("coverageTotal", fmtNum(coverage.total_events));
  setText("coveragePositive", fmtNum(coverage.positive_events));
  setText("coverageNegative", fmtNum(coverage.negative_events));
  setText("coverageSources", fmtNum(coverage.sources_covered));
  renderCoverageChart(coverage.daily || []);

  renderConnectorRows((connectorOverview && connectorOverview.connectors) || []);
  renderConnectorSlaRows((connectorSla && connectorSla.breaches) || []);
  renderConnectorSlaStateRows(connectorSlaStates || []);
  renderRunRows(summary.recent_runs || []);
  renderSLARows(sla.breaches || []);
  renderAlertRows(alerts || []);
  renderNlpMonitor(activeRuleset, driftMonitor);
  renderNlpSnapshots(driftSnapshots || []);

  ensureWorkbenchConnectorOptions();
  setText("lastUpdated", `Last update: ${new Date().toLocaleString()}`);
}

async function safeLoadDashboard() {
  try {
    await loadDashboard();
  } catch (err) {
    showError(`Dashboard load failed: ${err.message}`);
  }
}

async function safeLoadFailures() {
  try {
    await loadWorkbenchFailures();
  } catch (err) {
    showError(`Load failures failed: ${err.message}`);
  }
}

async function safeSaveRepair() {
  try {
    await saveWorkbenchRepair();
  } catch (err) {
    showError(`Save repair failed: ${err.message}`);
  }
}

async function safeReplaySelected() {
  try {
    await replaySelectedFailures();
  } catch (err) {
    showError(`Manual replay failed: ${err.message}`);
  }
}

async function safeRepairReplaySelected() {
  try {
    await repairReplaySelectedFailures();
  } catch (err) {
    showError(`Repair replay failed: ${err.message}`);
  }
}

async function safeSyncSla() {
  try {
    await syncSlaAlerts();
  } catch (err) {
    showError(`SLA sync failed: ${err.message}`);
  }
}

document.getElementById("refreshBtn")?.addEventListener("click", async () => {
  await safeLoadDashboard();
  await safeLoadFailures();
});

document.getElementById("syncSlaBtn")?.addEventListener("click", safeSyncSla);
document.getElementById("loadFailuresBtn")?.addEventListener("click", safeLoadFailures);
document.getElementById("wbConnectorSelect")?.addEventListener("change", safeLoadFailures);
document.getElementById("wbStatusFilter")?.addEventListener("change", safeLoadFailures);
document.getElementById("saveRepairBtn")?.addEventListener("click", safeSaveRepair);
document.getElementById("replaySelectedBtn")?.addEventListener("click", safeReplaySelected);
document.getElementById("repairReplaySelectedBtn")?.addEventListener("click", safeRepairReplaySelected);

document.getElementById("selectAllFailuresBtn")?.addEventListener("click", () => {
  const checks = Array.from(document.querySelectorAll(".wb-check"));
  if (!checks.length) return;
  const allChecked = checks.every((x) => x.checked);
  checks.forEach((x) => {
    x.checked = !allChecked;
  });
});

document.getElementById("wbErrorKeyword")?.addEventListener("keydown", (ev) => {
  if (ev.key === "Enter") {
    ev.preventDefault();
    void safeLoadFailures();
  }
});

document.getElementById("wbFailureRows")?.addEventListener("click", (ev) => {
  const target = ev.target;
  if (!(target instanceof HTMLElement)) return;
  if (!target.classList.contains("wb-edit")) return;
  const id = Number(target.getAttribute("data-id") || "0");
  if (!Number.isFinite(id) || id <= 0) return;
  selectFailureForEdit(id);
});

clearWorkbenchEditor();
renderReplayResultRows([]);

safeLoadDashboard()
  .then(() => safeLoadFailures())
  .catch((err) => showError(`Init failed: ${err.message}`));
setInterval(safeLoadDashboard, 60 * 1000);
