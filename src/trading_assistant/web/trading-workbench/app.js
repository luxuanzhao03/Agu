
const STORAGE_KEYS = {
  auth: "trading_workbench_auth_v1",
  form: "trading_workbench_form_v1",
};

const STRATEGY_PARAM_DEFAULTS = {
  trend_following: {
    entry_ma_fast: 20,
    entry_ma_slow: 60,
    atr_multiplier: 2.0,
  },
  mean_reversion: {
    z_enter: 2.0,
    z_exit: 0.0,
    min_turnover: 5000000,
  },
  multi_factor: {
    buy_threshold: 0.55,
    sell_threshold: 0.35,
    w_momentum: 0.35,
    w_quality: 0.25,
    w_low_vol: 0.2,
    w_liquidity: 0.2,
  },
  sector_rotation: {
    sector_strength: 0.6,
    risk_off_strength: 0.5,
  },
  event_driven: {
    event_score: 0.7,
    negative_event_score: 0.6,
  },
  small_capital_adaptive: {
    buy_threshold: 0.62,
    sell_threshold: 0.34,
    min_turnover20: 12000000,
    max_volatility20: 0.045,
    min_momentum20_buy: -0.02,
    max_momentum20_buy: 0.18,
    min_fundamental_score_buy: 0.45,
    max_positions: 3,
    cash_buffer_ratio: 0.1,
    risk_per_trade: 0.01,
    max_single_position: 0.35,
  },
};

const SMALL_CAPITAL_TEMPLATE_LIBRARY = {
  "2000": {
    label: "2000档（微型资金）",
    strategy_name: "small_capital_adaptive",
    small_capital_principal: 2000,
    small_capital_min_edge_bps: 140,
    initial_cash: 2000,
    lot_size: 100,
    max_single_position: 0.6,
    max_industry_exposure: 0.8,
    target_gross_exposure: 0.8,
    strategy_params: {
      buy_threshold: 0.68,
      sell_threshold: 0.32,
      min_turnover20: 15000000,
      max_volatility20: 0.035,
      min_momentum20_buy: -0.01,
      max_momentum20_buy: 0.12,
      min_fundamental_score_buy: 0.5,
      max_positions: 2,
      cash_buffer_ratio: 0.08,
      risk_per_trade: 0.008,
      max_single_position: 0.6,
    },
  },
  "5000": {
    label: "5000档（小资金）",
    strategy_name: "small_capital_adaptive",
    small_capital_principal: 5000,
    small_capital_min_edge_bps: 115,
    initial_cash: 5000,
    lot_size: 100,
    max_single_position: 0.45,
    max_industry_exposure: 0.65,
    target_gross_exposure: 0.9,
    strategy_params: {
      buy_threshold: 0.64,
      sell_threshold: 0.33,
      min_turnover20: 12000000,
      max_volatility20: 0.042,
      min_momentum20_buy: -0.015,
      max_momentum20_buy: 0.15,
      min_fundamental_score_buy: 0.47,
      max_positions: 3,
      cash_buffer_ratio: 0.1,
      risk_per_trade: 0.01,
      max_single_position: 0.45,
    },
  },
  "8000": {
    label: "8000档（准万元）",
    strategy_name: "small_capital_adaptive",
    small_capital_principal: 8000,
    small_capital_min_edge_bps: 95,
    initial_cash: 8000,
    lot_size: 100,
    max_single_position: 0.35,
    max_industry_exposure: 0.55,
    target_gross_exposure: 0.95,
    strategy_params: {
      buy_threshold: 0.62,
      sell_threshold: 0.34,
      min_turnover20: 10000000,
      max_volatility20: 0.048,
      min_momentum20_buy: -0.02,
      max_momentum20_buy: 0.18,
      min_fundamental_score_buy: 0.45,
      max_positions: 4,
      cash_buffer_ratio: 0.12,
      risk_per_trade: 0.012,
      max_single_position: 0.35,
    },
  },
};

const state = {
  strategies: [],
  latestSignalRequest: null,
  latestSignalPreps: [],
  latestBacktestRequest: null,
  latestBacktestResult: null,
  latestResearchRequest: null,
  latestResearchResult: null,
  latestMarketBars: null,
  latestRebalancePlan: null,
  selectedPrepIndex: 0,
  replaySignals: [],
  replayReport: null,
  savedFormSnapshot: null,
};

function el(id) {
  return document.getElementById(id);
}

function esc(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;");
}

function fmtNum(value, digits = 2) {
  const n = Number(value);
  if (!Number.isFinite(n)) return "-";
  return n.toLocaleString(undefined, {
    maximumFractionDigits: digits,
    minimumFractionDigits: 0,
  });
}

function fmtPct(value, digits = 2) {
  const n = Number(value);
  if (!Number.isFinite(n)) return "-";
  return `${(n * 100).toFixed(digits)}%`;
}

function fmtTs(value) {
  if (!value) return "-";
  const d = new Date(value);
  if (Number.isNaN(d.getTime())) return String(value);
  return d.toLocaleString();
}

function todayISO() {
  const d = new Date();
  return d.toISOString().slice(0, 10);
}

function minusDaysISO(days) {
  const d = new Date();
  d.setDate(d.getDate() - days);
  return d.toISOString().slice(0, 10);
}

function showGlobalError(message) {
  const banner = el("globalError");
  if (!banner) return;
  if (!message) {
    banner.classList.add("hidden");
    banner.textContent = "";
    return;
  }
  banner.textContent = message;
  banner.classList.remove("hidden");
}

function setActionMessage(message) {
  const msg = el("strategyActionMsg");
  if (msg) msg.textContent = message || "-";
}

function setExecutionMessage(message) {
  const msg = el("execSubmitMsg");
  if (msg) msg.textContent = message || "-";
}

function saveAuth() {
  const payload = {
    headerName: String(el("authHeaderName")?.value || "X-API-Key").trim() || "X-API-Key",
    apiKey: String(el("authApiKey")?.value || "").trim(),
  };
  localStorage.setItem(STORAGE_KEYS.auth, JSON.stringify(payload));
}

function loadAuth() {
  const raw = localStorage.getItem(STORAGE_KEYS.auth);
  if (!raw) return;
  try {
    const saved = JSON.parse(raw);
    if (saved && typeof saved === "object") {
      if (typeof saved.headerName === "string" && el("authHeaderName")) el("authHeaderName").value = saved.headerName;
      if (typeof saved.apiKey === "string" && el("authApiKey")) el("authApiKey").value = saved.apiKey;
    }
  } catch {
    // ignore parse failure
  }
}

function buildHeaders(json = false) {
  const headers = { Accept: "application/json" };
  if (json) headers["Content-Type"] = "application/json";

  const name = String(el("authHeaderName")?.value || "").trim();
  const key = String(el("authApiKey")?.value || "").trim();
  if (name && key) {
    headers[name] = key;
  }
  return headers;
}

async function fetchJSON(url) {
  const res = await fetch(url, { headers: buildHeaders(false) });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`HTTP ${res.status} ${url}: ${text.slice(0, 320)}`);
  }
  return res.json();
}

async function postJSON(url, body) {
  const res = await fetch(url, {
    method: "POST",
    headers: buildHeaders(true),
    body: JSON.stringify(body),
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`HTTP ${res.status} ${url}: ${text.slice(0, 320)}`);
  }
  return res.json();
}

function toNumber(raw, label, { integer = false, min = null, max = null } = {}) {
  const val = String(raw ?? "").trim();
  if (val === "") throw new Error(`${label} 不能为空`);
  const n = integer ? parseInt(val, 10) : Number(val);
  if (!Number.isFinite(n)) throw new Error(`${label} 不是有效数字`);
  if (integer && !Number.isInteger(n)) throw new Error(`${label} 必须是整数`);
  if (min !== null && n < min) throw new Error(`${label} 不能小于 ${min}`);
  if (max !== null && n > max) throw new Error(`${label} 不能大于 ${max}`);
  return n;
}

function toOptionalDate(raw) {
  const val = String(raw || "").trim();
  return val || null;
}

function setInputValue(id, value) {
  const node = el(id);
  if (!node || value === null || value === undefined) return;
  node.value = String(value);
}

function setCheckboxValue(id, checked) {
  const node = el(id);
  if (!(node instanceof HTMLInputElement)) return;
  node.checked = Boolean(checked);
}

function parseSymbols(raw) {
  const text = String(raw || "");
  return Array.from(
    new Set(
      text
        .split(/[\s,，;；]+/g)
        .map((s) => s.trim())
        .filter((s) => s)
    )
  );
}

function parseIndustryMap(raw) {
  const lines = String(raw || "")
    .split(/\r?\n/g)
    .map((x) => x.trim())
    .filter((x) => x);

  const mapping = {};
  for (const line of lines) {
    const idx = line.indexOf("=");
    if (idx <= 0) continue;
    const symbol = line.slice(0, idx).trim();
    const industry = line.slice(idx + 1).trim();
    if (!symbol || !industry) continue;
    mapping[symbol] = industry;
  }
  return mapping;
}

function normalizeDateString(value) {
  if (!value) return "";
  const raw = String(value).trim();
  if (!raw) return "";
  if (raw.length >= 10) return raw.slice(0, 10);
  return raw;
}

function parseRebalancePositions(raw) {
  const lines = String(raw || "")
    .split(/\r?\n/g)
    .map((x) => x.trim())
    .filter((x) => x);

  const rows = [];
  for (const line of lines) {
    const parts = line
      .split(/[\s,，;；]+/g)
      .map((x) => x.trim())
      .filter((x) => x);
    if (parts.length < 3) throw new Error(`current_positions 行格式错误: ${line}`);
    const symbol = parts[0];
    const quantity = toNumber(parts[1], `持仓数量(${symbol})`, { integer: true, min: 0 });
    const lastPrice = toNumber(parts[2], `最新价格(${symbol})`, { min: 0.0001 });
    rows.push({ symbol, quantity, last_price: lastPrice });
  }
  return rows;
}

function collectStrategyParams(rawMode = false) {
  const params = {};
  const inputs = Array.from(document.querySelectorAll("#strategyParamRows input[data-param-key]"));
  for (const input of inputs) {
    const key = String(input.dataset.paramKey || "");
    const type = String(input.dataset.paramType || "str").toLowerCase();
    const raw = String(input.value || "").trim();
    if (!key || raw === "") continue;
    if (rawMode) {
      params[key] = raw;
      continue;
    }
    if (type.includes("int")) params[key] = parseInt(raw, 10);
    else if (type.includes("float")) params[key] = Number(raw);
    else if (type.includes("bool")) params[key] = raw.toLowerCase() === "true" || raw === "1";
    else params[key] = raw;
  }
  return params;
}

function renderStrategyParams(strategyName, preferredRawValues = null) {
  const strategy = state.strategies.find((x) => x.name === strategyName);
  const host = el("strategyParamRows");
  const meta = el("strategyMeta");
  if (!host || !meta) return;

  if (!strategy) {
    host.innerHTML = "<p class=\"muted\">策略加载中...</p>";
    meta.textContent = "-";
    return;
  }

  const schema = strategy.params_schema || {};
  const defaults = STRATEGY_PARAM_DEFAULTS[strategyName] || {};
  const saved = preferredRawValues || {};

  meta.textContent = `${strategy.title} | 频率 ${strategy.frequency} | ${strategy.description}`;

  const keys = Object.keys(schema);
  if (!keys.length) {
    host.innerHTML = "<p class=\"muted\">该策略无可配置参数。</p>";
    return;
  }

  host.innerHTML = keys
    .map((key) => {
      const type = String(schema[key] || "str");
      const preferred = saved[key] !== undefined ? saved[key] : defaults[key];
      const value = preferred === undefined ? "" : String(preferred);
      const step = type.toLowerCase().includes("int") ? "1" : "0.0001";
      const inputType = type.toLowerCase().includes("bool") ? "text" : "number";
      const placeholder = type.toLowerCase().includes("bool") ? "true / false" : "";
      return `<div class="param-row">
        <div class="param-key">${esc(key)}</div>
        <div class="param-type">${esc(type)}</div>
        <input data-param-key="${esc(key)}" data-param-type="${esc(type)}" type="${inputType}" step="${step}" value="${esc(value)}" placeholder="${esc(placeholder)}" />
      </div>`;
    })
    .join("");
}

function buildCommonRequest() {
  const symbol = String(el("symbolInput")?.value || "").trim();
  const startDate = String(el("startDateInput")?.value || "").trim();
  const endDate = String(el("endDateInput")?.value || "").trim();
  const strategyName = String(el("strategySelect")?.value || "").trim();

  if (!symbol) throw new Error("单标的代码不能为空");
  if (!startDate || !endDate) throw new Error("开始日期和结束日期不能为空");
  if (startDate > endDate) throw new Error("开始日期必须早于或等于结束日期");
  if (!strategyName) throw new Error("请先选择策略");

  return {
    symbol,
    start_date: startDate,
    end_date: endDate,
    strategy_name: strategyName,
    strategy_params: collectStrategyParams(false),
    enable_event_enrichment: Boolean(el("eventEnrichInput")?.checked),
    enable_small_capital_mode: Boolean(el("smallCapitalModeInput")?.checked),
    small_capital_principal: toNumber(el("smallCapitalPrincipalInput")?.value, "小资金本金", { min: 100 }),
    small_capital_min_expected_edge_bps: toNumber(el("smallCapitalMinEdgeInput")?.value, "最低安全边际bps", {
      min: 0,
      max: 2000,
    }),
    event_lookback_days: toNumber(el("eventLookbackInput")?.value, "事件回看天数", { integer: true, min: 1, max: 3650 }),
    event_decay_half_life_days: toNumber(el("eventDecayInput")?.value, "事件衰减半衰期", {
      min: 0.01,
      max: 365,
    }),
    industry: String(el("industryInput")?.value || "").trim() || null,
  };
}

function buildSignalRequest() {
  const req = buildCommonRequest();

  if (el("usePositionCtx")?.checked) {
    req.current_position = {
      symbol: req.symbol,
      quantity: toNumber(el("positionQty")?.value, "持仓数量", { integer: true, min: 0 }),
      available_quantity: toNumber(el("positionAvailQty")?.value, "可卖数量", { integer: true, min: 0 }),
      avg_cost: toNumber(el("positionAvgCost")?.value, "持仓成本", { min: 0 }),
      market_value: toNumber(el("positionMarketValue")?.value, "持仓市值", { min: 0 }),
      industry: req.industry,
      last_buy_date: toOptionalDate(el("positionLastBuyDate")?.value),
    };
  }

  if (el("usePortfolioCtx")?.checked) {
    req.portfolio_snapshot = {
      total_value: toNumber(el("portfolioTotalValue")?.value, "组合总资产", { min: 0 }),
      cash: toNumber(el("portfolioCash")?.value, "现金", { min: 0 }),
      peak_value: toNumber(el("portfolioPeakValue")?.value, "峰值资产", { min: 0 }),
      current_drawdown: toNumber(el("portfolioDrawdown")?.value, "当前回撤", { min: 0, max: 1 }),
      industry_exposure: {},
    };
  }

  return req;
}

function buildBacktestRequest() {
  const req = buildCommonRequest();
  return {
    symbol: req.symbol,
    start_date: req.start_date,
    end_date: req.end_date,
    strategy_name: req.strategy_name,
    strategy_params: req.strategy_params,
    enable_event_enrichment: req.enable_event_enrichment,
    event_lookback_days: req.event_lookback_days,
    event_decay_half_life_days: req.event_decay_half_life_days,
    enable_small_capital_mode: req.enable_small_capital_mode,
    small_capital_principal: req.small_capital_principal,
    small_capital_min_expected_edge_bps: req.small_capital_min_expected_edge_bps,
    initial_cash: toNumber(el("initialCashInput")?.value, "初始资金", { min: 1000 }),
    commission_rate: toNumber(el("commissionRateInput")?.value, "手续费率", { min: 0, max: 0.02 }),
    slippage_rate: toNumber(el("slippageRateInput")?.value, "滑点率", { min: 0, max: 0.02 }),
    min_commission_cny: toNumber(el("minCommissionInput")?.value, "最低佣金", { min: 0, max: 500 }),
    stamp_duty_sell_rate: toNumber(el("stampDutyRateInput")?.value, "卖出印花税率", { min: 0, max: 0.02 }),
    transfer_fee_rate: toNumber(el("transferFeeRateInput")?.value, "过户费率", { min: 0, max: 0.01 }),
    lot_size: toNumber(el("lotSizeInput")?.value, "最小交易手数", { integer: true, min: 1 }),
    max_single_position: toNumber(el("maxSinglePositionInput")?.value, "单标的上限", { min: 0.001, max: 1 }),
  };
}

function buildResearchRequest() {
  const req = buildCommonRequest();
  const symbols = parseSymbols(el("symbolsInput")?.value);
  if (!symbols.length) throw new Error("多标的代码不能为空");

  return {
    symbols,
    start_date: req.start_date,
    end_date: req.end_date,
    strategy_name: req.strategy_name,
    strategy_params: req.strategy_params,
    enable_event_enrichment: req.enable_event_enrichment,
    enable_small_capital_mode: req.enable_small_capital_mode,
    small_capital_principal: req.small_capital_principal,
    small_capital_min_expected_edge_bps: req.small_capital_min_expected_edge_bps,
    event_lookback_days: req.event_lookback_days,
    event_decay_half_life_days: req.event_decay_half_life_days,
    industry_map: parseIndustryMap(el("industryMapInput")?.value),
    optimize_portfolio: Boolean(el("optimizePortfolioInput")?.checked),
    max_single_position: toNumber(el("maxSinglePositionInput")?.value, "单标的上限", { min: 0.001, max: 1 }),
    max_industry_exposure: toNumber(el("maxIndustryExposureInput")?.value, "行业敞口上限", { min: 0.001, max: 1 }),
    target_gross_exposure: toNumber(el("targetGrossExposureInput")?.value, "目标总仓位", { min: 0.001, max: 1 }),
  };
}

function syncBarsInputsFromStrategy() {
  const symbol = String(el("symbolInput")?.value || "").trim();
  const startDate = String(el("startDateInput")?.value || "").trim();
  const endDate = String(el("endDateInput")?.value || "").trim();
  if (el("barsSymbolInput") && symbol) el("barsSymbolInput").value = symbol;
  if (el("barsStartDateInput") && startDate) el("barsStartDateInput").value = startDate;
  if (el("barsEndDateInput") && endDate) el("barsEndDateInput").value = endDate;
}

function buildBarsRequest() {
  const symbol = String(el("barsSymbolInput")?.value || "").trim();
  const startDate = String(el("barsStartDateInput")?.value || "").trim();
  const endDate = String(el("barsEndDateInput")?.value || "").trim();
  if (!symbol) throw new Error("bars symbol 不能为空");
  if (!startDate || !endDate) throw new Error("bars 起止日期不能为空");
  if (startDate > endDate) throw new Error("bars start_date 必须 <= end_date");
  return {
    symbol,
    start_date: startDate,
    end_date: endDate,
    limit: toNumber(el("barsLimitInput")?.value, "bars limit", { integer: true, min: 1, max: 200 }),
  };
}

function syncRebalanceDefaults() {
  if (el("rebalanceTotalEquityInput") && el("initialCashInput")) {
    el("rebalanceTotalEquityInput").value = String(el("initialCashInput").value || "1000000");
  }
  if (el("rebalanceLotSizeInput") && el("lotSizeInput")) {
    el("rebalanceLotSizeInput").value = String(el("lotSizeInput").value || "100");
  }
}

function updateSmallCapitalHint() {
  const hint = el("smallCapitalHint");
  if (!hint) return;
  const enabled = Boolean(el("smallCapitalModeInput")?.checked);
  const principal = Number(el("smallCapitalPrincipalInput")?.value || 0);
  const edgeBps = Number(el("smallCapitalMinEdgeInput")?.value || 0);
  const currentStrategy = String(el("strategySelect")?.value || "").trim();
  const hasSmallCapitalStrategy = state.strategies.some((s) => s && s.name === "small_capital_adaptive");

  if (!enabled) {
    hint.textContent = "小资金模式未启用：系统按常规资金假设运行。";
    return;
  }

  if (Number.isFinite(principal) && principal > 0) {
    if (el("initialCashInput")) {
      const cur = Number(el("initialCashInput").value || 0);
      if (!Number.isFinite(cur) || cur > principal) {
        el("initialCashInput").value = String(principal);
      }
    }
    if (el("portfolioTotalValue")) el("portfolioTotalValue").value = String(principal);
    if (el("portfolioCash")) el("portfolioCash").value = String(principal);
    if (el("portfolioPeakValue")) el("portfolioPeakValue").value = String(principal);
  }
  if (hasSmallCapitalStrategy && Number.isFinite(principal) && principal > 0 && principal <= 10000) {
    const recommended = currentStrategy === "small_capital_adaptive";
    const suffix = recommended
      ? "当前策略已是 `small_capital_adaptive`。"
      : `建议切换策略为 \`small_capital_adaptive\`（当前：\`${currentStrategy || "-"}\`）。`;
    hint.textContent = `小资金模式已启用：本金=${fmtNum(principal, 0)} 元，安全边际=${fmtNum(edgeBps, 0)}bps。系统会执行“最小手数+费用+安全边际”过滤，并启用小资金仓位建议。${suffix}`;
    return;
  }

  hint.textContent = `小资金模式已启用：本金=${fmtNum(principal, 0)} 元，安全边际=${fmtNum(edgeBps, 0)}bps。系统会增加“可交易过滤（最小手数+费用）”。`;
}

function applySmallCapitalTemplate(profileKey) {
  const key = String(profileKey || "").trim();
  const profile = SMALL_CAPITAL_TEMPLATE_LIBRARY[key];
  if (!profile) throw new Error(`未找到小资金模板：${key}`);

  setCheckboxValue("smallCapitalModeInput", true);
  setInputValue("smallCapitalPrincipalInput", profile.small_capital_principal);
  setInputValue("smallCapitalMinEdgeInput", profile.small_capital_min_edge_bps);
  setInputValue("initialCashInput", profile.initial_cash);
  setInputValue("lotSizeInput", profile.lot_size);
  setInputValue("maxSinglePositionInput", profile.max_single_position);
  setInputValue("maxIndustryExposureInput", profile.max_industry_exposure);
  setInputValue("targetGrossExposureInput", profile.target_gross_exposure);
  setInputValue("portfolioTotalValue", profile.small_capital_principal);
  setInputValue("portfolioCash", profile.small_capital_principal);
  setInputValue("portfolioPeakValue", profile.small_capital_principal);
  setInputValue("rebalanceTotalEquityInput", profile.small_capital_principal);
  setInputValue("rebalanceLotSizeInput", profile.lot_size);
  setInputValue("minCommissionInput", 5);
  setInputValue("stampDutyRateInput", 0.0005);
  setInputValue("transferFeeRateInput", 0.00001);

  const strategySelect = el("strategySelect");
  let appliedStrategyName = profile.strategy_name;
  if (strategySelect instanceof HTMLSelectElement) {
    const hasTarget = Array.from(strategySelect.options).some((opt) => opt.value === profile.strategy_name);
    if (hasTarget) {
      strategySelect.value = profile.strategy_name;
    } else {
      appliedStrategyName = String(strategySelect.value || "");
    }
  }
  renderStrategyParams(appliedStrategyName, profile.strategy_params || {});

  updateSmallCapitalHint();
  syncRebalanceDefaults();
  updateRequestPreview();
  saveFormSnapshot();
  setActionMessage(
    `已套用${profile.label}模板：策略=${appliedStrategyName}，本金=${fmtNum(profile.small_capital_principal, 0)}，安全边际=${fmtNum(profile.small_capital_min_edge_bps, 0)}bps。`
  );
}

function updateRequestPreview() {
  const host = el("requestPreview");
  if (!host) return;
  try {
    const snapshot = {
      signal_request: buildSignalRequest(),
      backtest_request: buildBacktestRequest(),
      research_request: buildResearchRequest(),
    };
    try {
      snapshot.market_bars_request = buildBarsRequest();
    } catch {
      // optional for preview
    }
    try {
      snapshot.rebalance_request = buildRebalanceRequest({ allowEmptyPositions: true });
    } catch {
      // optional for preview
    }
    host.textContent = JSON.stringify(snapshot, null, 2);
  } catch (err) {
    host.textContent = `参数校验提示：${err.message}`;
  }
}

function statusChip(text, typeHint = "info") {
  const raw = String(text || "-").toUpperCase();
  let cls = "status-info";
  if (typeHint === "warn") cls = "status-warn";
  if (typeHint === "danger") cls = "status-danger";
  if (raw.includes("BUY") || raw.includes("PASS") || raw.includes("OK") || raw === "INFO") cls = "status-ok";
  if (raw.includes("WARNING") || raw.includes("WATCH")) cls = "status-warn";
  if (raw.includes("CRITICAL") || raw.includes("BLOCK") || raw.includes("SELL") || raw.includes("FAILED")) cls = "status-danger";
  return `<span class="status-chip ${cls}">${esc(raw)}</span>`;
}

function switchTab(name) {
  document.querySelectorAll(".tab").forEach((btn) => {
    const active = btn.dataset.tab === name;
    btn.classList.toggle("active", active);
  });
  document.querySelectorAll(".tab-panel").forEach((panel) => {
    const active = panel.id === `tab-${name}`;
    panel.classList.toggle("active", active);
  });
  window.location.hash = `#${name}`;
}

function bindTabEvents() {
  document.querySelectorAll(".tab").forEach((btn) => {
    btn.addEventListener("click", () => {
      switchTab(String(btn.dataset.tab || "strategy"));
    });
  });
  const hashName = String(window.location.hash || "").replace("#", "").trim();
  if (["strategy", "results", "execution"].includes(hashName)) switchTab(hashName);
}

function saveFormSnapshot() {
  const data = {};
  const selector = "#tab-strategy input[id], #tab-strategy textarea[id], #tab-strategy select[id]";
  document.querySelectorAll(selector).forEach((node) => {
    if (!(node instanceof HTMLInputElement || node instanceof HTMLTextAreaElement || node instanceof HTMLSelectElement)) return;
    if (!node.id) return;
    data[node.id] = node instanceof HTMLInputElement && node.type === "checkbox" ? node.checked : node.value;
  });
  data.strategy_param_raw = collectStrategyParams(true);
  localStorage.setItem(STORAGE_KEYS.form, JSON.stringify(data));
}

function loadFormSnapshot() {
  const raw = localStorage.getItem(STORAGE_KEYS.form);
  if (!raw) return;
  try {
    const saved = JSON.parse(raw);
    if (!saved || typeof saved !== "object") return;
    state.savedFormSnapshot = saved;
  } catch {
    state.savedFormSnapshot = null;
  }
}

function applyFormSnapshot() {
  const saved = state.savedFormSnapshot;
  if (!saved || typeof saved !== "object") return;

  const selector = "#tab-strategy input[id], #tab-strategy textarea[id], #tab-strategy select[id]";
  document.querySelectorAll(selector).forEach((node) => {
    if (!(node instanceof HTMLInputElement || node instanceof HTMLTextAreaElement || node instanceof HTMLSelectElement)) return;
    if (!node.id) return;
    if (!(node.id in saved)) return;
    if (node instanceof HTMLInputElement && node.type === "checkbox") {
      node.checked = Boolean(saved[node.id]);
    } else {
      node.value = String(saved[node.id] ?? "");
    }
  });

  const strategy = String(el("strategySelect")?.value || "");
  const rawParams = saved.strategy_param_raw && typeof saved.strategy_param_raw === "object" ? saved.strategy_param_raw : {};
  renderStrategyParams(strategy, rawParams);
}

async function loadStrategies() {
  const rows = await fetchJSON("/strategies");
  state.strategies = Array.isArray(rows) ? rows : [];

  const select = el("strategySelect");
  if (!select) return;
  const current = String(select.value || "");
  select.innerHTML = state.strategies
    .map((s) => `<option value="${esc(s.name)}">${esc(s.title)} (${esc(s.name)})</option>`)
    .join("");

  const savedName = state.savedFormSnapshot && typeof state.savedFormSnapshot.strategySelect === "string"
    ? state.savedFormSnapshot.strategySelect
    : "";
  const candidate = savedName || current || (state.strategies[0] && state.strategies[0].name) || "";
  if (candidate) {
    select.value = candidate;
    const rawParams = state.savedFormSnapshot && typeof state.savedFormSnapshot.strategy_param_raw === "object"
      ? state.savedFormSnapshot.strategy_param_raw
      : null;
    renderStrategyParams(candidate, rawParams);
  }
}

async function runSignal({ silent = false } = {}) {
  showGlobalError("");
  if (!silent) setActionMessage("正在运行信号生成...");
  const req = buildSignalRequest();
  const result = await postJSON("/signals/generate", req);
  state.latestSignalRequest = req;
  state.latestSignalPreps = Array.isArray(result) ? result : [];
  state.selectedPrepIndex = 0;
  syncBarsInputsFromStrategy();
  await loadMarketBars({ silent: true }).catch(() => {});
  renderResults();
  renderExecution();
  if (!silent) {
    setActionMessage(`信号生成完成：${state.latestSignalPreps.length} 条交易准备单`);
    switchTab("results");
  }
}

async function runBacktest({ silent = false } = {}) {
  showGlobalError("");
  if (!silent) setActionMessage("正在运行回测...");
  const req = buildBacktestRequest();
  const result = await postJSON("/backtest/run", req);
  state.latestBacktestRequest = req;
  state.latestBacktestResult = result;
  syncBarsInputsFromStrategy();
  await loadMarketBars({ silent: true }).catch(() => {});
  renderResults();
  if (!silent) {
    setActionMessage("回测完成，已更新曲线与指标");
    switchTab("results");
  }
}

async function runResearch({ silent = false } = {}) {
  showGlobalError("");
  if (!silent) setActionMessage("正在运行研究工作流...");
  const req = buildResearchRequest();
  const result = await postJSON("/research/run", req);
  state.latestResearchRequest = req;
  state.latestResearchResult = result;
  state.latestRebalancePlan = null;
  renderResults();
  if (!silent) {
    const count = Array.isArray(result.signals) ? result.signals.length : 0;
    setActionMessage(`研究工作流完成：候选信号 ${count} 条`);
    switchTab("results");
  }
}

async function loadMarketBars({ silent = false } = {}) {
  const meta = el("barsMeta");
  if (!silent && meta) meta.textContent = "正在加载 /market/bars ...";
  const req = buildBarsRequest();
  const qs = new URLSearchParams({
    symbol: req.symbol,
    start_date: req.start_date,
    end_date: req.end_date,
    limit: String(req.limit),
  });
  const resp = await fetchJSON(`/market/bars?${qs.toString()}`);
  state.latestMarketBars = resp;
  renderMarketBarRows();
  renderMarketKlineChart();
  if (meta) {
    const rows = Array.isArray(resp.bars) ? resp.bars.length : 0;
    meta.textContent = `数据源：${resp.provider || "-"} | 总行数：${fmtNum(resp.row_count, 0)} | 展示：${fmtNum(rows, 0)} 行`;
  }
}

function renderMarketBarRows() {
  const host = el("barRows");
  if (!host) return;
  const rows = state.latestMarketBars && Array.isArray(state.latestMarketBars.bars) ? state.latestMarketBars.bars : [];
  if (!rows.length) {
    host.innerHTML = '<tr><td colspan="6" class="muted">暂无K线数据。可点击“同步策略参数”后加载。</td></tr>';
    return;
  }
  host.innerHTML = rows
    .slice()
    .sort((a, b) => String(a.trade_date).localeCompare(String(b.trade_date)))
    .map(
      (bar) => `<tr>
      <td>${esc(normalizeDateString(bar.trade_date))}</td>
      <td>${fmtNum(bar.open, 4)}</td>
      <td>${fmtNum(bar.high, 4)}</td>
      <td>${fmtNum(bar.low, 4)}</td>
      <td>${fmtNum(bar.close, 4)}</td>
      <td>${fmtNum(bar.volume, 0)}</td>
    </tr>`
    )
    .join("");
}

function collectSignalMarkerMap() {
  const byDate = new Map();
  for (const sheet of state.latestSignalPreps || []) {
    if (!sheet || !sheet.signal) continue;
    const signal = sheet.signal;
    const d = normalizeDateString(signal.trade_date);
    if (!d) continue;
    if (!byDate.has(d)) byDate.set(d, []);
    byDate.get(d).push({
      action: String(signal.action || "WATCH").toUpperCase(),
      confidence: Number(signal.confidence || 0),
      symbol: String(signal.symbol || ""),
    });
  }
  return byDate;
}

function renderMarketKlineChart() {
  const host = el("marketKlineChart");
  if (!host) return;
  const rows = state.latestMarketBars && Array.isArray(state.latestMarketBars.bars) ? state.latestMarketBars.bars : [];
  if (!rows.length) {
    host.innerHTML = '<p class="chart-note">暂无K线数据。运行信号/回测后会自动尝试加载同区间数据。</p>';
    return;
  }

  const bars = rows
    .slice()
    .sort((a, b) => String(a.trade_date).localeCompare(String(b.trade_date)))
    .map((x) => ({
      trade_date: normalizeDateString(x.trade_date),
      open: Number(x.open),
      high: Number(x.high),
      low: Number(x.low),
      close: Number(x.close),
      volume: Number(x.volume),
    }))
    .filter((x) => Number.isFinite(x.open) && Number.isFinite(x.high) && Number.isFinite(x.low) && Number.isFinite(x.close));
  if (!bars.length) {
    host.innerHTML = '<p class="chart-note">K线数据格式异常，无法绘图。</p>';
    return;
  }

  const width = 1060;
  const height = 340;
  const padLeft = 54;
  const padRight = 20;
  const padTop = 18;
  const padBottom = 34;
  const usableW = width - padLeft - padRight;
  const usableH = height - padTop - padBottom;

  const lows = bars.map((x) => x.low);
  const highs = bars.map((x) => x.high);
  const minVal = Math.min(...lows);
  const maxVal = Math.max(...highs);
  const span = Math.max(maxVal - minVal, 1e-9);

  const xAt = (idx) => padLeft + ((idx + 0.5) / bars.length) * usableW;
  const yAt = (val) => padTop + ((maxVal - val) / span) * usableH;
  const candleWidth = Math.max(2, Math.min(12, usableW / Math.max(bars.length, 1) - 2));

  const gridRows = [0, 0.25, 0.5, 0.75, 1]
    .map((ratio) => {
      const yy = padTop + ratio * usableH;
      const price = maxVal - ratio * span;
      return `
        <line x1="${padLeft}" y1="${yy.toFixed(2)}" x2="${(width - padRight).toFixed(2)}" y2="${yy.toFixed(2)}" stroke="#dbe6d0" stroke-width="1" />
        <text x="6" y="${(yy + 4).toFixed(2)}" font-size="11" fill="#5f7262">${esc(fmtNum(price, 2))}</text>
      `;
    })
    .join("");

  const candles = bars
    .map((bar, idx) => {
      const xx = xAt(idx);
      const yHigh = yAt(bar.high);
      const yLow = yAt(bar.low);
      const yOpen = yAt(bar.open);
      const yClose = yAt(bar.close);
      const isUp = bar.close >= bar.open;
      const bodyTop = Math.min(yOpen, yClose);
      const bodyH = Math.max(Math.abs(yOpen - yClose), 1.3);
      const fill = isUp ? "#1c8b54" : "#cf4a22";
      return `
        <line x1="${xx.toFixed(2)}" y1="${yHigh.toFixed(2)}" x2="${xx.toFixed(2)}" y2="${yLow.toFixed(2)}" stroke="${fill}" stroke-width="1.2" />
        <rect x="${(xx - candleWidth / 2).toFixed(2)}" y="${bodyTop.toFixed(2)}" width="${candleWidth.toFixed(2)}" height="${bodyH.toFixed(2)}" fill="${fill}" opacity="0.92" />
      `;
    })
    .join("");

  const signalMap = collectSignalMarkerMap();
  const markers = bars
    .map((bar, idx) => {
      const signals = signalMap.get(bar.trade_date) || [];
      if (!signals.length) return "";
      return signals
        .slice(0, 2)
        .map((sig, sidx) => {
          const xx = xAt(idx) + sidx * 7;
          const yy = yAt(bar.high) - 9 - sidx * 7;
          let color = "#2266aa";
          if (sig.action === "BUY") color = "#137a44";
          if (sig.action === "SELL") color = "#bd3e1b";
          if (sig.action === "WATCH") color = "#9f6b10";
          return `<circle cx="${xx.toFixed(2)}" cy="${yy.toFixed(2)}" r="4" fill="${color}" stroke="#ffffff" stroke-width="1.2">
            <title>${esc(`${sig.action} ${bar.trade_date} confidence=${fmtPct(sig.confidence || 0, 2)}`)}</title>
          </circle>`;
        })
        .join("");
    })
    .join("");

  const firstDate = bars[0].trade_date;
  const lastDate = bars[bars.length - 1].trade_date;
  const lastClose = bars[bars.length - 1].close;

  host.innerHTML = `
    <svg viewBox="0 0 ${width} ${height}" role="img" aria-label="K线与信号叠加图">
      ${gridRows}
      <line x1="${padLeft}" y1="${height - padBottom}" x2="${width - padRight}" y2="${height - padBottom}" stroke="#b8c8ad" stroke-width="1.1" />
      ${candles}
      ${markers}
      <text x="${padLeft}" y="${height - 8}" font-size="11" fill="#60705d">${esc(firstDate)}</text>
      <text x="${width - padRight - 92}" y="${height - 8}" font-size="11" fill="#60705d">${esc(lastDate)}</text>
      <text x="${width - padRight - 220}" y="20" font-size="12" fill="#2f5842">最新收盘：${esc(fmtNum(lastClose, 3))}</text>
      <text x="${padLeft + 4}" y="${padTop + 12}" font-size="11" fill="#137a44">● BUY</text>
      <text x="${padLeft + 70}" y="${padTop + 12}" font-size="11" fill="#bd3e1b">● SELL</text>
      <text x="${padLeft + 140}" y="${padTop + 12}" font-size="11" fill="#9f6b10">● WATCH</text>
    </svg>
  `;
}

function renderSignalRows() {
  const host = el("signalRows");
  if (!host) return;
  const preps = state.latestSignalPreps || [];
  if (!preps.length) {
    host.innerHTML = '<tr><td colspan="10" class="muted">暂无信号结果。请先在“策略与参数页”运行信号生成。</td></tr>';
    return;
  }

  host.innerHTML = preps
    .map((sheet, idx) => {
      const active = idx === state.selectedPrepIndex ? "row-active" : "";
      const signal = sheet.signal || {};
      const risk = sheet.risk || {};
      const signalId = signal.metadata && signal.metadata.signal_id ? String(signal.metadata.signal_id) : "-";
      const fundamentalScore = signal.metadata && Number.isFinite(Number(signal.metadata.fundamental_score))
        ? Number(signal.metadata.fundamental_score)
        : null;
      const smallHit =
        Array.isArray(risk.hits) &&
        risk.hits.find((hit) => hit && hit.rule_name === "small_capital_tradability" && !hit.passed);
      return `<tr class="${active}" data-prep-idx="${idx}">
        <td>${esc(signal.symbol || "-")}</td>
        <td>${esc(signal.trade_date || "-")}</td>
        <td>${statusChip(signal.action || "WATCH")}</td>
        <td>${fmtPct(signal.confidence || 0, 2)}</td>
        <td>${fundamentalScore === null ? "-" : fmtNum(fundamentalScore, 3)}</td>
        <td>${esc((smallHit && smallHit.message) || "-")}</td>
        <td>${statusChip(risk.level || "INFO")}</td>
        <td>${risk.blocked ? statusChip("BLOCKED") : statusChip("PASS")}</td>
        <td>${esc(signal.reason || "-")}</td>
        <td><code>${esc(signalId)}</code></td>
      </tr>`;
    })
    .join("");
}

function renderRiskDetail() {
  const summary = el("riskSummary");
  const hitRows = el("riskHitRows");
  const recList = el("recommendationList");
  const disclaimer = el("disclaimerText");

  if (!summary || !hitRows || !recList || !disclaimer) return;

  const preps = state.latestSignalPreps || [];
  const sheet = preps[state.selectedPrepIndex] || null;
  if (!sheet) {
    summary.textContent = "-";
    hitRows.innerHTML = '<tr><td colspan="4" class="muted">暂无风控命中明细。</td></tr>';
    recList.innerHTML = "";
    disclaimer.textContent = "-";
    return;
  }

  summary.textContent = `风险摘要：${sheet.risk.summary || "-"}`;
  const hits = Array.isArray(sheet.risk.hits) ? sheet.risk.hits : [];
  hitRows.innerHTML = hits.length
    ? hits
        .map(
          (hit) => `<tr>
      <td>${esc(hit.rule_name)}</td>
      <td>${hit.passed ? statusChip("PASS") : statusChip("FAILED")}</td>
      <td>${statusChip(hit.level || "INFO")}</td>
      <td>${esc(hit.message || "-")}</td>
    </tr>`
        )
        .join("")
    : '<tr><td colspan="4" class="muted">该信号无额外风控命中记录。</td></tr>';

  const recs = Array.isArray(sheet.recommendations) ? sheet.recommendations : [];
  recList.innerHTML = recs.length ? recs.map((r) => `<li>${esc(r)}</li>`).join("") : "<li>无</li>";
  disclaimer.textContent = String(sheet.disclaimer || "-");
}

function renderResearchRows() {
  const host = el("researchRows");
  if (!host) return;
  const rows = state.latestResearchResult && Array.isArray(state.latestResearchResult.signals)
    ? state.latestResearchResult.signals
    : [];
  if (!rows.length) {
    host.innerHTML = '<tr><td colspan="10" class="muted">暂无研究候选结果。请先运行研究工作流。</td></tr>';
    return;
  }

  host.innerHTML = rows
    .map(
      (r) => `<tr>
      <td>${esc(r.symbol)}</td>
      <td>${esc(r.provider)}</td>
      <td>${statusChip(r.action)}</td>
      <td>${fmtPct(r.confidence, 2)}</td>
      <td>${r.fundamental_score === null || r.fundamental_score === undefined ? "-" : fmtNum(r.fundamental_score, 3)}</td>
      <td>${esc(r.small_capital_note || "-")}</td>
      <td>${r.blocked ? statusChip("BLOCKED") : statusChip("PASS")}</td>
      <td>${statusChip(r.level)}</td>
      <td>${fmtNum(r.event_rows_used, 0)}</td>
      <td><code>${esc(r.signal_id || "-")}</code></td>
    </tr>`
    )
    .join("");
}

function renderOptimizeRows() {
  const meta = el("optimizeMeta");
  const host = el("optWeightRows");
  if (!meta || !host) return;

  const optimized = state.latestResearchResult ? state.latestResearchResult.optimized_portfolio : null;
  if (!optimized || !Array.isArray(optimized.weights) || !optimized.weights.length) {
    meta.textContent = "暂无优化权重（可能未启用 optimize_portfolio 或没有可买信号）。";
    host.innerHTML = '<tr><td colspan="4" class="muted">暂无数据。</td></tr>';
    return;
  }

  meta.textContent = `未分配权重：${fmtPct(optimized.unallocated_weight || 0, 2)}`;
  host.innerHTML = optimized.weights
    .map(
      (w) => `<tr>
      <td>${esc(w.symbol)}</td>
      <td>${fmtPct(w.weight, 2)}</td>
      <td>${esc(w.industry)}</td>
      <td>${fmtNum(w.score, 4)}</td>
    </tr>`
    )
    .join("");
}

function getOptimizedWeights() {
  const optimized = state.latestResearchResult ? state.latestResearchResult.optimized_portfolio : null;
  if (!optimized || !Array.isArray(optimized.weights)) return [];
  return optimized.weights
    .map((x) => ({
      symbol: String(x.symbol || ""),
      industry: String(x.industry || "UNKNOWN"),
      weight: Number(x.weight || 0),
      score: Number(x.score || 0),
    }))
    .filter((x) => x.symbol && Number.isFinite(x.weight) && x.weight > 0);
}

function getIndustryExposureFromWeights(weights) {
  const optimized = state.latestResearchResult ? state.latestResearchResult.optimized_portfolio : null;
  if (optimized && optimized.industry_exposure && typeof optimized.industry_exposure === "object") {
    const items = Object.entries(optimized.industry_exposure)
      .map(([industry, weight]) => ({ key: industry, value: Number(weight || 0) }))
      .filter((x) => Number.isFinite(x.value) && x.value > 0)
      .sort((a, b) => b.value - a.value);
    if (items.length) return items;
  }

  const agg = {};
  for (const w of weights) {
    const k = w.industry || "UNKNOWN";
    agg[k] = (agg[k] || 0) + Number(w.weight || 0);
  }
  return Object.entries(agg)
    .map(([key, value]) => ({ key, value }))
    .filter((x) => Number.isFinite(x.value) && x.value > 0)
    .sort((a, b) => b.value - a.value);
}

function renderHorizontalBarChart(hostId, rows, valueFmt = (x) => fmtPct(x, 2)) {
  const host = el(hostId);
  if (!host) return;
  if (!rows.length) {
    host.innerHTML = '<p class="chart-note">暂无数据。</p>';
    return;
  }

  const list = rows.slice(0, 12);
  const width = 520;
  const rowH = 22;
  const pad = { top: 12, right: 16, bottom: 10, left: 120 };
  const height = pad.top + pad.bottom + list.length * rowH;
  const usableW = width - pad.left - pad.right;
  const maxVal = Math.max(...list.map((x) => Number(x.value || 0)), 1e-9);

  const bars = list
    .map((item, idx) => {
      const y = pad.top + idx * rowH;
      const raw = Number(item.value || 0);
      const w = Math.max(1, (raw / maxVal) * usableW);
      const color = idx % 2 === 0 ? "#1b7b9f" : "#20885e";
      return `
        <text x="6" y="${(y + 14).toFixed(2)}" font-size="11" fill="#4f6064">${esc(item.key)}</text>
        <rect x="${pad.left}" y="${(y + 4).toFixed(2)}" width="${w.toFixed(2)}" height="12" rx="3" fill="${color}" opacity="0.9" />
        <text x="${(pad.left + w + 6).toFixed(2)}" y="${(y + 14).toFixed(2)}" font-size="11" fill="#304249">${esc(valueFmt(raw))}</text>
      `;
    })
    .join("");

  host.innerHTML = `<svg viewBox="0 0 ${width} ${height}" role="img">${bars}</svg>`;
}

function renderWeightVisuals() {
  const weights = getOptimizedWeights();
  const meta = el("weightVisualMeta");
  if (!weights.length) {
    if (meta) meta.textContent = "暂无优化组合结果，请先运行研究工作流并启用 optimize_portfolio。";
    renderHorizontalBarChart("targetWeightChart", []);
    renderHorizontalBarChart("industryExposureChart", []);
    return;
  }

  const bySymbol = weights
    .slice()
    .sort((a, b) => b.weight - a.weight)
    .map((x) => ({ key: x.symbol, value: x.weight }));
  const byIndustry = getIndustryExposureFromWeights(weights);
  renderHorizontalBarChart("targetWeightChart", bySymbol);
  renderHorizontalBarChart("industryExposureChart", byIndustry);
  if (meta) {
    const top = bySymbol[0];
    meta.textContent = `已加载 ${fmtNum(weights.length, 0)} 个目标权重，第一权重标的 ${top.key}=${fmtPct(top.value, 2)}`;
  }
}

function buildRebalanceRequest({ allowEmptyPositions = false } = {}) {
  const weights = getOptimizedWeights();
  if (!weights.length) throw new Error("暂无优化权重，无法生成调仓建议。请先运行研究工作流。");
  const text = String(el("rebalancePositionsInput")?.value || "");
  const currentPositions = parseRebalancePositions(text);
  if (!allowEmptyPositions && !currentPositions.length) {
    throw new Error("current_positions 为空，请至少填写一行持仓。");
  }
  return {
    current_positions: currentPositions,
    target_weights: weights,
    total_equity: toNumber(el("rebalanceTotalEquityInput")?.value, "total_equity", { min: 1 }),
    lot_size: toNumber(el("rebalanceLotSizeInput")?.value, "lot_size", { integer: true, min: 1 }),
  };
}

function renderRebalanceRows() {
  const host = el("rebalanceRows");
  const meta = el("rebalanceMeta");
  if (!host || !meta) return;
  const plan = state.latestRebalancePlan;
  if (!plan || !Array.isArray(plan.orders)) {
    host.innerHTML = '<tr><td colspan="6" class="muted">暂无调仓建议，点击“生成调仓建议”后展示。</td></tr>';
    meta.textContent = "-";
    return;
  }
  if (!plan.orders.length) {
    host.innerHTML = '<tr><td colspan="6" class="muted">当前持仓已接近目标权重，无需调仓。</td></tr>';
    meta.textContent = `预计换手：${fmtPct(plan.estimated_turnover || 0, 2)}`;
    return;
  }
  host.innerHTML = plan.orders
    .map(
      (o) => `<tr>
      <td>${esc(o.symbol)}</td>
      <td>${statusChip(o.side)}</td>
      <td>${fmtPct(o.target_weight, 2)}</td>
      <td>${fmtPct(o.delta_weight, 2)}</td>
      <td>${fmtNum(o.quantity, 0)}</td>
      <td>${fmtNum(o.estimated_notional, 2)}</td>
    </tr>`
    )
    .join("");
  meta.textContent = `调仓指令 ${fmtNum(plan.orders.length, 0)} 条 | 预计换手 ${fmtPct(plan.estimated_turnover || 0, 2)}`;
}

async function runRebalancePlan() {
  const req = buildRebalanceRequest();
  const plan = await postJSON("/portfolio/rebalance/plan", req);
  state.latestRebalancePlan = plan;
  renderRebalanceRows();
}

function buildSamplePositionsFromSignals() {
  const lotSize = toNumber(el("rebalanceLotSizeInput")?.value, "lot_size", { integer: true, min: 1 });
  const total = toNumber(el("rebalanceTotalEquityInput")?.value, "total_equity", { min: 1 });
  const weights = getOptimizedWeights();
  if (!weights.length) throw new Error("暂无优化权重，无法自动构造持仓样例。");
  const lines = weights.slice(0, 8).map((w, idx) => {
    const refPrice =
      state.latestMarketBars &&
      Array.isArray(state.latestMarketBars.bars) &&
      state.latestMarketBars.bars.length &&
      String(state.latestMarketBars.symbol || "") === w.symbol
        ? Number(state.latestMarketBars.bars[state.latestMarketBars.bars.length - 1].close || 10)
        : 10 + idx * 0.6;
    const targetNotional = total * w.weight * 0.85;
    const rawQty = Math.max(lotSize, Math.floor(targetNotional / Math.max(refPrice, 0.01) / lotSize) * lotSize);
    return `${w.symbol},${rawQty},${refPrice.toFixed(2)}`;
  });
  if (el("rebalancePositionsInput")) el("rebalancePositionsInput").value = lines.join("\n");
}

function renderBacktestMetricsAndTrades() {
  const metricHost = el("backtestMetricRows");
  const tradeHost = el("tradeRows");
  const result = state.latestBacktestResult;
  if (!metricHost || !tradeHost) return;

  if (!result || !result.metrics) {
    metricHost.innerHTML = '<tr><td colspan="2" class="muted">暂无回测结果。请先运行回测。</td></tr>';
    tradeHost.innerHTML = '<tr><td colspan="7" class="muted">暂无成交记录。</td></tr>';
    return;
  }

  const m = result.metrics;
  const metrics = [
    ["总收益", fmtPct(m.total_return, 2)],
    ["年化收益", fmtPct(m.annualized_return, 2)],
    ["最大回撤", fmtPct(m.max_drawdown, 2)],
    ["夏普比率", fmtNum(m.sharpe, 4)],
    ["交易次数", fmtNum(m.trade_count, 0)],
    ["胜率", fmtPct(m.win_rate, 2)],
    ["阻断信号数", fmtNum(m.blocked_signal_count, 0)],
  ];

  metricHost.innerHTML = metrics.map(([k, v]) => `<tr><td>${esc(k)}</td><td>${esc(v)}</td></tr>`).join("");

  const trades = Array.isArray(result.trades) ? result.trades : [];
  tradeHost.innerHTML = trades.length
    ? trades
        .map(
          (t) => `<tr>
      <td>${esc(t.date)}</td>
      <td>${statusChip(t.action)}</td>
      <td>${fmtNum(t.price, 4)}</td>
      <td>${fmtNum(t.quantity, 0)}</td>
      <td>${fmtNum(t.cost, 2)}</td>
      <td>${t.blocked ? statusChip("BLOCKED") : statusChip("PASS")}</td>
      <td>${esc(t.reason || "-")}</td>
    </tr>`
        )
        .join("")
    : '<tr><td colspan="7" class="muted">暂无成交记录。</td></tr>';
}

function renderEquityChart() {
  const host = el("equityChart");
  if (!host) return;
  const points = state.latestBacktestResult && Array.isArray(state.latestBacktestResult.equity_curve)
    ? state.latestBacktestResult.equity_curve
    : [];

  if (!points.length) {
    host.innerHTML = '<p class="chart-note">暂无回测曲线。运行回测后将显示 equity_curve。</p>';
    return;
  }

  const width = 1020;
  const height = 280;
  const padLeft = 48;
  const padRight = 16;
  const padTop = 16;
  const padBottom = 32;
  const usableW = width - padLeft - padRight;
  const usableH = height - padTop - padBottom;

  const values = points.map((p) => Number(p.equity || 0));
  const minVal = Math.min(...values);
  const maxVal = Math.max(...values);
  const span = Math.max(maxVal - minVal, 1e-9);

  const x = (idx) => padLeft + (idx / Math.max(points.length - 1, 1)) * usableW;
  const y = (val) => padTop + ((maxVal - val) / span) * usableH;

  const linePath = points
    .map((p, idx) => `${idx === 0 ? "M" : "L"}${x(idx).toFixed(2)},${y(Number(p.equity || 0)).toFixed(2)}`)
    .join(" ");

  const grid = [0, 0.25, 0.5, 0.75, 1]
    .map((ratio) => {
      const yy = padTop + ratio * usableH;
      const value = maxVal - ratio * span;
      return `
        <line x1="${padLeft}" y1="${yy.toFixed(2)}" x2="${width - padRight}" y2="${yy.toFixed(2)}" stroke="#dfe8d5" stroke-width="1" />
        <text x="6" y="${(yy + 4).toFixed(2)}" font-size="11" fill="#60705d">${esc(fmtNum(value, 2))}</text>
      `;
    })
    .join("");

  const firstDate = String(points[0].date || "");
  const lastDate = String(points[points.length - 1].date || "");
  const lastEquity = values[values.length - 1];

  host.innerHTML = `
    <svg viewBox="0 0 ${width} ${height}" role="img" aria-label="回测资产曲线">
      ${grid}
      <line x1="${padLeft}" y1="${height - padBottom}" x2="${width - padRight}" y2="${height - padBottom}" stroke="#b9c8ac" stroke-width="1.2" />
      <path d="${linePath}" fill="none" stroke="#15764a" stroke-width="2.4" stroke-linecap="round" stroke-linejoin="round" />
      <circle cx="${x(points.length - 1).toFixed(2)}" cy="${y(lastEquity).toFixed(2)}" r="3.8" fill="#d44b1d" />
      <text x="${padLeft}" y="${height - 8}" font-size="11" fill="#60705d">${esc(firstDate)}</text>
      <text x="${width - padRight - 96}" y="${height - 8}" font-size="11" fill="#60705d">${esc(lastDate)}</text>
      <text x="${width - padRight - 210}" y="20" font-size="12" fill="#30553f">终值：${esc(fmtNum(lastEquity, 2))}</text>
    </svg>
  `;
}

function renderResultsKpis() {
  const preps = state.latestSignalPreps || [];
  const buys = preps.filter((x) => x && x.signal && x.signal.action === "BUY").length;
  const blocked = preps.filter((x) => x && x.risk && x.risk.blocked).length;
  const researchCount =
    state.latestResearchResult && Array.isArray(state.latestResearchResult.signals)
      ? state.latestResearchResult.signals.length
      : 0;

  el("kpiSignalCount").textContent = fmtNum(preps.length, 0);
  el("kpiBuyCount").textContent = fmtNum(buys, 0);
  el("kpiBlockedCount").textContent = fmtNum(blocked, 0);
  el("kpiResearchSignals").textContent = fmtNum(researchCount, 0);

  const m = state.latestBacktestResult && state.latestBacktestResult.metrics ? state.latestBacktestResult.metrics : null;
  el("kpiBacktestReturn").textContent = m ? fmtPct(m.total_return, 2) : "-";
  el("kpiBacktestMdd").textContent = m ? fmtPct(m.max_drawdown, 2) : "-";
  el("kpiBacktestSharpe").textContent = m ? fmtNum(m.sharpe, 3) : "-";
}

function renderResults() {
  renderResultsKpis();
  renderSignalRows();
  renderRiskDetail();
  renderResearchRows();
  renderOptimizeRows();
  renderWeightVisuals();
  renderRebalanceRows();
  renderMarketBarRows();
  renderMarketKlineChart();
  renderBacktestMetricsAndTrades();
  renderEquityChart();
}
function renderPrepRows() {
  const host = el("prepRows");
  if (!host) return;
  const preps = state.latestSignalPreps || [];
  if (!preps.length) {
    host.innerHTML = '<tr><td colspan="7" class="muted">暂无交易准备单。</td></tr>';
    return;
  }

  host.innerHTML = preps
    .map((sheet, idx) => {
      const signal = sheet.signal || {};
      const signalId = signal.metadata && signal.metadata.signal_id ? String(signal.metadata.signal_id) : "";
      const recCount = Array.isArray(sheet.recommendations) ? sheet.recommendations.length : 0;
      return `<tr>
        <td>${esc(signal.symbol || "-")}</td>
        <td>${statusChip(signal.action || "WATCH")}</td>
        <td>${fmtPct(signal.confidence || 0, 2)}</td>
        <td>${esc((sheet.risk && sheet.risk.summary) || "-")}</td>
        <td>${fmtNum(recCount, 0)}</td>
        <td><code>${esc(signalId || "-")}</code></td>
        <td><button type="button" class="badge-btn secondary" data-fill-prep="${idx}">填入执行单</button></td>
      </tr>`;
    })
    .join("");
}

function fillExecutionFormBySignal(signalLike) {
  if (!signalLike) return;
  const signalId = signalLike.signal_id || (signalLike.metadata && signalLike.metadata.signal_id) || "";
  if (el("execSignalId")) el("execSignalId").value = String(signalId || "");
  if (el("execSymbol")) el("execSymbol").value = String(signalLike.symbol || "");
  if (el("execSide")) el("execSide").value = String(signalLike.action || "BUY");
  if (el("execDate")) {
    const tradeDate = String(signalLike.trade_date || "").trim();
    el("execDate").value = tradeDate || todayISO();
  }
}

async function loadReplaySignals() {
  showGlobalError("");
  const params = new URLSearchParams();
  const symbol = String(el("replaySignalSymbol")?.value || "").trim();
  const limit = toNumber(el("replaySignalLimit")?.value, "replay signals limit", { integer: true, min: 1, max: 2000 });
  if (symbol) params.set("symbol", symbol);
  params.set("limit", String(limit));

  const rows = await fetchJSON(`/replay/signals?${params.toString()}`);
  state.replaySignals = Array.isArray(rows) ? rows : [];
  renderReplaySignalsTable();
}

function renderReplaySignalsTable() {
  const host = el("replaySignalRows");
  if (!host) return;
  const rows = state.replaySignals || [];
  if (!rows.length) {
    host.innerHTML = '<tr><td colspan="8" class="muted">暂无 replay signal 记录。</td></tr>';
    return;
  }

  host.innerHTML = rows
    .map(
      (r, idx) => `<tr>
      <td><code>${esc(String(r.signal_id || "-"))}</code></td>
      <td>${esc(r.symbol)}</td>
      <td>${esc(r.strategy_name)}</td>
      <td>${esc(r.trade_date)}</td>
      <td>${statusChip(r.action)}</td>
      <td>${fmtPct(r.confidence, 2)}</td>
      <td>${esc(r.reason || "-")}</td>
      <td><button type="button" class="badge-btn" data-fill-replay="${idx}">填入执行单</button></td>
    </tr>`
    )
    .join("");
}

async function submitExecutionRecord() {
  showGlobalError("");
  const signalId = String(el("execSignalId")?.value || "").trim();
  const symbol = String(el("execSymbol")?.value || "").trim();
  const executionDate = String(el("execDate")?.value || "").trim();
  const side = String(el("execSide")?.value || "BUY").trim();

  if (!signalId) throw new Error("signal_id 不能为空");
  if (!symbol) throw new Error("symbol 不能为空");
  if (!executionDate) throw new Error("execution_date 不能为空");

  const body = {
    signal_id: signalId,
    symbol,
    execution_date: executionDate,
    side,
    quantity: toNumber(el("execQty")?.value, "quantity", { integer: true, min: 0 }),
    price: toNumber(el("execPrice")?.value, "price", { min: 0 }),
    fee: toNumber(el("execFee")?.value, "fee", { min: 0 }),
    note: String(el("execNote")?.value || "").trim(),
  };

  const rowId = await postJSON("/replay/executions/record", body);
  setExecutionMessage(`执行回写成功，记录ID=${rowId}`);
}

async function loadReplayReport() {
  showGlobalError("");
  const params = new URLSearchParams();

  const symbol = String(el("reportSymbol")?.value || "").trim();
  const startDate = String(el("reportStartDate")?.value || "").trim();
  const endDate = String(el("reportEndDate")?.value || "").trim();
  const limit = toNumber(el("reportLimit")?.value, "replay report limit", { integer: true, min: 1, max: 2000 });

  if (symbol) params.set("symbol", symbol);
  if (startDate) params.set("start_date", startDate);
  if (endDate) params.set("end_date", endDate);
  params.set("limit", String(limit));

  const report = await fetchJSON(`/replay/report?${params.toString()}`);
  state.replayReport = report;
  renderReplayReport();
}

function renderReplayReport() {
  const host = el("replayReportRows");
  if (!host) return;
  const report = state.replayReport;

  if (!report || !Array.isArray(report.items)) {
    host.innerHTML = '<tr><td colspan="8" class="muted">暂无复盘报表数据。</td></tr>';
    el("reportSample").textContent = "-";
    el("reportFollowRate").textContent = "-";
    el("reportAvgDelay").textContent = "-";
    el("reportAvgSlip").textContent = "-";
    return;
  }

  el("reportSample").textContent = fmtNum(report.items.length, 0);
  el("reportFollowRate").textContent = fmtPct(report.follow_rate || 0, 2);
  el("reportAvgDelay").textContent = fmtNum(report.avg_delay_days || 0, 2);
  el("reportAvgSlip").textContent = fmtNum(report.avg_slippage_bps || 0, 2);

  host.innerHTML = report.items.length
    ? report.items
        .map(
          (item) => `<tr>
      <td><code>${esc(item.signal_id)}</code></td>
      <td>${esc(item.symbol)}</td>
      <td>${statusChip(item.signal_action)}</td>
      <td>${statusChip(item.executed_action || "NONE")}</td>
      <td>${fmtNum(item.executed_quantity, 0)}</td>
      <td>${fmtNum(item.executed_price, 4)}</td>
      <td>${fmtNum(item.delay_days, 0)}</td>
      <td>${item.followed ? statusChip("YES") : statusChip("NO")}</td>
    </tr>`
        )
        .join("")
    : '<tr><td colspan="8" class="muted">暂无复盘报表数据。</td></tr>';
}

function renderExecution() {
  renderPrepRows();
  renderReplaySignalsTable();
  renderReplayReport();
}

function bindResultSelectionEvents() {
  el("signalRows")?.addEventListener("click", (ev) => {
    const target = ev.target;
    if (!(target instanceof HTMLElement)) return;
    const row = target.closest("tr[data-prep-idx]");
    if (!row) return;
    const idx = Number(row.getAttribute("data-prep-idx") || "0");
    if (!Number.isFinite(idx) || idx < 0) return;
    state.selectedPrepIndex = idx;
    renderSignalRows();
    renderRiskDetail();
    renderMarketKlineChart();
  });

  el("prepRows")?.addEventListener("click", (ev) => {
    const target = ev.target;
    if (!(target instanceof HTMLElement)) return;
    const btn = target.closest("button[data-fill-prep]");
    if (!btn) return;
    const idx = Number(btn.getAttribute("data-fill-prep") || "0");
    const prep = state.latestSignalPreps[idx];
    if (!prep || !prep.signal) return;
    fillExecutionFormBySignal(prep.signal);
    switchTab("execution");
  });

  el("replaySignalRows")?.addEventListener("click", (ev) => {
    const target = ev.target;
    if (!(target instanceof HTMLElement)) return;
    const btn = target.closest("button[data-fill-replay]");
    if (!btn) return;
    const idx = Number(btn.getAttribute("data-fill-replay") || "0");
    const row = state.replaySignals[idx];
    if (!row) return;
    fillExecutionFormBySignal(row);
    switchTab("execution");
  });
}

function bindStrategyInputEvents() {
  const selector = "#tab-strategy input, #tab-strategy select, #tab-strategy textarea";
  document.querySelectorAll(selector).forEach((node) => {
    node.addEventListener("input", () => {
      updateRequestPreview();
      saveFormSnapshot();
    });
    node.addEventListener("change", () => {
      updateRequestPreview();
      saveFormSnapshot();
    });
  });

  el("strategySelect")?.addEventListener("change", () => {
    const strategyName = String(el("strategySelect")?.value || "");
    renderStrategyParams(strategyName, null);
    updateRequestPreview();
    saveFormSnapshot();
  });

  el("strategyParamRows")?.addEventListener("input", () => {
    updateRequestPreview();
    saveFormSnapshot();
  });

  ["symbolInput", "startDateInput", "endDateInput"].forEach((id) => {
    el(id)?.addEventListener("change", () => {
      syncBarsInputsFromStrategy();
      updateRequestPreview();
    });
  });

  ["initialCashInput", "lotSizeInput"].forEach((id) => {
    el(id)?.addEventListener("change", () => {
      syncRebalanceDefaults();
      updateRequestPreview();
    });
  });

  ["smallCapitalModeInput", "smallCapitalPrincipalInput", "smallCapitalMinEdgeInput"].forEach((id) => {
    el(id)?.addEventListener("change", () => {
      updateSmallCapitalHint();
      syncRebalanceDefaults();
      updateRequestPreview();
      saveFormSnapshot();
    });
  });

  el("smallCapitalTemplateActions")?.addEventListener("click", (ev) => {
    const target = ev.target;
    if (!(target instanceof HTMLElement)) return;
    const btn = target.closest("button[data-small-cap-template]");
    if (!btn) return;
    const key = btn.getAttribute("data-small-cap-template");
    try {
      applySmallCapitalTemplate(key || "");
    } catch (err) {
      showGlobalError(`套用小资金模板失败：${err.message}`);
    }
  });
}
function setDefaultDatesIfEmpty() {
  if (el("startDateInput") && !el("startDateInput").value) el("startDateInput").value = minusDaysISO(180);
  if (el("endDateInput") && !el("endDateInput").value) el("endDateInput").value = todayISO();
  if (el("barsStartDateInput") && !el("barsStartDateInput").value) el("barsStartDateInput").value = minusDaysISO(180);
  if (el("barsEndDateInput") && !el("barsEndDateInput").value) el("barsEndDateInput").value = todayISO();
  if (el("execDate") && !el("execDate").value) el("execDate").value = todayISO();
  if (el("reportStartDate") && !el("reportStartDate").value) el("reportStartDate").value = minusDaysISO(90);
  if (el("reportEndDate") && !el("reportEndDate").value) el("reportEndDate").value = todayISO();
}

async function initHandlers() {
  el("saveAuthBtn")?.addEventListener("click", () => {
    saveAuth();
    setActionMessage("认证信息已保存到本地浏览器。*");
  });

  el("refreshAllBtn")?.addEventListener("click", async () => {
    try {
      showGlobalError("");
      await loadStrategies();
      await loadReplaySignals().catch(() => {});
      await loadReplayReport().catch(() => {});
      syncBarsInputsFromStrategy();
      syncRebalanceDefaults();
      await loadMarketBars({ silent: true }).catch(() => {});
      updateRequestPreview();
      renderResults();
      renderExecution();
      el("lastUpdated").textContent = `最近更新时间：${new Date().toLocaleString()}`;
      setActionMessage("基础数据刷新完成");
    } catch (err) {
      showGlobalError(`刷新失败：${err.message}`);
    }
  });

  el("runSignalBtn")?.addEventListener("click", async () => {
    try {
      await runSignal();
      updateRequestPreview();
      saveFormSnapshot();
      el("lastUpdated").textContent = `最近更新时间：${new Date().toLocaleString()}`;
    } catch (err) {
      showGlobalError(`信号生成失败：${err.message}`);
    }
  });

  el("runBacktestBtn")?.addEventListener("click", async () => {
    try {
      await runBacktest();
      updateRequestPreview();
      saveFormSnapshot();
      el("lastUpdated").textContent = `最近更新时间：${new Date().toLocaleString()}`;
    } catch (err) {
      showGlobalError(`回测失败：${err.message}`);
    }
  });

  el("runResearchBtn")?.addEventListener("click", async () => {
    try {
      await runResearch();
      updateRequestPreview();
      saveFormSnapshot();
      el("lastUpdated").textContent = `最近更新时间：${new Date().toLocaleString()}`;
    } catch (err) {
      showGlobalError(`研究工作流失败：${err.message}`);
    }
  });

  el("runAllBtn")?.addEventListener("click", async () => {
    try {
      showGlobalError("");
      setActionMessage("一键全跑执行中：信号 -> 回测 -> 研究工作流");
      await runSignal({ silent: true });
      await runBacktest({ silent: true });
      await runResearch({ silent: true });
      setActionMessage("一键全跑完成，结果已更新。");
      switchTab("results");
      updateRequestPreview();
      saveFormSnapshot();
      el("lastUpdated").textContent = `最近更新时间：${new Date().toLocaleString()}`;
    } catch (err) {
      showGlobalError(`一键全跑失败：${err.message}`);
    }
  });

  el("syncBarsParamsBtn")?.addEventListener("click", () => {
    syncBarsInputsFromStrategy();
    updateRequestPreview();
    setActionMessage("已同步K线查询参数（symbol/start_date/end_date）");
  });

  el("loadBarsBtn")?.addEventListener("click", async () => {
    try {
      showGlobalError("");
      await loadMarketBars();
      setActionMessage("K线与信号叠加图已更新");
    } catch (err) {
      showGlobalError(`加载K线失败：${err.message}`);
    }
  });

  el("syncRebalanceParamsBtn")?.addEventListener("click", () => {
    syncRebalanceDefaults();
    updateRequestPreview();
    setActionMessage("已同步调仓默认参数（total_equity/lot_size）");
  });

  el("useBuySignalsAsPositionBtn")?.addEventListener("click", () => {
    try {
      buildSamplePositionsFromSignals();
      updateRequestPreview();
      setActionMessage("已根据研究权重生成持仓样例，可直接运行调仓建议");
    } catch (err) {
      showGlobalError(`构造持仓样例失败：${err.message}`);
    }
  });

  el("runRebalanceBtn")?.addEventListener("click", async () => {
    try {
      showGlobalError("");
      await runRebalancePlan();
      setActionMessage("调仓建议生成完成");
      switchTab("results");
    } catch (err) {
      showGlobalError(`生成调仓建议失败：${err.message}`);
    }
  });

  el("jumpToStrategyBtn")?.addEventListener("click", () => switchTab("strategy"));
  el("jumpToExecutionBtn")?.addEventListener("click", () => switchTab("execution"));

  el("loadReplaySignalsBtn")?.addEventListener("click", async () => {
    try {
      await loadReplaySignals();
      setExecutionMessage("已加载信号记录");
    } catch (err) {
      showGlobalError(`加载信号记录失败：${err.message}`);
    }
  });

  el("submitExecutionBtn")?.addEventListener("click", async () => {
    try {
      await submitExecutionRecord();
      await loadReplayReport().catch(() => {});
    } catch (err) {
      showGlobalError(`提交执行回写失败：${err.message}`);
    }
  });

  el("loadReplayReportBtn")?.addEventListener("click", async () => {
    try {
      await loadReplayReport();
      setExecutionMessage("复盘报表已刷新");
    } catch (err) {
      showGlobalError(`加载复盘报表失败：${err.message}`);
    }
  });
}

async function bootstrap() {
  try {
    loadAuth();
    loadFormSnapshot();
    setDefaultDatesIfEmpty();
    bindTabEvents();
    await loadStrategies();
    applyFormSnapshot();
    syncBarsInputsFromStrategy();
    syncRebalanceDefaults();
    updateSmallCapitalHint();

    bindStrategyInputEvents();
    bindResultSelectionEvents();
    await initHandlers();

    updateRequestPreview();
    renderResults();
    renderExecution();

    el("lastUpdated").textContent = `最近更新时间：${new Date().toLocaleString()}`;
  } catch (err) {
    showGlobalError(`初始化失败：${err.message}`);
  }
}

bootstrap();
