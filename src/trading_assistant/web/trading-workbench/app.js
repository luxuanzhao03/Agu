
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
  latestPortfolioBacktestRequest: null,
  latestPortfolioBacktestResult: null,
  latestResearchRequest: null,
  latestResearchResult: null,
  latestAutotuneRequest: null,
  latestAutotuneResult: null,
  latestAutotuneProfiles: [],
  latestAutotuneActiveProfile: null,
  latestRolloutRules: [],
  latestChallengeRequest: null,
  latestChallengeResult: null,
  latestMarketBars: null,
  latestRebalancePlan: null,
  latestHoldingTrades: [],
  latestHoldingPositions: null,
  latestHoldingAnalysis: null,
  latestHoldingAccuracyReport: null,
  latestGoLiveReadiness: null,
  selectedPrepIndex: 0,
  replaySignals: [],
  replayReport: null,
  replayAttribution: null,
  closureReport: null,
  latestCostCalibration: null,
  latestCostCalibrationHistory: [],
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

function setAutotuneMessage(message) {
  const msg = el("autotuneMsg");
  if (msg) msg.textContent = message || "-";
}

function setRolloutRuleMessage(message) {
  const msg = el("rolloutRuleMsg");
  if (msg) msg.textContent = message || "-";
}

function setChallengeMessage(message) {
  const msg = el("challengeMsg");
  if (msg) msg.textContent = message || "-";
}

function setHoldingMessage(message) {
  const msg = el("holdingMsg");
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

async function deleteJSON(url) {
  const res = await fetch(url, {
    method: "DELETE",
    headers: buildHeaders(false),
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

function parseThemeMap(raw) {
  return parseIndustryMap(raw);
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
  req.use_autotune_profile = Boolean(el("useAutotuneProfileInput")?.checked);

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
    use_autotune_profile: Boolean(el("useAutotuneProfileInput")?.checked),
    initial_cash: toNumber(el("initialCashInput")?.value, "初始资金", { min: 1000 }),
    commission_rate: toNumber(el("commissionRateInput")?.value, "手续费率", { min: 0, max: 0.02 }),
    slippage_rate: toNumber(el("slippageRateInput")?.value, "滑点率", { min: 0, max: 0.02 }),
    min_commission_cny: toNumber(el("minCommissionInput")?.value, "最低佣金", { min: 0, max: 500 }),
    stamp_duty_sell_rate: toNumber(el("stampDutyRateInput")?.value, "卖出印花税率", { min: 0, max: 0.02 }),
    transfer_fee_rate: toNumber(el("transferFeeRateInput")?.value, "过户费率", { min: 0, max: 0.01 }),
    lot_size: toNumber(el("lotSizeInput")?.value, "最小交易手数", { integer: true, min: 1 }),
    max_single_position: toNumber(el("maxSinglePositionInput")?.value, "单标的上限", { min: 0.001, max: 1 }),
    enable_realistic_cost_model: Boolean(el("realisticCostModelInput")?.checked),
    impact_cost_coeff: toNumber(el("impactCostCoeffInput")?.value, "impact_cost_coeff", { min: 0, max: 5 }),
    impact_cost_exponent: toNumber(el("impactCostExponentInput")?.value, "impact_cost_exponent", { min: 0.1, max: 2 }),
    fill_probability_floor: toNumber(el("fillProbabilityFloorInput")?.value, "fill_probability_floor", { min: 0, max: 1 }),
  };
}

function buildPortfolioBacktestRequest() {
  const req = buildCommonRequest();
  const symbols = parseSymbols(el("symbolsInput")?.value);
  if (!symbols.length) throw new Error("组合回测 symbols 不能为空");

  return {
    symbols,
    start_date: req.start_date,
    end_date: req.end_date,
    strategy_name: req.strategy_name,
    strategy_params: req.strategy_params,
    industry_map: parseIndustryMap(el("industryMapInput")?.value),
    theme_map: parseThemeMap(el("themeMapInput")?.value),
    rebalance_interval_days: toNumber(el("portfolioRebalanceIntervalInput")?.value, "rebalance_interval_days", {
      integer: true,
      min: 1,
      max: 250,
    }),
    initial_cash: toNumber(el("initialCashInput")?.value, "initial_cash", { min: 1000 }),
    target_gross_exposure: toNumber(el("targetGrossExposureInput")?.value, "target_gross_exposure", {
      min: 0.001,
      max: 1,
    }),
    cash_reserve_ratio: toNumber(el("portfolioCashReserveRatioInput")?.value, "cash_reserve_ratio", {
      min: 0,
      max: 0.95,
    }),
    max_single_position: toNumber(el("maxSinglePositionInput")?.value, "max_single_position", { min: 0.001, max: 1 }),
    max_industry_exposure: toNumber(el("maxIndustryExposureInput")?.value, "max_industry_exposure", {
      min: 0.001,
      max: 1,
    }),
    max_theme_exposure: toNumber(el("maxThemeExposureInput")?.value, "max_theme_exposure", { min: 0.001, max: 1 }),
    lot_size: toNumber(el("lotSizeInput")?.value, "lot_size", { integer: true, min: 1 }),
    commission_rate: toNumber(el("commissionRateInput")?.value, "commission_rate", { min: 0, max: 0.02 }),
    slippage_rate: toNumber(el("slippageRateInput")?.value, "slippage_rate", { min: 0, max: 0.02 }),
    min_commission_cny: toNumber(el("minCommissionInput")?.value, "min_commission_cny", { min: 0, max: 500 }),
    stamp_duty_sell_rate: toNumber(el("stampDutyRateInput")?.value, "stamp_duty_sell_rate", { min: 0, max: 0.02 }),
    transfer_fee_rate: toNumber(el("transferFeeRateInput")?.value, "transfer_fee_rate", { min: 0, max: 0.01 }),
    enable_realistic_cost_model: Boolean(el("realisticCostModelInput")?.checked),
    impact_cost_coeff: toNumber(el("impactCostCoeffInput")?.value, "impact_cost_coeff", { min: 0, max: 5 }),
    impact_cost_exponent: toNumber(el("impactCostExponentInput")?.value, "impact_cost_exponent", { min: 0.1, max: 2 }),
    fill_probability_floor: toNumber(el("fillProbabilityFloorInput")?.value, "fill_probability_floor", { min: 0, max: 1 }),
    enable_portfolio_risk_control: Boolean(el("portfolioRiskControlInput")?.checked),
    risk_max_drawdown: toNumber(el("portfolioRiskMaxDrawdownInput")?.value, "risk_max_drawdown", { min: 0, max: 1 }),
    risk_max_consecutive_losses: toNumber(
      el("portfolioRiskMaxConsecutiveLossesInput")?.value,
      "risk_max_consecutive_losses",
      { integer: true, min: 1, max: 200 }
    ),
    risk_max_daily_loss: toNumber(el("portfolioRiskMaxDailyLossInput")?.value, "risk_max_daily_loss", { min: 0, max: 1 }),
    risk_var_confidence: toNumber(el("portfolioRiskVarConfidenceInput")?.value, "risk_var_confidence", {
      min: 0.5,
      max: 0.999,
    }),
    risk_max_var: toNumber(el("portfolioRiskMaxVarInput")?.value, "risk_max_var", { min: 0, max: 1 }),
    risk_max_es: toNumber(el("portfolioRiskMaxEsInput")?.value, "risk_max_es", { min: 0, max: 1 }),
    risk_return_lookback_days: toNumber(el("portfolioRiskReturnLookbackInput")?.value, "risk_return_lookback_days", {
      integer: true,
      min: 20,
      max: 5000,
    }),
    risk_loss_streak_lookback_trades: toNumber(
      el("portfolioRiskTradeLookbackInput")?.value,
      "risk_loss_streak_lookback_trades",
      { integer: true, min: 10, max: 10000 }
    ),
    use_autotune_profile: Boolean(el("useAutotuneProfileInput")?.checked),
    enable_event_enrichment: req.enable_event_enrichment,
    enable_fundamental_enrichment: true,
    event_lookback_days: req.event_lookback_days,
    event_decay_half_life_days: req.event_decay_half_life_days,
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
    use_autotune_profile: Boolean(el("useAutotuneProfileInput")?.checked),
    industry_map: parseIndustryMap(el("industryMapInput")?.value),
    optimize_portfolio: Boolean(el("optimizePortfolioInput")?.checked),
    max_single_position: toNumber(el("maxSinglePositionInput")?.value, "单标的上限", { min: 0.001, max: 1 }),
    max_industry_exposure: toNumber(el("maxIndustryExposureInput")?.value, "行业敞口上限", { min: 0.001, max: 1 }),
    target_gross_exposure: toNumber(el("targetGrossExposureInput")?.value, "目标总仓位", { min: 0.001, max: 1 }),
  };
}

function parseAutotuneSearchSpace(raw, { tolerant = false } = {}) {
  const text = String(raw || "").trim();
  if (!text) return {};
  try {
    const parsed = JSON.parse(text);
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
      if (tolerant) return {};
      throw new Error("search_space JSON 必须是对象");
    }
    const out = {};
    for (const [key, value] of Object.entries(parsed)) {
      if (!Array.isArray(value)) {
        if (tolerant) continue;
        throw new Error(`search_space.${key} 必须是数组`);
      }
      const items = value
        .filter((x) => x !== null && x !== undefined)
        .map((x) => {
          if (typeof x === "string" || typeof x === "number" || typeof x === "boolean") return x;
          return String(x);
        });
      if (!items.length) continue;
      out[String(key)] = items;
    }
    return out;
  } catch (err) {
    if (tolerant) return {};
    throw new Error(`search_space JSON 解析失败：${err.message}`);
  }
}

function buildAutotuneRequest({ tolerantSearchSpace = false } = {}) {
  const req = buildCommonRequest();
  return {
    symbol: req.symbol,
    start_date: req.start_date,
    end_date: req.end_date,
    strategy_name: req.strategy_name,
    base_strategy_params: req.strategy_params,
    search_space: parseAutotuneSearchSpace(el("autotuneSearchSpaceInput")?.value, {
      tolerant: tolerantSearchSpace,
    }),
    max_combinations: toNumber(el("autotuneMaxCombInput")?.value, "max_combinations", {
      integer: true,
      min: 1,
      max: 5000,
    }),
    validation_ratio: toNumber(el("autotuneValidationRatioInput")?.value, "validation_ratio", { min: 0, max: 0.8 }),
    validation_weight: toNumber(el("autotuneValidationWeightInput")?.value, "validation_weight", { min: 0, max: 1 }),
    min_train_bars: toNumber(el("autotuneMinTrainBarsInput")?.value, "min_train_bars", {
      integer: true,
      min: 20,
      max: 5000,
    }),
    min_validation_bars: toNumber(el("autotuneMinValidationBarsInput")?.value, "min_validation_bars", {
      integer: true,
      min: 10,
      max: 5000,
    }),
    min_trade_count: toNumber(el("autotuneMinTradeCountInput")?.value, "min_trade_count", {
      integer: true,
      min: 0,
      max: 5000,
    }),
    low_trade_penalty: toNumber(el("autotuneLowTradePenaltyInput")?.value, "low_trade_penalty", { min: 0, max: 5 }),
    objective_weight_overfit_gap: toNumber(el("autotuneOverfitWeightInput")?.value, "objective_weight_overfit_gap", {
      min: 0,
      max: 5,
    }),
    objective_weight_stability: toNumber(el("autotuneStabilityWeightInput")?.value, "objective_weight_stability", {
      min: 0,
      max: 5,
    }),
    objective_weight_param_drift: toNumber(
      el("autotuneParamDriftWeightInput")?.value,
      "objective_weight_param_drift",
      {
        min: 0,
        max: 5,
      }
    ),
    objective_weight_return_variance: toNumber(
      el("autotuneReturnVarWeightInput")?.value,
      "objective_weight_return_variance",
      { min: 0, max: 5 }
    ),
    stability_eval_top_n: toNumber(el("autotuneStabilityTopNInput")?.value, "stability_eval_top_n", {
      integer: true,
      min: 0,
      max: 500,
    }),
    walk_forward_slices: toNumber(el("autotuneWalkForwardSlicesInput")?.value, "walk_forward_slices", {
      integer: true,
      min: 0,
      max: 12,
    }),
    low_sample_penalty: toNumber(el("autotuneLowSamplePenaltyInput")?.value, "low_sample_penalty", { min: 0, max: 5 }),
    auto_apply: Boolean(el("autotuneAutoApplyInput")?.checked),
    apply_scope: String(el("autotuneApplyScopeInput")?.value || "GLOBAL"),
    min_improvement_to_apply: toNumber(el("autotuneMinImprovementInput")?.value, "min_improvement_to_apply", {
      min: -5,
      max: 5,
    }),
    apply_require_validation: Boolean(el("autotuneRequireValidationInput")?.checked),
    apply_min_validation_total_return: toNumber(
      el("autotuneMinValidationReturnInput")?.value,
      "apply_min_validation_total_return",
      { min: -1, max: 5 }
    ),
    apply_max_train_validation_gap: toNumber(el("autotuneMaxGapInput")?.value, "apply_max_train_validation_gap", {
      min: 0,
      max: 5,
    }),
    apply_min_walk_forward_samples: toNumber(
      el("autotuneMinWfSamplesInput")?.value,
      "apply_min_walk_forward_samples",
      { integer: true, min: 0, max: 20 }
    ),
    create_governance_draft: Boolean(el("autotuneCreateGovDraftInput")?.checked),
    governance_submit_review: Boolean(el("autotuneSubmitReviewInput")?.checked),
    run_by: String(el("autotuneRunByInput")?.value || "").trim() || "workbench_user",
    enable_event_enrichment: req.enable_event_enrichment,
    enable_fundamental_enrichment: true,
    enable_small_capital_mode: req.enable_small_capital_mode,
    small_capital_principal: req.small_capital_principal,
    small_capital_min_expected_edge_bps: req.small_capital_min_expected_edge_bps,
    event_lookback_days: req.event_lookback_days,
    event_decay_half_life_days: req.event_decay_half_life_days,
    initial_cash: toNumber(el("initialCashInput")?.value, "initial_cash", { min: 1000 }),
    commission_rate: toNumber(el("commissionRateInput")?.value, "commission_rate", { min: 0, max: 0.02 }),
    slippage_rate: toNumber(el("slippageRateInput")?.value, "slippage_rate", { min: 0, max: 0.02 }),
    min_commission_cny: toNumber(el("minCommissionInput")?.value, "min_commission_cny", { min: 0, max: 500 }),
    stamp_duty_sell_rate: toNumber(el("stampDutyRateInput")?.value, "stamp_duty_sell_rate", { min: 0, max: 0.02 }),
    transfer_fee_rate: toNumber(el("transferFeeRateInput")?.value, "transfer_fee_rate", { min: 0, max: 0.01 }),
    lot_size: toNumber(el("lotSizeInput")?.value, "lot_size", { integer: true, min: 1 }),
    max_single_position: toNumber(el("maxSinglePositionInput")?.value, "max_single_position", { min: 0.001, max: 1 }),
    enable_realistic_cost_model: Boolean(el("autotuneRealisticCostInput")?.checked),
    impact_cost_coeff: toNumber(el("autotuneImpactCoeffInput")?.value, "impact_cost_coeff", { min: 0, max: 5 }),
    impact_cost_exponent: toNumber(el("autotuneImpactExponentInput")?.value, "impact_cost_exponent", { min: 0.1, max: 2 }),
    fill_probability_floor: toNumber(el("autotuneFillProbFloorInput")?.value, "fill_probability_floor", {
      min: 0,
      max: 1,
    }),
  };
}

function parseChallengeStrategyNames() {
  const picks = Array.from(document.querySelectorAll(".challenge-strategy-picker"));
  const selected = [];
  for (const node of picks) {
    if (!(node instanceof HTMLInputElement)) continue;
    if (!node.checked) continue;
    const name = String(node.value || "").trim().toLowerCase();
    if (!name || selected.includes(name)) continue;
    selected.push(name);
  }
  return selected;
}

function parseChallengeBaseParamsMap(raw, { tolerant = false } = {}) {
  const text = String(raw || "").trim();
  if (!text) return {};
  try {
    const parsed = JSON.parse(text);
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
      if (tolerant) return {};
      throw new Error("base_strategy_params_map JSON 必须是对象");
    }
    const out = {};
    for (const [strategyName, value] of Object.entries(parsed)) {
      if (!value || typeof value !== "object" || Array.isArray(value)) {
        if (tolerant) continue;
        throw new Error(`base_strategy_params_map.${strategyName} 必须是对象`);
      }
      const params = {};
      for (const [key, item] of Object.entries(value)) {
        if (item === null || item === undefined) continue;
        if (typeof item === "string" || typeof item === "number" || typeof item === "boolean") {
          params[String(key)] = item;
        } else {
          params[String(key)] = String(item);
        }
      }
      out[String(strategyName).trim().toLowerCase()] = params;
    }
    return out;
  } catch (err) {
    if (tolerant) return {};
    throw new Error(`base_strategy_params_map JSON 解析失败：${err.message}`);
  }
}

function parseChallengeSearchSpaceMap(raw, { tolerant = false } = {}) {
  const text = String(raw || "").trim();
  if (!text) return {};
  try {
    const parsed = JSON.parse(text);
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
      if (tolerant) return {};
      throw new Error("search_space_map JSON 必须是对象");
    }
    const out = {};
    for (const [strategyName, value] of Object.entries(parsed)) {
      if (!value || typeof value !== "object" || Array.isArray(value)) {
        if (tolerant) continue;
        throw new Error(`search_space_map.${strategyName} 必须是对象`);
      }
      const strategySpace = {};
      for (const [paramName, candidates] of Object.entries(value)) {
        if (!Array.isArray(candidates)) {
          if (tolerant) continue;
          throw new Error(`search_space_map.${strategyName}.${paramName} 必须是数组`);
        }
        const filtered = candidates
          .filter((x) => x !== null && x !== undefined)
          .map((x) => {
            if (typeof x === "string" || typeof x === "number" || typeof x === "boolean") return x;
            return String(x);
          });
        if (!filtered.length) continue;
        strategySpace[String(paramName)] = filtered;
      }
      out[String(strategyName).trim().toLowerCase()] = strategySpace;
    }
    return out;
  } catch (err) {
    if (tolerant) return {};
    throw new Error(`search_space_map JSON 解析失败：${err.message}`);
  }
}

function buildStrategyChallengeRequest({ tolerantSearchSpace = false } = {}) {
  const req = buildCommonRequest();
  const selectedStrategies = parseChallengeStrategyNames();
  const baseMap = parseChallengeBaseParamsMap(el("challengeBaseParamsMapInput")?.value, {
    tolerant: tolerantSearchSpace,
  });
  const searchMap = parseChallengeSearchSpaceMap(el("challengeSearchSpaceMapInput")?.value, {
    tolerant: tolerantSearchSpace,
  });

  const currentParams = collectStrategyParams(false);
  if (req.strategy_name && Object.keys(currentParams).length && !(req.strategy_name in baseMap)) {
    baseMap[req.strategy_name] = currentParams;
  }

  return {
    symbol: req.symbol,
    start_date: req.start_date,
    end_date: req.end_date,
    strategy_names: selectedStrategies,
    base_strategy_params_map: baseMap,
    search_space_map: searchMap,
    per_strategy_max_combinations: toNumber(
      el("challengePerStrategyMaxCombInput")?.value,
      "per_strategy_max_combinations",
      { integer: true, min: 1, max: 5000 }
    ),
    validation_ratio: toNumber(el("challengeValidationRatioInput")?.value, "validation_ratio", { min: 0, max: 0.8 }),
    validation_weight: toNumber(el("challengeValidationWeightInput")?.value, "validation_weight", { min: 0, max: 1 }),
    min_train_bars: toNumber(el("challengeMinTrainBarsInput")?.value, "min_train_bars", {
      integer: true,
      min: 20,
      max: 5000,
    }),
    min_validation_bars: toNumber(el("challengeMinValidationBarsInput")?.value, "min_validation_bars", {
      integer: true,
      min: 10,
      max: 5000,
    }),
    min_trade_count: toNumber(el("challengeMinTradeCountInput")?.value, "min_trade_count", {
      integer: true,
      min: 0,
      max: 5000,
    }),
    low_trade_penalty: toNumber(el("autotuneLowTradePenaltyInput")?.value, "low_trade_penalty", { min: 0, max: 5 }),
    objective_weight_total_return: 0.55,
    objective_weight_annualized_return: 0.2,
    objective_weight_sharpe: 0.2,
    objective_weight_win_rate: 0.1,
    objective_weight_trade_count: 0.05,
    objective_weight_max_drawdown: 0.35,
    objective_weight_blocked_ratio: 0.05,
    objective_weight_overfit_gap: toNumber(el("autotuneOverfitWeightInput")?.value, "objective_weight_overfit_gap", {
      min: 0,
      max: 5,
    }),
    objective_weight_stability: toNumber(el("autotuneStabilityWeightInput")?.value, "objective_weight_stability", {
      min: 0,
      max: 5,
    }),
    objective_weight_param_drift: toNumber(
      el("autotuneParamDriftWeightInput")?.value,
      "objective_weight_param_drift",
      { min: 0, max: 5 }
    ),
    objective_weight_return_variance: toNumber(
      el("autotuneReturnVarWeightInput")?.value,
      "objective_weight_return_variance",
      { min: 0, max: 5 }
    ),
    stability_eval_top_n: toNumber(el("challengeStabilityTopNInput")?.value, "stability_eval_top_n", {
      integer: true,
      min: 0,
      max: 500,
    }),
    walk_forward_slices: toNumber(el("challengeWalkForwardSlicesInput")?.value, "walk_forward_slices", {
      integer: true,
      min: 0,
      max: 12,
    }),
    low_sample_penalty: toNumber(el("challengeLowSamplePenaltyInput")?.value, "low_sample_penalty", { min: 0, max: 5 }),
    enable_event_enrichment: req.enable_event_enrichment,
    enable_fundamental_enrichment: true,
    enable_small_capital_mode: req.enable_small_capital_mode,
    small_capital_principal: req.small_capital_principal,
    small_capital_min_expected_edge_bps: req.small_capital_min_expected_edge_bps,
    fundamental_max_staleness_days: 540,
    event_lookback_days: req.event_lookback_days,
    event_decay_half_life_days: req.event_decay_half_life_days,
    initial_cash: toNumber(el("initialCashInput")?.value, "initial_cash", { min: 1000 }),
    commission_rate: toNumber(el("commissionRateInput")?.value, "commission_rate", { min: 0, max: 0.02 }),
    slippage_rate: toNumber(el("slippageRateInput")?.value, "slippage_rate", { min: 0, max: 0.02 }),
    min_commission_cny: toNumber(el("minCommissionInput")?.value, "min_commission_cny", { min: 0, max: 500 }),
    stamp_duty_sell_rate: toNumber(el("stampDutyRateInput")?.value, "stamp_duty_sell_rate", { min: 0, max: 0.02 }),
    transfer_fee_rate: toNumber(el("transferFeeRateInput")?.value, "transfer_fee_rate", { min: 0, max: 0.01 }),
    lot_size: toNumber(el("lotSizeInput")?.value, "lot_size", { integer: true, min: 1 }),
    max_single_position: toNumber(el("maxSinglePositionInput")?.value, "max_single_position", { min: 0.001, max: 1 }),
    enable_realistic_cost_model: Boolean(el("realisticCostModelInput")?.checked),
    impact_cost_coeff: toNumber(el("impactCostCoeffInput")?.value, "impact_cost_coeff", { min: 0, max: 5 }),
    impact_cost_exponent: toNumber(el("impactCostExponentInput")?.value, "impact_cost_exponent", { min: 0.1, max: 2 }),
    fill_probability_floor: toNumber(el("fillProbabilityFloorInput")?.value, "fill_probability_floor", { min: 0, max: 1 }),
    gate_require_validation: Boolean(el("challengeGateRequireValidationInput")?.checked),
    gate_min_validation_total_return: toNumber(
      el("challengeGateMinValidationReturnInput")?.value,
      "gate_min_validation_total_return",
      { min: -1, max: 5 }
    ),
    gate_max_validation_drawdown: toNumber(
      el("challengeGateMaxValidationDrawdownInput")?.value,
      "gate_max_validation_drawdown",
      { min: 0, max: 1 }
    ),
    gate_min_validation_sharpe: toNumber(
      el("challengeGateMinValidationSharpeInput")?.value,
      "gate_min_validation_sharpe",
      { min: -10, max: 20 }
    ),
    gate_min_validation_trade_count: toNumber(
      el("challengeGateMinValidationTradeCountInput")?.value,
      "gate_min_validation_trade_count",
      { integer: true, min: 0, max: 10000 }
    ),
    gate_min_walk_forward_samples: toNumber(el("challengeGateMinWfSamplesInput")?.value, "gate_min_walk_forward_samples", {
      integer: true,
      min: 0,
      max: 50,
    }),
    gate_max_walk_forward_return_std: toNumber(
      el("challengeGateMaxWfStdInput")?.value,
      "gate_max_walk_forward_return_std",
      { min: 0, max: 5 }
    ),
    rank_weight_validation_return: toNumber(el("challengeRankWeightReturnInput")?.value, "rank_weight_validation_return", {
      min: 0,
      max: 5,
    }),
    rank_weight_validation_sharpe: toNumber(el("challengeRankWeightSharpeInput")?.value, "rank_weight_validation_sharpe", {
      min: 0,
      max: 5,
    }),
    rank_weight_stability: toNumber(el("challengeRankWeightStabilityInput")?.value, "rank_weight_stability", {
      min: 0,
      max: 5,
    }),
    rank_weight_drawdown_penalty: toNumber(
      el("challengeRankWeightDrawdownInput")?.value,
      "rank_weight_drawdown_penalty",
      { min: 0, max: 5 }
    ),
    rank_weight_variance_penalty: toNumber(
      el("challengeRankWeightVarianceInput")?.value,
      "rank_weight_variance_penalty",
      { min: 0, max: 5 }
    ),
    rollout_gray_days: toNumber(el("challengeRolloutGrayDaysInput")?.value, "rollout_gray_days", {
      integer: true,
      min: 7,
      max: 20,
    }),
    run_by: String(el("challengeRunByInput")?.value || "").trim() || "workbench_user",
  };
}

function getHoldingStrategyName() {
  const explicit = String(el("holdingAnalyzeStrategyInput")?.value || "").trim();
  if (explicit) return explicit;
  return String(el("strategySelect")?.value || "").trim();
}

function buildHoldingTradeRequest() {
  const tradeDate = String(el("holdingTradeDateInput")?.value || "").trim() || todayISO();
  const symbol = String(el("holdingTradeSymbolInput")?.value || "").trim().toUpperCase();
  if (!symbol) throw new Error("holding symbol 不能为空");
  const side = String(el("holdingTradeSideInput")?.value || "BUY").trim().toUpperCase();
  if (!["BUY", "SELL"].includes(side)) throw new Error("holding side 必须是 BUY 或 SELL");
  const referenceRaw = String(el("holdingTradeReferencePriceInput")?.value || "").trim();
  const executedAtRaw = String(el("holdingTradeExecutedAtInput")?.value || "").trim();
  const partialFill = Boolean(el("holdingTradePartialFillInput")?.checked);
  const unfilledReason = String(el("holdingTradeUnfilledReasonInput")?.value || "").trim();
  if (partialFill && !unfilledReason) throw new Error("is_partial_fill=true 时请填写 unfilled_reason");
  return {
    trade_date: tradeDate,
    symbol,
    symbol_name: String(el("holdingTradeNameInput")?.value || "").trim(),
    side,
    price: toNumber(el("holdingTradePriceInput")?.value, "holding price", { min: 0.0001 }),
    lots: toNumber(el("holdingTradeLotsInput")?.value, "holding lots", { integer: true, min: 1, max: 1_000_000 }),
    lot_size: toNumber(el("holdingTradeLotSizeInput")?.value, "holding lot_size", {
      integer: true,
      min: 1,
      max: 10_000,
    }),
    fee: toNumber(el("holdingTradeFeeInput")?.value, "holding fee", { min: 0, max: 1_000_000 }),
    reference_price: referenceRaw ? toNumber(referenceRaw, "holding reference_price", { min: 0.0001 }) : null,
    executed_at: executedAtRaw || null,
    is_partial_fill: partialFill,
    unfilled_reason: unfilledReason,
    note: String(el("holdingTradeNoteInput")?.value || "").trim(),
  };
}

function buildHoldingTradeQuery() {
  const params = new URLSearchParams();
  const symbol = String(el("holdingTradeFilterSymbolInput")?.value || "").trim().toUpperCase();
  const startDate = String(el("holdingTradeFilterStartDateInput")?.value || "").trim();
  const endDate = String(el("holdingTradeFilterEndDateInput")?.value || "").trim();
  const limit = toNumber(el("holdingTradeFilterLimitInput")?.value, "holding trade limit", {
    integer: true,
    min: 1,
    max: 5000,
  });
  if (startDate && endDate && startDate > endDate) throw new Error("holding start_date 必须 <= end_date");
  if (symbol) params.set("symbol", symbol);
  if (startDate) params.set("start_date", startDate);
  if (endDate) params.set("end_date", endDate);
  params.set("limit", String(limit));
  return params;
}

function buildHoldingAccuracyQuery() {
  const params = new URLSearchParams();
  const lookbackDays = toNumber(el("holdingAccLookbackInput")?.value, "accuracy lookback_days", {
    integer: true,
    min: 1,
    max: 3650,
  });
  const endDate = String(el("holdingAccEndDateInput")?.value || "").trim();
  const strategyName = String(el("holdingAccStrategyInput")?.value || "").trim();
  const symbol = String(el("holdingAccSymbolInput")?.value || "").trim().toUpperCase();
  const minConfidence = toNumber(el("holdingAccMinConfidenceInput")?.value, "accuracy min_confidence", {
    min: 0,
    max: 1,
  });
  const limit = toNumber(el("holdingAccLimitInput")?.value, "accuracy limit", {
    integer: true,
    min: 1,
    max: 20000,
  });
  params.set("lookback_days", String(lookbackDays));
  if (endDate) params.set("end_date", endDate);
  if (strategyName) params.set("strategy_name", strategyName);
  if (symbol) params.set("symbol", symbol);
  params.set("min_confidence", String(minConfidence));
  params.set("limit", String(limit));
  return params;
}

function buildGoLiveReadinessQuery() {
  const params = new URLSearchParams();
  const lookbackDays = toNumber(el("goLiveLookbackInput")?.value, "go-live lookback_days", {
    integer: true,
    min: 1,
    max: 3650,
  });
  const endDate = String(el("goLiveEndDateInput")?.value || "").trim();
  const strategyName = String(el("goLiveStrategyInput")?.value || "").trim();
  const symbol = String(el("goLiveSymbolInput")?.value || "").trim().toUpperCase();
  const minConfidence = toNumber(el("goLiveMinConfidenceInput")?.value, "go-live min_confidence", {
    min: 0,
    max: 1,
  });
  const limit = toNumber(el("goLiveLimitInput")?.value, "go-live limit", {
    integer: true,
    min: 1,
    max: 20000,
  });
  params.set("lookback_days", String(lookbackDays));
  if (endDate) params.set("end_date", endDate);
  if (strategyName) params.set("strategy_name", strategyName);
  if (symbol) params.set("symbol", symbol);
  params.set("min_confidence", String(minConfidence));
  params.set("limit", String(limit));
  return params;
}

function buildHoldingAnalysisRequest() {
  const asOfDate = String(el("holdingAsOfDateInput")?.value || "").trim() || todayISO();
  const strategyName = getHoldingStrategyName();
  if (!strategyName) throw new Error("holding strategy_name 不能为空");
  return {
    as_of_date: asOfDate,
    strategy_name: strategyName,
    strategy_params: collectStrategyParams(false),
    use_autotune_profile: Boolean(el("useAutotuneProfileInput")?.checked),
    available_cash: toNumber(el("holdingAvailableCashInput")?.value, "holding available_cash", { min: 0 }),
    candidate_symbols: parseSymbols(el("holdingCandidateSymbolsInput")?.value),
    max_new_buys: toNumber(el("holdingMaxNewBuysInput")?.value, "holding max_new_buys", {
      integer: true,
      min: 0,
      max: 50,
    }),
    max_single_position_ratio: toNumber(
      el("holdingMaxSingleRatioInput")?.value,
      "holding max_single_position_ratio",
      { min: 0.01, max: 1 }
    ),
    lot_size: toNumber(el("holdingAnalyzeLotSizeInput")?.value, "holding lot_size", { integer: true, min: 1 }),
    intraday_interval: String(el("holdingIntradayIntervalInput")?.value || "15m").trim() || "15m",
    intraday_lookback_days: toNumber(
      el("holdingIntradayLookbackInput")?.value,
      "holding intraday_lookback_days",
      { integer: true, min: 1, max: 15 }
    ),
  };
}

async function submitHoldingTrade() {
  showGlobalError("");
  const req = buildHoldingTradeRequest();
  const row = await postJSON("/holdings/trades", req);
  await loadHoldingTrades({ silent: true });
  await loadHoldingPositions({ silent: true }).catch(() => {});
  setHoldingMessage(
    `成交已录入：id=${row.id} ${row.symbol} ${row.side} ${fmtNum(row.quantity, 0)} 股 @ ${fmtNum(row.price, 4)}`
  );
}

async function loadHoldingTrades({ silent = false } = {}) {
  const params = buildHoldingTradeQuery();
  const rows = await fetchJSON(`/holdings/trades?${params.toString()}`);
  state.latestHoldingTrades = Array.isArray(rows) ? rows : [];
  renderHoldingTrades();
  if (!silent) setHoldingMessage(`成交台账已刷新：${fmtNum(state.latestHoldingTrades.length, 0)} 条。`);
}

async function deleteHoldingTrade(tradeId) {
  const id = toNumber(tradeId, "holding trade_id", { integer: true, min: 1, max: 9_999_999 });
  await deleteJSON(`/holdings/trades/${id}`);
  await loadHoldingTrades({ silent: true });
  await loadHoldingPositions({ silent: true }).catch(() => {});
  setHoldingMessage(`成交已删除：trade_id=${id}`);
}

async function loadHoldingPositions({ silent = false } = {}) {
  const asOfDate = String(el("holdingAsOfDateInput")?.value || "").trim() || todayISO();
  if (el("holdingAsOfDateInput") && !el("holdingAsOfDateInput").value) el("holdingAsOfDateInput").value = asOfDate;
  const result = await fetchJSON(`/holdings/positions?${new URLSearchParams({ as_of_date: asOfDate }).toString()}`);
  state.latestHoldingPositions = result;
  renderHoldingPositionSummary();
  renderHoldingPositionRows();
  if (!silent) {
    const count = result && result.summary ? Number(result.summary.position_count || 0) : 0;
    setHoldingMessage(`持仓快照已刷新：${fmtNum(count, 0)} 个标的（as_of=${asOfDate}）。`);
  }
}

async function runHoldingAnalyze() {
  showGlobalError("");
  const req = buildHoldingAnalysisRequest();
  const result = await postJSON("/holdings/analyze", req);
  state.latestHoldingAnalysis = result;
  renderHoldingAnalysis();
  await loadHoldingAccuracyReport({ silent: true }).catch(() => {});
  await loadGoLiveReadinessReport({ silent: true }).catch(() => {});
  const recCount = Array.isArray(result.recommendations) ? result.recommendations.length : 0;
  setHoldingMessage(
    `持仓分析完成：run_id=${result.analysis_run_id || "-"}，建议 ${fmtNum(recCount, 0)} 条，下一交易日=${result.next_trade_date || "-"}`
  );
}

async function loadHoldingAccuracyReport({ silent = false } = {}) {
  const params = buildHoldingAccuracyQuery();
  const report = await fetchJSON(`/reports/strategy-accuracy?${params.toString()}`);
  state.latestHoldingAccuracyReport = report;
  renderHoldingAccuracy();
  if (!silent) {
    setHoldingMessage(
      `准确性看板已刷新：sample=${fmtNum(report.sample_size || 0, 0)} hit=${fmtPct(report.hit_rate || 0, 2)}`
    );
  }
}

async function loadGoLiveReadinessReport({ silent = false } = {}) {
  const params = buildGoLiveReadinessQuery();
  const report = await fetchJSON(`/reports/go-live-readiness?${params.toString()}`);
  state.latestGoLiveReadiness = report;
  renderGoLiveReadiness();
  if (!silent) {
    setHoldingMessage(
      `上线准入已刷新：${report.readiness_level || "-"}（硬失败=${fmtNum(report.failed_gate_count || 0, 0)}）`
    );
  }
}

function renderHoldingTrades() {
  const host = el("holdingTradeRows");
  if (!host) return;
  const rows = Array.isArray(state.latestHoldingTrades) ? state.latestHoldingTrades : [];
  if (!rows.length) {
    host.innerHTML = '<tr><td colspan="15" class="muted">暂无手工成交记录，请先录入成交。</td></tr>';
    return;
  }
  host.innerHTML = rows
    .map(
      (row) => `<tr>
      <td>${fmtNum(row.id, 0)}</td>
      <td>${esc(row.trade_date || "-")}</td>
      <td>${esc(row.symbol || "-")}</td>
      <td>${esc(row.symbol_name || "-")}</td>
      <td>${statusChip(row.side || "-")}</td>
      <td>${fmtNum(row.price, 4)}</td>
      <td>${row.reference_price === null || row.reference_price === undefined ? "-" : fmtNum(row.reference_price, 4)}</td>
      <td>${fmtTs(row.executed_at)}</td>
      <td>${row.is_partial_fill ? statusChip("YES", "warn") : statusChip("NO")}</td>
      <td>${esc(row.unfilled_reason || "-")}</td>
      <td>${fmtNum(row.lots, 0)}</td>
      <td>${fmtNum(row.quantity, 0)}</td>
      <td>${fmtNum(row.fee, 2)}</td>
      <td>${esc(row.note || "-")}</td>
      <td><button type="button" class="badge-btn secondary" data-holding-trade-delete-id="${esc(row.id)}">删除</button></td>
    </tr>`
    )
    .join("");
}

function renderHoldingPositionSummary() {
  const result = state.latestHoldingPositions;
  const summary = result && result.summary ? result.summary : null;
  const positionMeta = el("holdingPositionMeta");
  const positionCountEl = el("holdingPositionCountKpi");
  const quantityEl = el("holdingQuantityKpi");
  const costEl = el("holdingCostKpi");
  const marketEl = el("holdingMarketValueKpi");
  const pnlEl = el("holdingPnlKpi");
  const pnlPctEl = el("holdingPnlPctKpi");
  if (!positionMeta || !positionCountEl || !quantityEl || !costEl || !marketEl || !pnlEl || !pnlPctEl) return;

  if (!summary) {
    positionMeta.textContent = "暂无持仓快照，请点击“刷新持仓快照”。";
    positionCountEl.textContent = "-";
    quantityEl.textContent = "-";
    costEl.textContent = "-";
    marketEl.textContent = "-";
    pnlEl.textContent = "-";
    pnlPctEl.textContent = "-";
    return;
  }

  const asOf = result.as_of_date || "-";
  const provider = result.provider || "-";
  positionMeta.textContent = `as_of_date=${asOf} | provider=${provider}`;
  positionCountEl.textContent = fmtNum(summary.position_count, 0);
  quantityEl.textContent = fmtNum(summary.total_quantity, 0);
  costEl.textContent = fmtNum(summary.total_cost_value, 2);
  marketEl.textContent = fmtNum(summary.total_market_value, 2);
  pnlEl.textContent = fmtNum(summary.total_unrealized_pnl, 2);
  pnlPctEl.textContent = fmtPct(summary.total_unrealized_pnl_pct, 2);
}

function renderHoldingPositionRows() {
  const host = el("holdingPositionRows");
  if (!host) return;
  const rows = state.latestHoldingPositions && Array.isArray(state.latestHoldingPositions.positions)
    ? state.latestHoldingPositions.positions
    : [];
  if (!rows.length) {
    host.innerHTML = '<tr><td colspan="15" class="muted">暂无持仓快照。</td></tr>';
    return;
  }
  host.innerHTML = rows
    .map(
      (row) => `<tr>
      <td>${esc(row.symbol || "-")}</td>
      <td>${esc(row.symbol_name || "-")}</td>
      <td>${fmtNum(row.quantity, 0)}</td>
      <td>${fmtNum(row.lots, 0)}</td>
      <td>${fmtNum(row.avg_cost, 4)}</td>
      <td>${fmtNum(row.latest_price, 4)}</td>
      <td>${row.day_change_pct === null || row.day_change_pct === undefined ? "-" : fmtPct(row.day_change_pct, 2)}</td>
      <td>${fmtNum(row.cost_value, 2)}</td>
      <td>${fmtNum(row.market_value, 2)}</td>
      <td>${fmtNum(row.unrealized_pnl, 2)} (${fmtPct(row.unrealized_pnl_pct, 2)})</td>
      <td>${fmtPct(row.weight, 2)}</td>
      <td>${row.momentum20 === null || row.momentum20 === undefined ? "-" : fmtPct(row.momentum20, 2)}</td>
      <td>${row.volatility20 === null || row.volatility20 === undefined ? "-" : fmtPct(row.volatility20, 2)}</td>
      <td>${row.fundamental_score === null || row.fundamental_score === undefined ? "-" : fmtNum(row.fundamental_score, 3)}</td>
      <td>${esc(row.market_comment || "-")}</td>
    </tr>`
    )
    .join("");
}

function renderHoldingAnalysis() {
  const meta = el("holdingOverviewMeta");
  const posHost = el("holdingAnalyzePositionRows");
  const recHost = el("holdingRecommendationRows");
  if (!meta || !posHost || !recHost) return;

  const result = state.latestHoldingAnalysis;
  if (!result) {
    meta.textContent = "尚未运行持仓分析。";
    posHost.innerHTML = '<tr><td colspan="13" class="muted">暂无持仓分析结果。</td></tr>';
    recHost.innerHTML = '<tr><td colspan="17" class="muted">暂无建议清单。</td></tr>';
    return;
  }

  const overview = String(result.market_overview || "").trim() || "暂无市场综述。";
  meta.textContent =
    `as_of=${result.as_of_date || "-"} | next_trade_date=${result.next_trade_date || "-"} | strategy=${result.strategy_name || "-"} | ${overview}`;

  const positions = Array.isArray(result.positions) ? result.positions : [];
  posHost.innerHTML = positions.length
    ? positions
        .map(
          (row) => `<tr>
      <td>${esc(row.symbol || "-")}</td>
      <td>${esc(row.symbol_name || "-")}</td>
      <td>${statusChip(row.strategy_signal || "WATCH")}</td>
      <td>${fmtPct(row.expected_next_day_return, 2)}</td>
      <td>${fmtPct(row.up_probability, 2)}</td>
      <td>${statusChip(row.suggested_action || "HOLD")}</td>
      <td>${fmtNum(row.suggested_delta_lots, 0)}</td>
      <td>${esc(row.recommended_execution_window || "-")}</td>
      <td>${esc(Array.isArray(row.avoid_execution_windows) && row.avoid_execution_windows.length ? row.avoid_execution_windows.join(",") : "-")}</td>
      <td>${esc(row.intraday_risk_level || "-")}</td>
      <td>${fmtPct((Number(row.event_score || 0) - Number(row.negative_event_score || 0)), 2)}</td>
      <td>${esc(row.style_regime || "-")}</td>
      <td>${esc(row.analysis_note || "-")}</td>
    </tr>`
        )
        .join("")
    : '<tr><td colspan="13" class="muted">暂无持仓分析结果。</td></tr>';

  const recommendations = Array.isArray(result.recommendations) ? result.recommendations : [];
  recHost.innerHTML = recommendations.length
    ? recommendations
        .map(
          (row) => `<tr>
      <td>${esc(row.symbol || "-")}</td>
      <td>${esc(row.symbol_name || "-")}</td>
      <td>${statusChip(row.action || "WATCH")}</td>
      <td>${fmtNum(row.target_lots, 0)}</td>
      <td>${fmtNum(row.delta_lots, 0)}</td>
      <td>${fmtPct(row.confidence, 2)}</td>
      <td>${fmtPct(row.expected_next_day_return, 2)}</td>
      <td>${fmtPct(row.up_probability, 2)}</td>
      <td>${esc(row.next_trade_date || "-")}</td>
      <td>${esc(row.style_regime || "-")}</td>
      <td>${esc(row.execution_window || "-")}</td>
      <td>${esc(Array.isArray(row.avoid_execution_windows) && row.avoid_execution_windows.length ? row.avoid_execution_windows.join(",") : "-")}</td>
      <td>${esc(row.intraday_risk_level || "-")}</td>
      <td>${row.stop_loss_hint_pct === null || row.stop_loss_hint_pct === undefined ? "-" : fmtPct(row.stop_loss_hint_pct, 2)}</td>
      <td>${row.take_profit_hint_pct === null || row.take_profit_hint_pct === undefined ? "-" : fmtPct(row.take_profit_hint_pct, 2)}</td>
      <td>${esc(Array.isArray(row.risk_flags) && row.risk_flags.length ? row.risk_flags.join(",") : "-")}</td>
      <td>${esc(row.rationale || "-")}</td>
    </tr>`
        )
        .join("")
    : '<tr><td colspan="17" class="muted">暂无建议清单。</td></tr>';
}

function renderHoldingAccuracy() {
  const report = state.latestHoldingAccuracyReport;
  const meta = el("holdingAccuracyMeta");
  const sampleKpi = el("holdingAccSampleKpi");
  const hitKpi = el("holdingAccHitRateKpi");
  const brierKpi = el("holdingAccBrierKpi");
  const costAdjKpi = el("holdingAccCostAdjKpi");
  const coverageKpi = el("holdingAccCoverageKpi");
  const strategyHost = el("holdingAccByStrategyRows");
  const symbolHost = el("holdingAccBySymbolRows");
  const detailHost = el("holdingAccDetailRows");
  const noteHost = el("holdingAccuracyNoteList");
  if (
    !meta ||
    !sampleKpi ||
    !hitKpi ||
    !brierKpi ||
    !costAdjKpi ||
    !coverageKpi ||
    !strategyHost ||
    !symbolHost ||
    !detailHost ||
    !noteHost
  ) {
    return;
  }

  if (!report) {
    meta.textContent = "尚未加载准确性报告。";
    sampleKpi.textContent = "-";
    hitKpi.textContent = "-";
    brierKpi.textContent = "-";
    costAdjKpi.textContent = "-";
    coverageKpi.textContent = "-";
    strategyHost.innerHTML = '<tr><td colspan="7" class="muted">暂无按策略统计。</td></tr>';
    symbolHost.innerHTML = '<tr><td colspan="7" class="muted">暂无按标的统计。</td></tr>';
    detailHost.innerHTML = '<tr><td colspan="13" class="muted">暂无样本明细。</td></tr>';
    noteHost.innerHTML = "<li>无</li>";
    return;
  }

  meta.textContent =
    `窗口=${report.start_date || "-"}~${report.end_date || "-"} | sample=${fmtNum(report.sample_size || 0, 0)} | ` +
    `strategy=${report.strategy_name || "ALL"} | symbol=${report.symbol || "ALL"} | min_confidence=${fmtPct(report.min_confidence || 0, 2)}`;
  sampleKpi.textContent = fmtNum(report.sample_size || 0, 0);
  hitKpi.textContent = fmtPct(report.hit_rate || 0, 2);
  brierKpi.textContent = report.brier_score === null || report.brier_score === undefined ? "-" : fmtNum(report.brier_score, 4);
  costAdjKpi.textContent = fmtPct(report.cost_adjusted_return_mean || 0, 2);
  coverageKpi.textContent = fmtPct(report.execution_coverage || 0, 2);

  const byStrategy = Array.isArray(report.by_strategy) ? report.by_strategy : [];
  strategyHost.innerHTML = byStrategy.length
    ? byStrategy
        .map(
          (row) => `<tr>
      <td>${esc(row.bucket_key || "-")}</td>
      <td>${fmtNum(row.sample_size, 0)}</td>
      <td>${fmtPct(row.hit_rate, 2)}</td>
      <td>${row.brier_score === null || row.brier_score === undefined ? "-" : fmtNum(row.brier_score, 4)}</td>
      <td>${fmtPct(row.return_bias, 2)}</td>
      <td>${fmtPct(row.cost_adjusted_return_mean, 2)}</td>
      <td>${fmtPct(row.execution_coverage, 2)}</td>
    </tr>`
        )
        .join("")
    : '<tr><td colspan="7" class="muted">暂无按策略统计。</td></tr>';

  const bySymbol = Array.isArray(report.by_symbol) ? report.by_symbol : [];
  symbolHost.innerHTML = bySymbol.length
    ? bySymbol
        .map(
          (row) => `<tr>
      <td>${esc(row.bucket_key || "-")}</td>
      <td>${fmtNum(row.sample_size, 0)}</td>
      <td>${fmtPct(row.hit_rate, 2)}</td>
      <td>${row.brier_score === null || row.brier_score === undefined ? "-" : fmtNum(row.brier_score, 4)}</td>
      <td>${fmtPct(row.return_bias, 2)}</td>
      <td>${fmtPct(row.cost_adjusted_return_mean, 2)}</td>
      <td>${fmtPct(row.execution_coverage, 2)}</td>
    </tr>`
        )
        .join("")
    : '<tr><td colspan="7" class="muted">暂无按标的统计。</td></tr>';

  const details = Array.isArray(report.details) ? report.details : [];
  detailHost.innerHTML = details.length
    ? details
        .slice(0, 300)
        .map(
          (row) => `<tr>
      <td>${esc(row.as_of_date || "-")}</td>
      <td>${esc(row.next_trade_date || "-")}</td>
      <td>${esc(row.strategy_name || "-")}</td>
      <td>${esc(row.symbol || "-")}</td>
      <td>${statusChip(row.action || "WATCH")}</td>
      <td>${fmtPct(row.confidence, 2)}</td>
      <td>${fmtPct(row.expected_next_day_return, 2)}</td>
      <td>${row.realized_next_day_return === null || row.realized_next_day_return === undefined ? "-" : fmtPct(row.realized_next_day_return, 2)}</td>
      <td>${row.direction_hit === null || row.direction_hit === undefined ? "-" : row.direction_hit ? statusChip("YES") : statusChip("NO", "warn")}</td>
      <td>${row.brier_score === null || row.brier_score === undefined ? "-" : fmtNum(row.brier_score, 4)}</td>
      <td>${row.executed ? statusChip("YES") : statusChip("NO", "warn")}</td>
      <td>${row.execution_cost_bps === null || row.execution_cost_bps === undefined ? "-" : fmtNum(row.execution_cost_bps, 2)}</td>
      <td>${row.cost_adjusted_action_return === null || row.cost_adjusted_action_return === undefined ? "-" : fmtPct(row.cost_adjusted_action_return, 2)}</td>
    </tr>`
        )
        .join("")
    : '<tr><td colspan="13" class="muted">暂无样本明细。</td></tr>';

  const notes = Array.isArray(report.notes) ? report.notes : [];
  noteHost.innerHTML = notes.length ? notes.map((x) => `<li>${esc(x)}</li>`).join("") : "<li>无</li>";
}

function renderGoLiveReadiness() {
  const report = state.latestGoLiveReadiness;
  const meta = el("goLiveMeta");
  const passKpi = el("goLivePassKpi");
  const levelKpi = el("goLiveLevelKpi");
  const failedKpi = el("goLiveFailedGateKpi");
  const warnKpi = el("goLiveWarnGateKpi");
  const gateHost = el("goLiveGateRows");
  const rollbackHost = el("goLiveRollbackRows");
  const checklistHost = el("goLiveChecklistRows");
  const noteHost = el("goLiveNoteList");
  if (
    !meta ||
    !passKpi ||
    !levelKpi ||
    !failedKpi ||
    !warnKpi ||
    !gateHost ||
    !rollbackHost ||
    !checklistHost ||
    !noteHost
  ) {
    return;
  }

  if (!report) {
    meta.textContent = "尚未加载上线准入报告。";
    passKpi.textContent = "-";
    levelKpi.textContent = "-";
    failedKpi.textContent = "-";
    warnKpi.textContent = "-";
    gateHost.innerHTML = '<tr><td colspan="7" class="muted">暂无门槛检查结果。</td></tr>';
    rollbackHost.innerHTML = '<tr><td colspan="4" class="muted">暂无回滚规则。</td></tr>';
    checklistHost.innerHTML = '<tr><td colspan="4" class="muted">暂无每日验收清单。</td></tr>';
    noteHost.innerHTML = "<li>无</li>";
    return;
  }

  meta.textContent =
    `窗口=${report.start_date || "-"}~${report.end_date || "-"} | strategy=${report.strategy_name || "ALL"} | symbol=${report.symbol || "ALL"} | lookback=${fmtNum(report.lookback_days, 0)}d`;
  passKpi.textContent = report.overall_passed ? "YES" : "NO";
  levelKpi.textContent = String(report.readiness_level || "-");
  failedKpi.textContent = fmtNum(report.failed_gate_count || 0, 0);
  warnKpi.textContent = fmtNum(report.warning_gate_count || 0, 0);

  const gates = Array.isArray(report.gate_checks) ? report.gate_checks : [];
  gateHost.innerHTML = gates.length
    ? gates
        .map(
          (row) => `<tr>
      <td>${esc(row.gate_name || row.gate_key || "-")}</td>
      <td>${row.passed ? statusChip("PASS") : statusChip("FAIL", "danger")}</td>
      <td>${esc(row.actual_value === null || row.actual_value === undefined ? "-" : String(row.actual_value))}</td>
      <td>${esc(row.threshold_value === null || row.threshold_value === undefined ? "-" : String(row.threshold_value))}</td>
      <td>${esc(`${row.comparator || ""}`)}</td>
      <td>${statusChip(row.severity || "WARNING")}</td>
      <td>${esc(row.detail || "-")}</td>
    </tr>`
        )
        .join("")
    : '<tr><td colspan="7" class="muted">暂无门槛检查结果。</td></tr>';

  const rollback = Array.isArray(report.rollback_rules) ? report.rollback_rules : [];
  rollbackHost.innerHTML = rollback.length
    ? rollback
        .map(
          (row) => `<tr>
      <td>${esc(row.trigger_name || row.trigger_key || "-")}</td>
      <td>${esc(row.condition || "-")}</td>
      <td>${esc(row.action || "-")}</td>
      <td>${esc(row.owner || "-")}</td>
    </tr>`
        )
        .join("")
    : '<tr><td colspan="4" class="muted">暂无回滚规则。</td></tr>';

  const checklist = Array.isArray(report.daily_checklist) ? report.daily_checklist : [];
  checklistHost.innerHTML = checklist.length
    ? checklist
        .map(
          (row) => `<tr>
      <td>${esc(row.item_name || row.item_key || "-")}</td>
      <td>${statusChip(row.status || "PENDING", row.status === "FAIL" ? "danger" : row.status === "WARN" ? "warn" : "info")}</td>
      <td>${esc(row.detail || "-")}</td>
      <td><code class="code-inline">${esc(row.evidence || "-")}</code></td>
    </tr>`
        )
        .join("")
    : '<tr><td colspan="4" class="muted">暂无每日验收清单。</td></tr>';

  const notes = Array.isArray(report.notes) ? report.notes : [];
  noteHost.innerHTML = notes.length ? notes.map((x) => `<li>${esc(x)}</li>`).join("") : "<li>无</li>";
}

function renderHoldings() {
  renderHoldingTrades();
  renderHoldingPositionSummary();
  renderHoldingPositionRows();
  renderHoldingAnalysis();
  renderHoldingAccuracy();
  renderGoLiveReadiness();
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
      portfolio_backtest_request: buildPortfolioBacktestRequest(),
      research_request: buildResearchRequest(),
      autotune_request: buildAutotuneRequest({ tolerantSearchSpace: true }),
      strategy_challenge_request: buildStrategyChallengeRequest({ tolerantSearchSpace: true }),
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
  if (["strategy", "autotune", "challenge", "results", "holdings", "execution"].includes(hashName)) switchTab(hashName);
}

function saveFormSnapshot() {
  const data = {};
  const selector =
    "#tab-strategy input[id], #tab-strategy textarea[id], #tab-strategy select[id], " +
    "#tab-autotune input[id], #tab-autotune textarea[id], #tab-autotune select[id], " +
    "#tab-challenge input[id], #tab-challenge textarea[id], #tab-challenge select[id], " +
    "#tab-holdings input[id], #tab-holdings textarea[id], #tab-holdings select[id]";
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

  const selector =
    "#tab-strategy input[id], #tab-strategy textarea[id], #tab-strategy select[id], " +
    "#tab-autotune input[id], #tab-autotune textarea[id], #tab-autotune select[id], " +
    "#tab-challenge input[id], #tab-challenge textarea[id], #tab-challenge select[id], " +
    "#tab-holdings input[id], #tab-holdings textarea[id], #tab-holdings select[id]";
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

async function runPortfolioBacktest({ silent = false } = {}) {
  showGlobalError("");
  if (!silent) setActionMessage("正在运行组合净值回测...");
  const req = buildPortfolioBacktestRequest();
  const result = await postJSON("/backtest/portfolio-run", req);
  state.latestPortfolioBacktestRequest = req;
  state.latestPortfolioBacktestResult = result;
  renderResults();
  if (!silent) {
    setActionMessage("组合净值回测完成，已更新组合净值曲线与组合成交。");
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

async function runAutotune({ silent = false } = {}) {
  showGlobalError("");
  if (!silent) setAutotuneMessage("正在运行自动调参...");
  const req = buildAutotuneRequest();
  const result = await postJSON("/autotune/run", req);
  state.latestAutotuneRequest = req;
  state.latestAutotuneResult = result;
  await loadAutotuneProfiles({ silent: true }).catch(() => {});
  await loadAutotuneActiveProfile({ silent: true }).catch(() => {});
  renderAutotuneWorkbench();
  renderResults();
  if (!silent) {
    const bestObj = result && result.best ? fmtNum(result.best.objective_score, 6) : "-";
    setAutotuneMessage(
      `自动调参完成：候选=${fmtNum(result.evaluated_count || 0, 0)}，best objective=${bestObj}，决策=${result.apply_decision || "-"}.`
    );
    switchTab("autotune");
  }
}

async function runStrategyChallenge({ silent = false } = {}) {
  showGlobalError("");
  if (!silent) setChallengeMessage("正在运行跨策略挑战赛...");
  const req = buildStrategyChallengeRequest();
  const result = await postJSON("/challenge/run", req);
  state.latestChallengeRequest = req;
  state.latestChallengeResult = result;
  renderChallengeWorkbench();
  if (!silent) {
    setChallengeMessage(
      `挑战赛完成：参赛=${fmtNum((result.strategy_names || []).length, 0)}，入围=${fmtNum(result.qualified_count || 0, 0)}，冠军=${result.champion_strategy || "-"}。`
    );
    switchTab("challenge");
  }
}

async function loadAutotuneProfiles({ silent = false } = {}) {
  const strategyName = String(el("strategySelect")?.value || "").trim();
  if (!strategyName) throw new Error("strategy_name 不能为空");
  const symbol = String(el("symbolInput")?.value || "").trim();
  const qs = new URLSearchParams({ strategy_name: strategyName, limit: "200" });
  if (symbol) qs.set("symbol", symbol);
  const rows = await fetchJSON(`/autotune/profiles?${qs.toString()}`);
  state.latestAutotuneProfiles = Array.isArray(rows) ? rows : [];
  renderAutotuneWorkbench();
  if (!silent) setAutotuneMessage(`已加载参数画像 ${fmtNum(state.latestAutotuneProfiles.length, 0)} 条。`);
}

async function loadAutotuneActiveProfile({ silent = false } = {}) {
  const strategyName = String(el("strategySelect")?.value || "").trim();
  if (!strategyName) throw new Error("strategy_name 不能为空");
  const symbol = String(el("symbolInput")?.value || "").trim();
  const qs = new URLSearchParams({ strategy_name: strategyName });
  if (symbol) qs.set("symbol", symbol);
  const row = await fetchJSON(`/autotune/profiles/active?${qs.toString()}`);
  state.latestAutotuneActiveProfile = row && typeof row === "object" ? row : null;
  renderAutotuneWorkbench();
  if (!silent) {
    if (state.latestAutotuneActiveProfile) {
      setAutotuneMessage(
        `当前生效画像：id=${state.latestAutotuneActiveProfile.id} scope=${state.latestAutotuneActiveProfile.scope} symbol=${state.latestAutotuneActiveProfile.symbol || "-"}`
      );
    } else {
      setAutotuneMessage("当前策略暂无生效画像。");
    }
  }
}

async function activateAutotuneProfile(profileId) {
  const id = toNumber(profileId, "profile_id", { integer: true, min: 1, max: 9_999_999 });
  const row = await postJSON(`/autotune/profiles/${id}/activate`, {});
  state.latestAutotuneActiveProfile = row && typeof row === "object" ? row : null;
  await loadAutotuneProfiles({ silent: true }).catch(() => {});
  renderAutotuneWorkbench();
  setAutotuneMessage(`画像已切换：id=${id}（可用于版本回退）。`);
}

async function rollbackAutotuneProfile() {
  const strategyName = String(el("strategySelect")?.value || "").trim();
  if (!strategyName) throw new Error("strategy_name 不能为空");
  const symbol = String(el("symbolInput")?.value || "").trim() || null;
  const scopeRaw = String(el("autotuneRollbackScopeInput")?.value || "AUTO").trim().toUpperCase();
  const body = {
    strategy_name: strategyName,
    symbol,
  };
  if (scopeRaw === "GLOBAL" || scopeRaw === "SYMBOL") body.scope = scopeRaw;
  const row = await postJSON("/autotune/profiles/rollback", body);
  state.latestAutotuneActiveProfile = row && typeof row === "object" ? row : null;
  await loadAutotuneProfiles({ silent: true }).catch(() => {});
  renderAutotuneWorkbench();
  setAutotuneMessage(`回滚完成：已切换到画像 id=${row.id}。`);
}

function getRolloutStrategyName() {
  const explicit = String(el("rolloutRuleStrategyInput")?.value || "").trim();
  if (explicit) return explicit;
  return String(el("strategySelect")?.value || "").trim();
}

async function loadRolloutRules({ silent = false } = {}) {
  const strategyName = getRolloutStrategyName();
  if (!strategyName) throw new Error("strategy_name 不能为空");
  const symbol = String(el("rolloutRuleSymbolInput")?.value || "").trim();
  const qs = new URLSearchParams({ strategy_name: strategyName, limit: "500" });
  if (symbol) qs.set("symbol", symbol);
  const rows = await fetchJSON(`/autotune/rollout/rules?${qs.toString()}`);
  state.latestRolloutRules = Array.isArray(rows) ? rows : [];
  renderRolloutRuleRows();
  if (!silent) setRolloutRuleMessage(`已加载灰度规则 ${fmtNum(state.latestRolloutRules.length, 0)} 条。`);
}

async function upsertRolloutRule() {
  const strategyName = getRolloutStrategyName();
  if (!strategyName) throw new Error("strategy_name 不能为空");
  const symbol = String(el("rolloutRuleSymbolInput")?.value || "").trim() || null;
  const body = {
    strategy_name: strategyName,
    symbol,
    enabled: Boolean(el("rolloutRuleEnabledInput")?.checked),
    note: String(el("rolloutRuleNoteInput")?.value || "").trim(),
  };
  const row = await postJSON("/autotune/rollout/rules/upsert", body);
  await loadRolloutRules({ silent: true }).catch(() => {});
  setRolloutRuleMessage(`灰度规则已保存：id=${row.id} strategy=${row.strategy_name} symbol=${row.symbol || "-"}`);
}

async function deleteRolloutRule(ruleId) {
  const id = toNumber(ruleId, "rule_id", { integer: true, min: 1, max: 9_999_999 });
  await deleteJSON(`/autotune/rollout/rules/${id}`);
  await loadRolloutRules({ silent: true }).catch(() => {});
  setRolloutRuleMessage(`灰度规则已删除：id=${id}`);
}

function applyStrategyParamsToForm(strategyName, params, sourceLabel = "参数来源") {
  const select = el("strategySelect");
  if (select instanceof HTMLSelectElement && strategyName) {
    const hasTarget = Array.from(select.options).some((opt) => opt.value === strategyName);
    if (hasTarget) select.value = strategyName;
  }
  const finalStrategy = String(el("strategySelect")?.value || strategyName || "").trim();
  renderStrategyParams(finalStrategy, params || {});
  updateSmallCapitalHint();
  updateRequestPreview();
  saveFormSnapshot();
  setAutotuneMessage(`${sourceLabel} 已回填到策略参数区，可直接运行信号/回测/研究。`);
}

function applyChallengeChampionToStrategyForm() {
  const result = state.latestChallengeResult;
  if (!result || !result.champion_strategy) {
    throw new Error("暂无挑战赛冠军参数，请先运行挑战赛。");
  }
  const rows = Array.isArray(result.results) ? result.results : [];
  const winner = rows.find((x) => String(x.strategy_name || "") === String(result.champion_strategy || ""));
  if (!winner || !winner.best_params || !Object.keys(winner.best_params).length) {
    throw new Error("冠军策略缺少可回填参数。");
  }
  applyStrategyParamsToForm(String(winner.strategy_name || ""), winner.best_params, "挑战赛冠军参数");
  setChallengeMessage(`冠军参数已回填：strategy=${winner.strategy_name}`);
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

function renderAutotuneSummaryKpis() {
  const result = state.latestAutotuneResult;
  const evalEl = el("autotuneEvalCount");
  const bestEl = el("autotuneBestObjective");
  const improveEl = el("autotuneImprovement");
  const decisionEl = el("autotuneApplyDecision");
  if (evalEl) evalEl.textContent = result ? fmtNum(result.evaluated_count || 0, 0) : "-";
  if (bestEl) bestEl.textContent = result && result.best ? fmtNum(result.best.objective_score, 6) : "-";
  if (improveEl) improveEl.textContent = result ? fmtNum(result.improvement_vs_baseline || 0, 6) : "-";
  if (decisionEl) decisionEl.textContent = result ? String(result.apply_decision || "-") : "-";
}

function renderAutotuneCandidateRows() {
  const host = el("autotuneCandidateRows");
  if (!host) return;
  const rows = state.latestAutotuneResult && Array.isArray(state.latestAutotuneResult.candidates)
    ? state.latestAutotuneResult.candidates
    : [];
  if (!rows.length) {
    host.innerHTML = '<tr><td colspan="12" class="muted">暂无自动调参结果。请先运行自动调参。</td></tr>';
    return;
  }

  host.innerHTML = rows
    .slice(0, 80)
    .map((item, idx) => {
      const guard = item.apply_eligible ? "eligible" : String(item.apply_guard_reason || "-");
      return `<tr>
        <td>${fmtNum(item.rank, 0)}</td>
        <td>${fmtNum(item.objective_score, 6)}</td>
        <td>${fmtNum(item.train_score, 6)}</td>
        <td>${item.validation_score === null || item.validation_score === undefined ? "-" : fmtNum(item.validation_score, 6)}</td>
        <td>${fmtNum(item.overfit_penalty || 0, 6)}</td>
        <td>${fmtNum(item.stability_penalty || 0, 6)}</td>
        <td>${fmtNum(item.param_drift_penalty || 0, 6)}</td>
        <td>${fmtNum(item.return_variance_penalty || 0, 6)}</td>
        <td>${fmtNum(item.walk_forward_samples || 0, 0)}</td>
        <td>${esc(guard)}</td>
        <td><code class="code-inline">${esc(JSON.stringify(item.strategy_params || {}))}</code></td>
        <td><button type="button" class="badge-btn secondary" data-autotune-candidate-idx="${idx}">回填参数</button></td>
      </tr>`;
    })
    .join("");
}

function renderAutotuneProfileRows() {
  const host = el("autotuneProfileRows");
  if (!host) return;
  const rows = state.latestAutotuneProfiles || [];
  if (!rows.length) {
    host.innerHTML = '<tr><td colspan="9" class="muted">暂无画像记录。可先运行自动调参或点击“加载画像列表”。</td></tr>';
    return;
  }
  host.innerHTML = rows
    .map(
      (item) => `<tr class="${item.active ? "row-active" : ""}">
      <td>${fmtNum(item.id, 0)}</td>
      <td>${esc(item.strategy_name || "-")}</td>
      <td>${esc(item.scope || "-")}</td>
      <td>${esc(item.symbol || "-")}</td>
      <td>${fmtNum(item.objective_score, 6)}</td>
      <td>${item.active ? statusChip("ACTIVE") : statusChip("INACTIVE", "warn")}</td>
      <td>${fmtTs(item.updated_at)}</td>
      <td><code>${esc(item.source_run_id || "-")}</code></td>
      <td>${
        item.active
          ? '<span class="muted">生效中</span>'
          : `<button type="button" class="badge-btn secondary" data-autotune-activate-id="${esc(item.id)}">设为生效（回退）</button>`
      }</td>
    </tr>`
    )
    .join("");
}

function renderRolloutRuleRows() {
  const host = el("rolloutRuleRows");
  if (!host) return;
  const rows = Array.isArray(state.latestRolloutRules) ? state.latestRolloutRules : [];
  if (!rows.length) {
    host.innerHTML = '<tr><td colspan="7" class="muted">暂无灰度规则。</td></tr>';
    return;
  }
  host.innerHTML = rows
    .map(
      (item) => `<tr>
      <td>${fmtNum(item.id, 0)}</td>
      <td>${esc(item.strategy_name || "-")}</td>
      <td>${esc(item.symbol || "-")}</td>
      <td>${item.enabled ? statusChip("ENABLED") : statusChip("DISABLED", "warn")}</td>
      <td>${fmtTs(item.updated_at)}</td>
      <td>${esc(item.note || "-")}</td>
      <td><button type="button" class="badge-btn secondary" data-rollout-delete-id="${esc(item.id)}">删除</button></td>
    </tr>`
    )
    .join("");
}

function renderAutotuneComparison() {
  const meta = el("autotuneCompareMeta");
  const host = el("autotuneCompareRows");
  if (!meta || !host) return;
  const result = state.latestAutotuneResult;
  if (!result || !result.best || !result.baseline) {
    meta.textContent = "暂无自动调参结果。";
    host.innerHTML = '<tr><td colspan="3" class="muted">运行自动调参后显示基线与最优参数对比。</td></tr>';
    return;
  }

  const base = result.baseline;
  const best = result.best;
  meta.textContent = `策略=${result.strategy_name} | 决策=${result.apply_decision || "-"} | 提升=${fmtNum(result.improvement_vs_baseline || 0, 6)}`;

  const rows = [
    ["objective_score", fmtNum(base.objective_score, 6), fmtNum(best.objective_score, 6)],
    ["train_score", fmtNum(base.train_score, 6), fmtNum(best.train_score, 6)],
    [
      "validation_score",
      base.validation_score === null || base.validation_score === undefined ? "-" : fmtNum(base.validation_score, 6),
      best.validation_score === null || best.validation_score === undefined ? "-" : fmtNum(best.validation_score, 6),
    ],
    ["train_total_return", fmtPct(base.train_metrics.total_return, 2), fmtPct(best.train_metrics.total_return, 2)],
    [
      "validation_total_return",
      base.validation_metrics ? fmtPct(base.validation_metrics.total_return, 2) : "-",
      best.validation_metrics ? fmtPct(best.validation_metrics.total_return, 2) : "-",
    ],
    ["max_drawdown(train)", fmtPct(base.train_metrics.max_drawdown, 2), fmtPct(best.train_metrics.max_drawdown, 2)],
    ["sharpe(train)", fmtNum(base.train_metrics.sharpe, 4), fmtNum(best.train_metrics.sharpe, 4)],
    ["overfit_penalty", fmtNum(base.overfit_penalty || 0, 6), fmtNum(best.overfit_penalty || 0, 6)],
    ["stability_penalty", fmtNum(base.stability_penalty || 0, 6), fmtNum(best.stability_penalty || 0, 6)],
    ["param_drift_penalty", fmtNum(base.param_drift_penalty || 0, 6), fmtNum(best.param_drift_penalty || 0, 6)],
    ["return_variance_penalty", fmtNum(base.return_variance_penalty || 0, 6), fmtNum(best.return_variance_penalty || 0, 6)],
    ["walk_forward_return_std", fmtNum(base.walk_forward_return_std || 0, 6), fmtNum(best.walk_forward_return_std || 0, 6)],
    ["walk_forward_samples", fmtNum(base.walk_forward_samples || 0, 0), fmtNum(best.walk_forward_samples || 0, 0)],
  ];
  host.innerHTML = rows.map(([k, v1, v2]) => `<tr><td>${esc(k)}</td><td>${esc(v1)}</td><td>${esc(v2)}</td></tr>`).join("");
}

function renderAutotuneWorkbench() {
  renderAutotuneSummaryKpis();
  renderAutotuneCandidateRows();
  renderAutotuneProfileRows();
  renderRolloutRuleRows();
  renderAutotuneComparison();
  const result = state.latestAutotuneResult;
  const quickMeta = el("autotuneResultsMeta");
  if (quickMeta) {
    if (result && result.best) {
      quickMeta.textContent =
        `最近一次调参：strategy=${result.strategy_name} improvement=${fmtNum(result.improvement_vs_baseline || 0, 6)} decision=${result.apply_decision || "-"}`;
    } else {
      quickMeta.textContent = "自动调参详细候选榜、基线对比、画像回滚和灰度规则请在“自动调参页”查看。";
    }
  }
}

function renderChallengeSummaryKpis() {
  const result = state.latestChallengeResult;
  const evalEl = el("challengeEvaluatedCount");
  const qualEl = el("challengeQualifiedCount");
  const championEl = el("challengeChampion");
  const runnerUpEl = el("challengeRunnerUp");
  if (evalEl) evalEl.textContent = result ? fmtNum(result.evaluated_count || 0, 0) : "-";
  if (qualEl) qualEl.textContent = result ? fmtNum(result.qualified_count || 0, 0) : "-";
  if (championEl) championEl.textContent = result ? String(result.champion_strategy || "-") : "-";
  if (runnerUpEl) runnerUpEl.textContent = result ? String(result.runner_up_strategy || "-") : "-";
}

function renderChallengePlan() {
  const result = state.latestChallengeResult;
  const meta = el("challengeSummaryMeta");
  const planHost = el("challengePlanRows");
  if (!meta || !planHost) return;
  if (!result) {
    meta.textContent = "尚未运行挑战赛。";
    planHost.innerHTML = '<tr><td colspan="2" class="muted">暂无灰度计划。</td></tr>';
    return;
  }

  meta.textContent =
    `run_id=${result.run_id || "-"} | symbol=${result.symbol || "-"} | 窗口=${result.start_date || "-"}~${result.end_date || "-"} | ${result.market_fit_summary || ""}`;

  const plan = result.rollout_plan;
  if (!plan) {
    planHost.innerHTML = '<tr><td colspan="2" class="muted">未返回灰度计划。</td></tr>';
    return;
  }
  const rows = [
    ["enabled", String(Boolean(plan.enabled))],
    ["strategy_name", String(plan.strategy_name || "-")],
    ["symbol", String(plan.symbol || "-")],
    ["gray_days", fmtNum(plan.gray_days, 0)],
    ["activation_scope", String(plan.activation_scope || "-")],
    [
      "rollback_triggers",
      Array.isArray(plan.rollback_triggers) && plan.rollback_triggers.length
        ? plan.rollback_triggers.join("; ")
        : "-",
    ],
  ];
  planHost.innerHTML = rows
    .map(([k, v]) => `<tr><td>${esc(k)}</td><td>${esc(v)}</td></tr>`)
    .join("");
}

function renderChallengeReasonRows() {
  const host = el("challengeReasonRows");
  if (!host) return;
  const rows = state.latestChallengeResult && Array.isArray(state.latestChallengeResult.results)
    ? state.latestChallengeResult.results
    : [];
  if (!rows.length) {
    host.innerHTML = '<tr><td colspan="3" class="muted">暂无挑战赛结果。</td></tr>';
    return;
  }
  host.innerHTML = rows
    .map((item) => {
      const reasons = [];
      if (Array.isArray(item.qualification_reasons) && item.qualification_reasons.length) {
        reasons.push(...item.qualification_reasons);
      }
      if (item.error) reasons.push(`error=${item.error}`);
      const reasonText = reasons.length ? reasons.join("; ") : "qualified";
      return `<tr>
        <td>${esc(item.strategy_name || "-")}</td>
        <td>${item.qualified ? statusChip("QUALIFIED") : statusChip("REJECTED", "warn")}</td>
        <td>${esc(reasonText)}</td>
      </tr>`;
    })
    .join("");
}

function renderChallengeResultRows() {
  const host = el("challengeResultRows");
  if (!host) return;
  const rows = state.latestChallengeResult && Array.isArray(state.latestChallengeResult.results)
    ? state.latestChallengeResult.results
    : [];
  if (!rows.length) {
    host.innerHTML = '<tr><td colspan="12" class="muted">暂无挑战赛候选榜。</td></tr>';
    return;
  }

  host.innerHTML = rows
    .map((item) => {
      const vm = item.validation_metrics || null;
      return `<tr>
        <td>${esc(item.strategy_name || "-")}</td>
        <td>${item.qualified ? statusChip("QUALIFIED") : statusChip("REJECTED", "warn")}</td>
        <td>${item.ranking_score === null || item.ranking_score === undefined ? "-" : fmtNum(item.ranking_score, 6)}</td>
        <td>${vm ? fmtPct(vm.total_return, 2) : "-"}</td>
        <td>${vm ? fmtPct(vm.max_drawdown, 2) : "-"}</td>
        <td>${vm ? fmtNum(vm.sharpe, 4) : "-"}</td>
        <td>${fmtNum(item.walk_forward_samples || 0, 0)}</td>
        <td>${item.walk_forward_return_std === null || item.walk_forward_return_std === undefined ? "-" : fmtNum(item.walk_forward_return_std, 6)}</td>
        <td>${fmtNum(item.stability_penalty || 0, 6)}</td>
        <td>${fmtNum(item.param_drift_penalty || 0, 6)}</td>
        <td>${fmtNum(item.return_variance_penalty || 0, 6)}</td>
        <td><code class="code-inline">${esc(JSON.stringify(item.best_params || {}))}</code></td>
      </tr>`;
    })
    .join("");
}

function renderChallengeWorkbench() {
  renderChallengeSummaryKpis();
  renderChallengePlan();
  renderChallengeReasonRows();
  renderChallengeResultRows();
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

function renderPortfolioEquityChart(points) {
  const host = el("portfolioEquityChart");
  if (!host) return;
  if (!Array.isArray(points) || !points.length) {
    host.innerHTML = '<p class="chart-note">暂无组合净值曲线。</p>';
    return;
  }

  const rows = points
    .map((x) => ({
      date: normalizeDateString(x.date),
      equity: Number(x.equity),
    }))
    .filter((x) => x.date && Number.isFinite(x.equity));
  if (!rows.length) {
    host.innerHTML = '<p class="chart-note">组合净值数据格式异常。</p>';
    return;
  }

  const width = 980;
  const height = 280;
  const padLeft = 48;
  const padRight = 14;
  const padTop = 16;
  const padBottom = 28;
  const usableW = width - padLeft - padRight;
  const usableH = height - padTop - padBottom;

  const minV = Math.min(...rows.map((x) => x.equity));
  const maxV = Math.max(...rows.map((x) => x.equity));
  const span = Math.max(maxV - minV, 1e-9);
  const xAt = (idx) => padLeft + (idx / Math.max(rows.length - 1, 1)) * usableW;
  const yAt = (val) => padTop + ((maxV - val) / span) * usableH;

  const path = rows
    .map((row, idx) => `${idx === 0 ? "M" : "L"} ${xAt(idx).toFixed(2)} ${yAt(row.equity).toFixed(2)}`)
    .join(" ");
  const first = rows[0];
  const last = rows[rows.length - 1];

  host.innerHTML = `
    <svg viewBox="0 0 ${width} ${height}" role="img" aria-label="组合净值曲线">
      <line x1="${padLeft}" y1="${height - padBottom}" x2="${width - padRight}" y2="${height - padBottom}" stroke="#c3d4bc" stroke-width="1" />
      <path d="${path}" fill="none" stroke="#0f7f4f" stroke-width="2.3" />
      <text x="${padLeft}" y="${height - 8}" font-size="11" fill="#5f7262">${esc(first.date)}</text>
      <text x="${width - padRight - 92}" y="${height - 8}" font-size="11" fill="#5f7262">${esc(last.date)}</text>
      <text x="${padLeft + 2}" y="${padTop + 10}" font-size="11" fill="#2e5b44">${esc(`min=${fmtNum(minV, 2)} max=${fmtNum(maxV, 2)}`)}</text>
    </svg>
  `;
}

function renderPortfolioBacktest() {
  const result = state.latestPortfolioBacktestResult;
  const meta = el("portfolioBacktestMeta");
  const returnKpi = el("portfolioReturnKpi");
  const mddKpi = el("portfolioMddKpi");
  const sharpeKpi = el("portfolioSharpeKpi");
  const utilKpi = el("portfolioUtilKpi");
  const tradeCountKpi = el("portfolioTradeCountKpi");
  const weightHost = el("portfolioWeightRows");
  const tradeHost = el("portfolioTradeRows");

  if (!meta || !returnKpi || !mddKpi || !sharpeKpi || !utilKpi || !tradeCountKpi || !weightHost || !tradeHost) return;

  if (!result || !result.metrics) {
    meta.textContent = "暂无组合级回测结果。可回到“策略与参数页”点击“运行组合净值回测”。";
    returnKpi.textContent = "-";
    mddKpi.textContent = "-";
    sharpeKpi.textContent = "-";
    utilKpi.textContent = "-";
    tradeCountKpi.textContent = "-";
    weightHost.innerHTML = '<tr><td colspan="4" class="muted">暂无最终权重。</td></tr>';
    tradeHost.innerHTML = '<tr><td colspan="7" class="muted">暂无组合成交记录。</td></tr>';
    renderPortfolioEquityChart([]);
    return;
  }

  const m = result.metrics;
  returnKpi.textContent = fmtPct(m.total_return, 2);
  mddKpi.textContent = fmtPct(m.max_drawdown, 2);
  sharpeKpi.textContent = fmtNum(m.sharpe, 4);
  utilKpi.textContent = fmtPct(m.avg_utilization, 2);
  tradeCountKpi.textContent = fmtNum(m.trade_count, 0);
  meta.textContent =
    `标的数=${fmtNum((result.symbols || []).length, 0)} | 行业超限次数=${fmtNum(m.industry_breach_count || 0, 0)} | 主题超限次数=${fmtNum(m.theme_breach_count || 0, 0)} | 风控阻断日=${fmtNum(m.risk_blocked_days || 0, 0)} | 风控预警日=${fmtNum(m.risk_warning_days || 0, 0)}`;

  const req = state.latestPortfolioBacktestRequest || {};
  const industryMap = req.industry_map && typeof req.industry_map === "object" ? req.industry_map : {};
  const themeMap = req.theme_map && typeof req.theme_map === "object" ? req.theme_map : {};
  const finalWeights = Object.entries(result.final_weights || {})
    .map(([symbol, weight]) => ({ symbol, weight: Number(weight || 0) }))
    .filter((x) => x.symbol)
    .sort((a, b) => b.weight - a.weight);

  weightHost.innerHTML = finalWeights.length
    ? finalWeights
        .map(
          (row) => `<tr>
      <td>${esc(row.symbol)}</td>
      <td>${fmtPct(row.weight, 2)}</td>
      <td>${esc(industryMap[row.symbol] || "-")}</td>
      <td>${esc(themeMap[row.symbol] || "-")}</td>
    </tr>`
        )
        .join("")
    : '<tr><td colspan="4" class="muted">暂无最终权重。</td></tr>';

  const trades = Array.isArray(result.trades) ? result.trades : [];
  tradeHost.innerHTML = trades.length
    ? trades
        .slice(0, 300)
        .map(
          (t) => `<tr>
      <td>${esc(t.date)}</td>
      <td>${esc(t.symbol)}</td>
      <td>${statusChip(t.action)}</td>
      <td>${fmtNum(t.price, 4)}</td>
      <td>${fmtNum(t.quantity, 0)}</td>
      <td>${fmtNum(t.fee, 2)}</td>
      <td>${fmtPct(t.fill_ratio, 2)}</td>
    </tr>`
        )
        .join("")
    : '<tr><td colspan="7" class="muted">暂无组合成交记录。</td></tr>';

  renderPortfolioEquityChart(Array.isArray(result.equity_curve) ? result.equity_curve : []);
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
  renderAutotuneWorkbench();
  renderSignalRows();
  renderRiskDetail();
  renderResearchRows();
  renderOptimizeRows();
  renderWeightVisuals();
  renderRebalanceRows();
  renderMarketBarRows();
  renderMarketKlineChart();
  renderPortfolioBacktest();
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
  if (el("execReferencePrice")) {
    const ref = signalLike.reference_price ?? signalLike.latest_close ?? signalLike.close ?? "";
    el("execReferencePrice").value = ref === null || ref === undefined || ref === "" ? "" : String(ref);
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
  const referencePriceRaw = String(el("execReferencePrice")?.value || "").trim();

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
    reference_price: referencePriceRaw ? toNumber(referencePriceRaw, "reference_price", { min: 0.0001 }) : null,
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
  const strategyName = String(el("reportStrategyName")?.value || "").trim();
  const startDate = String(el("reportStartDate")?.value || "").trim();
  const endDate = String(el("reportEndDate")?.value || "").trim();
  const limit = toNumber(el("reportLimit")?.value, "replay report limit", { integer: true, min: 1, max: 2000 });

  if (symbol) params.set("symbol", symbol);
  if (strategyName) params.set("strategy_name", strategyName);
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

function buildAttributionFilters() {
  const params = new URLSearchParams();
  const symbol = String(el("attrSymbolInput")?.value || "").trim();
  const strategyName = String(el("attrStrategyInput")?.value || "").trim();
  const startDate = String(el("attrStartDateInput")?.value || "").trim();
  const endDate = String(el("attrEndDateInput")?.value || "").trim();
  const limit = toNumber(el("attrLimitInput")?.value, "attribution limit", { integer: true, min: 1, max: 2000 });
  if (symbol) params.set("symbol", symbol);
  if (strategyName) params.set("strategy_name", strategyName);
  if (startDate) params.set("start_date", startDate);
  if (endDate) params.set("end_date", endDate);
  params.set("limit", String(limit));
  return { params, symbol, strategyName, startDate, endDate, limit };
}

async function loadReplayAttribution() {
  const { params } = buildAttributionFilters();
  const report = await fetchJSON(`/replay/attribution?${params.toString()}`);
  state.replayAttribution = report;
  renderReplayAttribution();
}

function renderReplayAttribution() {
  const report = state.replayAttribution;
  const sampleEl = el("attrSample");
  const followEl = el("attrFollowRate");
  const delayEl = el("attrAvgDelay");
  const slipEl = el("attrAvgSlip");
  const reasonHost = el("attributionReasonRows");
  const itemHost = el("attributionItemRows");
  const suggestionHost = el("attributionSuggestionList");
  if (!sampleEl || !followEl || !delayEl || !slipEl || !reasonHost || !itemHost || !suggestionHost) return;

  if (!report) {
    sampleEl.textContent = "-";
    followEl.textContent = "-";
    delayEl.textContent = "-";
    slipEl.textContent = "-";
    reasonHost.innerHTML = '<tr><td colspan="4" class="muted">暂无归因统计。</td></tr>';
    itemHost.innerHTML = '<tr><td colspan="8" class="muted">暂无归因明细。</td></tr>';
    suggestionHost.innerHTML = "<li>无</li>";
    return;
  }

  sampleEl.textContent = fmtNum(report.sample_size || 0, 0);
  followEl.textContent = fmtPct(report.follow_rate || 0, 2);
  delayEl.textContent = fmtNum(report.avg_delay_days || 0, 2);
  slipEl.textContent = fmtNum(report.avg_slippage_bps || 0, 2);

  const reasons = Object.entries(report.reason_counts || {}).sort((a, b) => Number(b[1]) - Number(a[1]));
  reasonHost.innerHTML = reasons.length
    ? reasons
        .map(([reason, count]) => {
          const ratio = Number((report.reason_rates || {})[reason] || 0);
          const drag = Number((report.reason_cost_bps || {})[reason] || 0);
          return `<tr>
      <td>${esc(reason)}</td>
      <td>${fmtNum(count, 0)}</td>
      <td>${fmtPct(ratio, 2)}</td>
      <td>${fmtNum(drag, 2)}</td>
    </tr>`;
        })
        .join("")
    : '<tr><td colspan="4" class="muted">暂无归因统计。</td></tr>';

  const suggestions = Array.isArray(report.suggestions) ? report.suggestions : [];
  suggestionHost.innerHTML = suggestions.length ? suggestions.map((x) => `<li>${esc(x)}</li>`).join("") : "<li>无</li>";

  const items = Array.isArray(report.items) ? report.items : [];
  itemHost.innerHTML = items.length
    ? items
        .slice(0, 300)
        .map(
          (item) => `<tr>
      <td><code>${esc(item.signal_id || "-")}</code></td>
      <td>${esc(item.symbol || "-")}</td>
      <td>${esc(item.strategy_name || "-")}</td>
      <td>${esc(item.reason_code || "-")}</td>
      <td>${statusChip(item.severity || "INFO")}</td>
      <td>${fmtNum(item.estimated_drag_bps || 0, 2)}</td>
      <td>${esc(item.detail || "-")}</td>
      <td>${esc(item.suggestion || "-")}</td>
    </tr>`
        )
        .join("")
    : '<tr><td colspan="8" class="muted">暂无归因明细。</td></tr>';
}

async function generateClosureReport() {
  const { symbol, strategyName, startDate, endDate, limit } = buildAttributionFilters();
  const body = {
    report_type: "closure",
    symbol: symbol || null,
    strategy_name: strategyName || null,
    start_date: startDate || null,
    end_date: endDate || null,
    limit,
    save_to_file: true,
  };
  const result = await postJSON("/reports/generate", body);
  state.closureReport = result;
  renderClosureReport();
}

function renderClosureReport() {
  const meta = el("closureReportMeta");
  const preview = el("closureReportPreview");
  if (!meta || !preview) return;
  const result = state.closureReport;
  if (!result) {
    meta.textContent = "尚未生成 closure 报表。";
    preview.textContent = "-";
    return;
  }
  meta.textContent = `title=${result.title || "-"} | saved_path=${result.saved_path || "(未落盘)"}`;
  preview.textContent = String(result.content || "-");
}

function buildCostCalibrationPayload() {
  const symbol = String(el("costModelSymbol")?.value || "").trim();
  const strategyName = String(el("costModelStrategy")?.value || "").trim();
  const startDate = String(el("costModelStartDate")?.value || "").trim();
  const endDate = String(el("costModelEndDate")?.value || "").trim();
  const limit = toNumber(el("costModelLimit")?.value, "cost model limit", { integer: true, min: 1, max: 5000 });
  const minSamples = toNumber(el("costModelMinSamples")?.value, "cost model min_samples", {
    integer: true,
    min: 1,
    max: 2000,
  });
  const saveRecord = Boolean(el("costModelSaveRecord")?.checked);
  return {
    symbol: symbol || null,
    strategy_name: strategyName || null,
    start_date: startDate || null,
    end_date: endDate || null,
    limit,
    min_samples: minSamples,
    save_record: saveRecord,
  };
}

async function runCostCalibration() {
  const payload = buildCostCalibrationPayload();
  const result = await postJSON("/replay/cost-model/calibrate", payload);
  state.latestCostCalibration = result;
  renderCostCalibration();
}

async function loadCostCalibrationHistory() {
  const params = new URLSearchParams();
  const symbol = String(el("costModelSymbol")?.value || "").trim();
  const limit = toNumber(el("costModelHistoryLimit")?.value, "cost model history limit", {
    integer: true,
    min: 1,
    max: 200,
  });
  if (symbol) params.set("symbol", symbol);
  params.set("limit", String(limit));
  const rows = await fetchJSON(`/replay/cost-model/calibrations?${params.toString()}`);
  state.latestCostCalibrationHistory = Array.isArray(rows) ? rows : [];
  renderCostCalibrationHistory();
}

function renderCostCalibration() {
  const meta = el("costModelMeta");
  const host = el("costModelRecommendationRows");
  const noteHost = el("costModelNoteList");
  if (!meta || !host || !noteHost) return;
  const result = state.latestCostCalibration;
  if (!result) {
    meta.textContent = "尚未运行成本模型重估。";
    host.innerHTML = '<tr><td colspan="2" class="muted">暂无推荐参数。</td></tr>';
    noteHost.innerHTML = "<li>无</li>";
    return;
  }
  meta.textContent = `samples=${fmtNum(result.sample_size, 0)} | coverage=${fmtPct(
    result.slippage_coverage,
    2
  )} | confidence=${fmtPct(result.confidence, 2)} | calibration_id=${result.calibration_id || "-"}`;
  host.innerHTML = `
    <tr><td>slippage_rate</td><td>${fmtNum(result.recommended_slippage_rate, 6)}</td></tr>
    <tr><td>impact_cost_coeff</td><td>${fmtNum(result.recommended_impact_cost_coeff, 6)}</td></tr>
    <tr><td>fill_probability_floor</td><td>${fmtNum(result.recommended_fill_probability_floor, 6)}</td></tr>
    <tr><td>p90_abs_slippage_bps</td><td>${fmtNum(result.p90_abs_slippage_bps, 2)}</td></tr>
    <tr><td>avg_delay_days</td><td>${fmtNum(result.avg_delay_days, 2)}</td></tr>
  `;
  const notes = Array.isArray(result.notes) ? result.notes : [];
  noteHost.innerHTML = notes.length ? notes.map((x) => `<li>${esc(x)}</li>`).join("") : "<li>无</li>";
}

function renderCostCalibrationHistory() {
  const host = el("costModelHistoryRows");
  if (!host) return;
  const rows = Array.isArray(state.latestCostCalibrationHistory) ? state.latestCostCalibrationHistory : [];
  host.innerHTML = rows.length
    ? rows
        .map((row) => {
          const r = row.result || {};
          return `<tr>
      <td>${fmtNum(row.id, 0)}</td>
      <td>${fmtTs(row.created_at)}</td>
      <td>${esc(r.symbol || "ALL")}</td>
      <td>${esc(r.strategy_name || "ALL")}</td>
      <td>${fmtNum(r.sample_size || 0, 0)}</td>
      <td>${fmtPct(r.slippage_coverage || 0, 2)}</td>
      <td>${fmtNum(r.recommended_slippage_rate || 0, 6)}</td>
      <td>${fmtNum(r.recommended_impact_cost_coeff || 0, 6)}</td>
      <td>${fmtNum(r.recommended_fill_probability_floor || 0, 6)}</td>
      <td>${fmtPct(r.confidence || 0, 2)}</td>
    </tr>`;
        })
        .join("")
    : '<tr><td colspan="10" class="muted">暂无成本重估历史。</td></tr>';
}

function renderExecution() {
  renderPrepRows();
  renderReplaySignalsTable();
  renderReplayReport();
  renderReplayAttribution();
  renderClosureReport();
  renderCostCalibration();
  renderCostCalibrationHistory();
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

  el("autotuneCandidateRows")?.addEventListener("click", (ev) => {
    const target = ev.target;
    if (!(target instanceof HTMLElement)) return;
    const btn = target.closest("button[data-autotune-candidate-idx]");
    if (!btn) return;
    const idx = Number(btn.getAttribute("data-autotune-candidate-idx") || "0");
    const rows = state.latestAutotuneResult && Array.isArray(state.latestAutotuneResult.candidates)
      ? state.latestAutotuneResult.candidates
      : [];
    const item = rows[idx];
    if (!item) return;
    const strategyName = String(state.latestAutotuneResult?.strategy_name || el("strategySelect")?.value || "");
    applyStrategyParamsToForm(strategyName, item.strategy_params || {}, `候选参数(rank=${item.rank || "-"})`);
  });

  el("autotuneProfileRows")?.addEventListener("click", async (ev) => {
    const target = ev.target;
    if (!(target instanceof HTMLElement)) return;
    const btn = target.closest("button[data-autotune-activate-id]");
    if (!btn) return;
    const profileId = btn.getAttribute("data-autotune-activate-id");
    if (!profileId) return;
    try {
      await activateAutotuneProfile(profileId);
    } catch (err) {
      showGlobalError(`切换参数画像失败：${err.message}`);
    }
  });

  el("rolloutRuleRows")?.addEventListener("click", async (ev) => {
    const target = ev.target;
    if (!(target instanceof HTMLElement)) return;
    const btn = target.closest("button[data-rollout-delete-id]");
    if (!btn) return;
    const ruleId = btn.getAttribute("data-rollout-delete-id");
    if (!ruleId) return;
    try {
      await deleteRolloutRule(ruleId);
    } catch (err) {
      showGlobalError(`删除灰度规则失败：${err.message}`);
    }
  });

  el("holdingTradeRows")?.addEventListener("click", async (ev) => {
    const target = ev.target;
    if (!(target instanceof HTMLElement)) return;
    const btn = target.closest("button[data-holding-trade-delete-id]");
    if (!btn) return;
    const tradeId = btn.getAttribute("data-holding-trade-delete-id");
    if (!tradeId) return;
    try {
      await deleteHoldingTrade(tradeId);
    } catch (err) {
      showGlobalError(`删除手工成交失败：${err.message}`);
    }
  });
}

function bindStrategyInputEvents() {
  const selector =
    "#tab-strategy input, #tab-strategy select, #tab-strategy textarea, " +
    "#tab-autotune input, #tab-autotune select, #tab-autotune textarea, " +
    "#tab-challenge input, #tab-challenge select, #tab-challenge textarea, " +
    "#tab-holdings input, #tab-holdings select, #tab-holdings textarea";
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
    loadRolloutRules({ silent: true }).catch(() => {});
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
  if (el("attrStartDateInput") && !el("attrStartDateInput").value) el("attrStartDateInput").value = minusDaysISO(90);
  if (el("attrEndDateInput") && !el("attrEndDateInput").value) el("attrEndDateInput").value = todayISO();
  if (el("costModelStartDate") && !el("costModelStartDate").value) el("costModelStartDate").value = minusDaysISO(120);
  if (el("costModelEndDate") && !el("costModelEndDate").value) el("costModelEndDate").value = todayISO();
  if (el("holdingTradeDateInput") && !el("holdingTradeDateInput").value) el("holdingTradeDateInput").value = todayISO();
  if (el("holdingAsOfDateInput") && !el("holdingAsOfDateInput").value) el("holdingAsOfDateInput").value = todayISO();
  if (el("holdingTradeFilterStartDateInput") && !el("holdingTradeFilterStartDateInput").value) {
    el("holdingTradeFilterStartDateInput").value = minusDaysISO(180);
  }
  if (el("holdingTradeFilterEndDateInput") && !el("holdingTradeFilterEndDateInput").value) {
    el("holdingTradeFilterEndDateInput").value = todayISO();
  }
  if (el("holdingAccEndDateInput") && !el("holdingAccEndDateInput").value) {
    el("holdingAccEndDateInput").value = todayISO();
  }
  if (el("goLiveEndDateInput") && !el("goLiveEndDateInput").value) {
    el("goLiveEndDateInput").value = todayISO();
  }
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
      await loadAutotuneProfiles({ silent: true }).catch(() => {});
      await loadAutotuneActiveProfile({ silent: true }).catch(() => {});
      await loadRolloutRules({ silent: true }).catch(() => {});
      await loadReplaySignals().catch(() => {});
      await loadReplayReport().catch(() => {});
      await loadReplayAttribution().catch(() => {});
      await loadHoldingTrades({ silent: true }).catch(() => {});
      await loadHoldingPositions({ silent: true }).catch(() => {});
      await loadHoldingAccuracyReport({ silent: true }).catch(() => {});
      await loadGoLiveReadinessReport({ silent: true }).catch(() => {});
      syncBarsInputsFromStrategy();
      syncRebalanceDefaults();
      await loadMarketBars({ silent: true }).catch(() => {});
      updateRequestPreview();
      renderResults();
      renderChallengeWorkbench();
      renderHoldings();
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

  el("runPortfolioBacktestBtn")?.addEventListener("click", async () => {
    try {
      await runPortfolioBacktest();
      updateRequestPreview();
      saveFormSnapshot();
      el("lastUpdated").textContent = `最近更新时间：${new Date().toLocaleString()}`;
    } catch (err) {
      showGlobalError(`组合回测失败：${err.message}`);
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

  el("runAutotuneBtn")?.addEventListener("click", async () => {
    try {
      await runAutotune();
      updateRequestPreview();
      saveFormSnapshot();
      el("lastUpdated").textContent = `最近更新时间：${new Date().toLocaleString()}`;
    } catch (err) {
      showGlobalError(`自动调参失败：${err.message}`);
    }
  });

  el("runChallengeBtn")?.addEventListener("click", async () => {
    try {
      await runStrategyChallenge();
      updateRequestPreview();
      saveFormSnapshot();
      el("lastUpdated").textContent = `最近更新时间：${new Date().toLocaleString()}`;
    } catch (err) {
      showGlobalError(`挑战赛运行失败：${err.message}`);
    }
  });

  el("applyChallengeChampionBtn")?.addEventListener("click", () => {
    try {
      applyChallengeChampionToStrategyForm();
      switchTab("strategy");
    } catch (err) {
      showGlobalError(`冠军参数回填失败：${err.message}`);
    }
  });

  el("loadAutotuneProfilesBtn")?.addEventListener("click", async () => {
    try {
      await loadAutotuneProfiles();
    } catch (err) {
      showGlobalError(`加载参数画像失败：${err.message}`);
    }
  });

  el("loadAutotuneActiveBtn")?.addEventListener("click", async () => {
    try {
      await loadAutotuneActiveProfile();
    } catch (err) {
      showGlobalError(`读取生效画像失败：${err.message}`);
    }
  });

  el("applyAutotuneBestParamsBtn")?.addEventListener("click", () => {
    const result = state.latestAutotuneResult;
    if (!result || !result.best) {
      showGlobalError("暂无自动调参最优结果，请先运行自动调参。");
      return;
    }
    applyStrategyParamsToForm(result.strategy_name, result.best.strategy_params || {}, "最优参数");
  });

  el("rollbackAutotuneBtn")?.addEventListener("click", async () => {
    try {
      await rollbackAutotuneProfile();
    } catch (err) {
      showGlobalError(`回滚参数画像失败：${err.message}`);
    }
  });

  el("upsertRolloutRuleBtn")?.addEventListener("click", async () => {
    try {
      await upsertRolloutRule();
    } catch (err) {
      showGlobalError(`保存灰度规则失败：${err.message}`);
    }
  });

  el("loadRolloutRulesBtn")?.addEventListener("click", async () => {
    try {
      await loadRolloutRules();
    } catch (err) {
      showGlobalError(`加载灰度规则失败：${err.message}`);
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

  el("submitHoldingTradeBtn")?.addEventListener("click", async () => {
    try {
      await submitHoldingTrade();
      el("lastUpdated").textContent = `最近更新时间：${new Date().toLocaleString()}`;
    } catch (err) {
      showGlobalError(`提交手工成交失败：${err.message}`);
    }
  });

  el("loadHoldingTradesBtn")?.addEventListener("click", async () => {
    try {
      await loadHoldingTrades();
    } catch (err) {
      showGlobalError(`加载成交台账失败：${err.message}`);
    }
  });

  el("applyHoldingTradeFilterBtn")?.addEventListener("click", async () => {
    try {
      await loadHoldingTrades();
    } catch (err) {
      showGlobalError(`按条件加载成交台账失败：${err.message}`);
    }
  });

  el("loadHoldingPositionsBtn")?.addEventListener("click", async () => {
    try {
      await loadHoldingPositions();
    } catch (err) {
      showGlobalError(`加载持仓快照失败：${err.message}`);
    }
  });

  el("loadHoldingAccuracyBtn")?.addEventListener("click", async () => {
    try {
      await loadHoldingAccuracyReport();
    } catch (err) {
      showGlobalError(`刷新准确性看板失败：${err.message}`);
    }
  });

  el("loadGoLiveReadinessBtn")?.addEventListener("click", async () => {
    try {
      await loadGoLiveReadinessReport();
    } catch (err) {
      showGlobalError(`刷新上线准入报告失败：${err.message}`);
    }
  });

  el("runHoldingAnalyzeBtn")?.addEventListener("click", async () => {
    try {
      await runHoldingAnalyze();
      switchTab("holdings");
    } catch (err) {
      showGlobalError(`运行持仓分析失败：${err.message}`);
    }
  });

  el("syncHoldingStrategyBtn")?.addEventListener("click", () => {
    const strategyName = String(el("strategySelect")?.value || "").trim();
    if (el("holdingAnalyzeStrategyInput")) el("holdingAnalyzeStrategyInput").value = strategyName;
    if (el("holdingAnalyzeLotSizeInput") && el("lotSizeInput")) {
      el("holdingAnalyzeLotSizeInput").value = String(el("lotSizeInput").value || "100");
    }
    setHoldingMessage("已同步策略名称和 lot_size，可直接运行持仓分析。");
  });

  el("jumpToStrategyBtn")?.addEventListener("click", () => switchTab("strategy"));
  el("jumpToAutotuneBtn")?.addEventListener("click", () => switchTab("autotune"));
  el("jumpToChallengeBtn")?.addEventListener("click", () => switchTab("challenge"));
  el("jumpToHoldingsBtn")?.addEventListener("click", () => switchTab("holdings"));
  el("jumpToAutotuneCardBtn")?.addEventListener("click", () => switchTab("autotune"));
  el("jumpToAutotuneFromResultsHeadBtn")?.addEventListener("click", () => switchTab("autotune"));
  el("jumpToAutotuneFromResultsBtn")?.addEventListener("click", () => switchTab("autotune"));
  el("jumpToStrategyFromAutotuneBtn")?.addEventListener("click", () => switchTab("strategy"));
  el("jumpToResultsFromAutotuneBtn")?.addEventListener("click", () => switchTab("results"));
  el("jumpToStrategyFromChallengeBtn")?.addEventListener("click", () => switchTab("strategy"));
  el("jumpToAutotuneFromChallengeBtn")?.addEventListener("click", () => switchTab("autotune"));
  el("jumpToResultsFromChallengeBtn")?.addEventListener("click", () => switchTab("results"));
  el("jumpToStrategyFromHoldingsBtn")?.addEventListener("click", () => switchTab("strategy"));
  el("jumpToResultsFromHoldingsBtn")?.addEventListener("click", () => switchTab("results"));
  el("jumpToExecutionFromHoldingsBtn")?.addEventListener("click", () => switchTab("execution"));
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

  el("loadReplayAttributionBtn")?.addEventListener("click", async () => {
    try {
      await loadReplayAttribution();
      setExecutionMessage("偏差归因报表已刷新");
    } catch (err) {
      showGlobalError(`加载偏差归因失败：${err.message}`);
    }
  });

  el("generateClosureReportBtn")?.addEventListener("click", async () => {
    try {
      await generateClosureReport();
      setExecutionMessage("closure 报表已生成");
    } catch (err) {
      showGlobalError(`生成 closure 报表失败：${err.message}`);
    }
  });

  el("runCostCalibrationBtn")?.addEventListener("click", async () => {
    try {
      await runCostCalibration();
      setExecutionMessage("成本模型重估完成");
      await loadCostCalibrationHistory().catch(() => {});
    } catch (err) {
      showGlobalError(`运行成本模型重估失败：${err.message}`);
    }
  });

  el("loadCostCalibrationHistoryBtn")?.addEventListener("click", async () => {
    try {
      await loadCostCalibrationHistory();
      setExecutionMessage("成本模型重估历史已刷新");
    } catch (err) {
      showGlobalError(`加载成本模型重估历史失败：${err.message}`);
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
    await loadAutotuneProfiles({ silent: true }).catch(() => {});
    await loadAutotuneActiveProfile({ silent: true }).catch(() => {});
    await loadRolloutRules({ silent: true }).catch(() => {});
    await loadHoldingTrades({ silent: true }).catch(() => {});
    await loadHoldingPositions({ silent: true }).catch(() => {});
    await loadHoldingAccuracyReport({ silent: true }).catch(() => {});
    await loadGoLiveReadinessReport({ silent: true }).catch(() => {});
    await loadCostCalibrationHistory().catch(() => {});
    syncBarsInputsFromStrategy();
    syncRebalanceDefaults();
    updateSmallCapitalHint();

    bindStrategyInputEvents();
    bindResultSelectionEvents();
    await initHandlers();

    updateRequestPreview();
    renderResults();
    renderChallengeWorkbench();
    renderHoldings();
    renderExecution();

    el("lastUpdated").textContent = `最近更新时间：${new Date().toLocaleString()}`;
  } catch (err) {
    showGlobalError(`初始化失败：${err.message}`);
  }
}

bootstrap();
