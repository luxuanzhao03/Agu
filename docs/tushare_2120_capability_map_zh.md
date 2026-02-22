# Tushare 2120 积分能力映射（系统接入版）

本文档用于说明：在 `Tushare 2120` 积分下，系统当前如何识别可用数据种类、哪些已经接入策略主链路、如何一键预取。

## 1. 当前接入原则

- 数据优先级：`tushare -> akshare`（失败自动回退）。
- 积分门槛：按 `min_points_hint` 计算可用性，2120 默认覆盖 2000 档数据，不覆盖 5000 档（例如 `bak_daily`）。
- 接口可用性：同时检查本地 `pro_api` 是否存在对应方法（`api_available`）。
- 策略接入标准：只有“日频可对齐 + 与交易决策直接相关”的字段才进入因子引擎实时打分。

## 2. 数据集能力目录（2120 视角）

### 2.1 已接入策略主链路（自动融合到 `/market/bars` 日线）

1. `daily_basic`（2000）
- 关键字段：`turnover_rate`, `turnover_rate_f`, `volume_ratio`, `pe_ttm`, `pb`, `ps_ttm`, `dv_ttm`, `circ_mv`
- 系统字段：`ts_turnover_rate*`, `ts_pe_ttm`, `ts_pb`, `ts_ps_ttm`, `ts_dv_ttm`, `ts_circ_mv`
- 用途：估值评分 + 可交易性评分

2. `moneyflow`（2000）
- 关键字段：`net_mf_amount`, `buy_elg_amount`, `sell_elg_amount`, `buy_lg_amount`, `sell_lg_amount`
- 系统字段：`ts_net_mf_amount`, `ts_main_net_mf_amount`, `ts_buy_elg_amount`, `ts_sell_elg_amount`
- 用途：资金流评分 + 买入侧确认

3. `stk_limit`（2000）
- 关键字段：`up_limit`, `down_limit`
- 系统字段：`ts_up_limit`, `ts_down_limit`
- 用途：可交易空间评分、涨跌停边界感知

4. `adj_factor`（2000）
- 关键字段：`adj_factor`
- 系统字段：`ts_adj_factor`
- 用途：复权一致性辅助检查

5. `fina_indicator` / `income` / `balancesheet` / `cashflow`（2000）
- 用途：财报快照与基本面评分链路
- 说明：由基本面增强服务统一接入，不直接作为日线 merge 字段。

### 2.2 已纳入能力目录与批量预取（暂未直接进入实时策略打分）

- `forecast`, `express`, `dividend`, `fina_audit`
- `top10_holders`, `top10_floatholders`, `stk_holdernumber`
- `pledge_stat`, `pledge_detail`, `repurchase`, `share_float`, `block_trade`

用途定位：
- 事件治理/NLP：公告触发、情绪修正、风险事件标签。
- 研究分析：股东结构变化、质押风险、公司行为。
- 合规与复盘：预取留痕，便于证据包与审计追溯。

### 2.3 2120 下默认不可用（能力目录会标记）

- `bak_daily`（5000）

## 3. 新增系统 API（可直接在前端或脚本调用）

1. 查询能力目录：

```bash
curl "http://127.0.0.1:8000/market/tushare/capabilities?user_points=2120"
```

返回重点字段：
- `eligible`: 积分是否满足
- `api_available`: 本地 pro_api 是否具备该方法
- `ready_to_call`: 当前是否可直接调用
- `integrated_in_system`: 是否已接入策略主链路
- `integrated_targets`: 接入模块清单

2. 批量预取（逐数据集状态）：

```bash
curl -X POST "http://127.0.0.1:8000/market/tushare/prefetch" \
  -H "Content-Type: application/json" \
  -d "{\"symbol\":\"000001\",\"start_date\":\"2025-01-01\",\"end_date\":\"2025-12-31\",\"user_points\":2120,\"include_ineligible\":false}"
```

返回重点字段：
- `summary.success/failed/skipped`
- `results[].status`（`success|failed|skipped_ineligible|skipped_api_unavailable`）
- `results[].used_params`（实际调用参数）
- `results[].row_count/column_count`

## 4. 策略与因子接入结果

1. 因子引擎新增：
- `tushare_valuation_score`
- `tushare_moneyflow_score`
- `tushare_tradability_score`
- `tushare_advanced_score`
- `tushare_advanced_completeness`

2. 策略升级：
- `multi_factor`：新增 `w_tushare_advanced` 与 `min_tushare_score_buy`
- `small_capital_adaptive`：新增 `min_tushare_advanced_score_buy`，低分直接降级 `WATCH`

3. 兼容性：
- 若高级字段缺失，不会报错；默认回退到中性分（0.5），确保主流程稳定。

## 5. 推荐使用流程

1. 在 `.env` 设置：`DATA_PROVIDER_PRIORITY=tushare,akshare` 并填写 `TUSHARE_TOKEN`。
2. 调用 `/market/tushare/capabilities` 确认 2120 下的 `ready_to_call`。
3. 对重点股票先调用 `/market/tushare/prefetch` 做预热与可用性检查。
4. 再运行 `/signals/generate` / `/research/run` / `/backtest/run`，观察 `tushare_advanced_score` 的影响。
