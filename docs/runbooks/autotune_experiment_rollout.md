# 自动调参与灰度生效 Runbook

## 1. 目标与适用范围
- 目标：规范“自动调参实验 -> 画像生效 -> 监控 -> 回滚 -> 证据归档”全流程，降低一次性全量切换风险。
- 适用范围：`/autotune/*`、`/backtest/portfolio-run`、交易工作台“自动调参页”、运维作业 `auto_tune` / `execution_review`。

## 2. 预检清单（执行前）
- 策略状态：策略版本已通过治理审批（若开启 `ENFORCE_APPROVED_STRATEGY=true`）。
- 数据可用性：目标区间行情、事件、财报数据完整；`/data/quality/report` 无 critical 问题。
- 回测环境：费用模型参数与当前交易环境一致（佣金、印花税、过户费、滑点、冲击成本、成交概率底线）。
- 风险阈值：确认组合风控阈值（最大回撤、单日亏损、连续亏损、行业/主题集中、VaR/ES）已配置。
- 权限校验：执行账号具有 `research`/`risk`（必要时 `admin`）角色。

## 3. 实验执行流程
1. 在工作台“自动调参页”配置 `search_space`、`walk_forward_slices`、`objective_weight_param_drift`。
2. 开启拟真成本模型（`enable_realistic_cost_model=true`），并设置冲击成本/成交概率参数。
3. 运行 `POST /autotune/run`，检查输出：
   - `best` 与 `baseline` 的 `objective_score` 和 `improvement_vs_baseline`
   - `stability_penalty`、`param_drift_penalty`
   - `apply_decision` 与 `apply_guard_reason`
4. 如需手动激活，使用 `POST /autotune/profiles/{profile_id}/activate`。

## 4. 灰度发布策略
- 策略级灰度：`POST /autotune/rollout/rules/upsert`，`symbol` 置空。
- 标的级灰度：同接口填写 `symbol`，仅对单标的启停画像覆盖。
- 推荐顺序：
  1. 先单标的灰度（高流动性、低风险标的）。
  2. 再策略级小范围放量。
  3. 最后全策略生效。
- 禁用画像：设置 `enabled=false`，系统将回退到显式请求参数。

## 5. 回滚流程
- 一键回滚：`POST /autotune/profiles/rollback`。
- 回滚后验证：
  - `GET /autotune/profiles/active` 确认 active 画像 ID 已切换。
  - 抽样运行 `signals/backtest`，确认关键风险指标回归正常。

## 6. 作业失败与重试策略
- `ops/job_service.py` 支持 payload 内重试配置：
  - `_retry.max_retries`
  - `_retry.backoff_seconds`
- 推荐参数：
  - 非交易时段批处理：`max_retries=3`, `backoff_seconds=60`
  - 盘中轻量任务：`max_retries=1`, `backoff_seconds=15`
- 触发失败后，优先检查：
  - 数据源连通性与 token 额度
  - 日期区间与字段完整性
  - 策略参数是否越界

## 7. 告警与降噪
- 告警源：审计事件 + SLA 事件 + 作业运行状态。
- 降噪规则（`alerts.service`）建议：
  - `min_repeat_count`：同类重复达到阈值才升级通知
  - `max_escalation_level`：限制升级层级
  - `message_keywords`：仅对核心失败关键词触发
  - `allow_critical_suppression=false`：critical 不抑制

## 8. 复盘闭环
- 每日自动任务：`execution_review`（生成 closure 报表）。
- 核心报表：
  - `/replay/report`：跟随率、延迟、滑点
  - `/replay/attribution`：偏差原因统计 + 参数修正建议
  - `/reports/generate`（`report_type=closure`）：固化复盘结论
- 输出动作：
  - 将高频偏差原因映射到调参候选约束（如提高成交概率底线、降低仓位上限、调整调仓频率）。

## 9. 证据包与审计留存
- 导出：`POST /compliance/evidence/export`
- 关键文件：
  - `autotune_events.jsonl`
  - `autotune_profiles.json`
  - `autotune_rollout_rules.json`
- 审计要求：
  - 记录每次实验 run_id、生效画像 ID、灰度规则变更、回滚操作与执行人。
  - 保留至少一个完整“实验->生效->回滚（如发生）”链路样本。

## 10. 例行巡检（建议）
- 日巡检：
  - 最新 active 画像是否符合当前市场状态
  - 灰度规则是否存在过期例外（symbol 级规则长期未清理）
- 周巡检：
  - Walk-forward 稳定性是否下降
  - 参数漂移惩罚是否持续上升
  - Closure 报表中偏差原因是否结构性变化
