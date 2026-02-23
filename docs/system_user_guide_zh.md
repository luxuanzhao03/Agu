# A股半自动交易辅助系统使用手册（Windows）

更新时间：2026-02-23
适用版本：当前仓库主干（含自动调参、挑战赛、持仓分析、准确性与上线准入看板）

## 1. 系统定位与边界

本系统是“投研与交易决策支持平台”，不是自动下单系统。

- 支持：数据拉取、因子计算、策略信号、风控、回测、组合优化、执行回写、复盘、审计、运维监控。
- 不支持：券商 API 自动下单、收益保证、零回撤保证。
- 结论：系统目标是“提升胜率与执行质量、控制风险”，不是“保证不亏钱”。

## 2. 页面与导航总览

系统前端入口共 3 个：

1. 主界面：`/ui/`
2. 投研交易工作台：`/trading/workbench`
3. 运维看板：`/ops/dashboard`

工作台内有 6 个主标签页：

1. 策略与参数页
2. 自动调参页
3. 跨策略挑战赛页
4. 结果可视化页
5. 持仓分析页
6. 交易准备单与执行回写页

## 3. 功能覆盖矩阵（已实现）

| 功能域 | 已实现能力 | 主要页面/API |
|---|---|---|
| 自动调参防过拟合 | walk-forward 多窗口、稳定性惩罚、参数漂移惩罚、收益方差惩罚 | `POST /autotune/run`、自动调参页 |
| 自动调参前端化 | 任务运行、候选榜、基线对比、画像激活/回滚、灰度规则 | 自动调参页、`/autotune/*` |
| 画像回滚与灰度 | 一键回滚上个画像、按策略/按标的灰度启用 | `POST /autotune/profiles/rollback`、`/autotune/rollout/rules/*` |
| 跨策略挑战赛 | 六策略同窗评测、硬门槛筛选、冠军亚军、灰度计划 | 挑战赛页、`POST /challenge/run` |
| 组合级回测 | 多标的净值、调仓周期、仓位上限、行业/主题约束、资金利用率 | `POST /backtest/portfolio-run` |
| 拟真成本与成交 | 最低佣金、印花税、过户费、冲击成本、分档滑点、成交概率 | 回测引擎、`/replay/cost-model/*` |
| 风控增强 | 连续亏损、单日亏损、VaR/ES、集中度阈值、T+1/ST/停牌等 | `POST /risk/check`、`POST /portfolio/risk/check` |
| 研究-执行闭环 | 建议单 -> 手工执行回写 -> 偏差归因 -> 参数建议 | 执行回写页、`/replay/*`、`/reports/generate` |
| 数据层增强 | 多源回退、时序缓存与增量补拉、字段质量评分 | `data/*`、`POST /data/quality/report` |
| 数据许可证治理 | 数据集授权登记、用途校验、导出权限检查 | `POST /data/licenses/register`、`POST /data/licenses/check` |
| 因子快照与落库留痕 | 因子实时快照、数据快照哈希登记、可追溯 | `GET /factors/snapshot`、`POST /data/snapshots/register` |
| 策略治理与审批 | 版本注册、送审、多角色审批、运行时强制只用已审批版本 | `/strategy-governance/*` |
| 研究流水线编排 | 每日研究管线一键运行（信号+事件增强+小资金过滤） | `POST /pipeline/daily-run` |
| 模型风险监控 | 策略漂移检测、风险告警、审计留痕 | `POST /model-risk/drift-check` |
| 系统配置与权限 | 环境配置查看、鉴权角色识别、权限矩阵查询 | `/system/config`、`/system/auth/*` |
| 生产运维与审计 | 作业调度、SLA、告警降噪、证据包、审计哈希链 | `/ops/*`、`/alerts/*`、`/compliance/evidence/*` |
| 持仓分析闭环 | 手工成交、持仓快照、次日预测、动作建议（ADD/REDUCE/EXIT/HOLD/BUY_NEW） | 持仓分析页、`/holdings/*` |
| 策略准确性看板 | OOS 命中率、Brier、收益偏差、成本后收益、执行覆盖率 | `GET /reports/strategy-accuracy` |
| 上线准入与回滚建议 | 门槛判定、自动回滚触发器、每日验收清单 | `GET /reports/go-live-readiness` |

## 4. 启动方式与端口

### 4.1 一键启动（推荐）

双击：`start_system_windows.bat`

默认后端端口：`127.0.0.1:8000`

脚本会自动：

1. 创建/复用 `.venv`
2. 创建/复用 `.env`
3. 安装依赖
4. 启动 API
5. 打开三个页面：`/ui/`、`/trading/workbench`、`/ops/dashboard`

### 4.2 手动启动（PowerShell）

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\bootstrap.ps1
powershell -ExecutionPolicy Bypass -File .\scripts\run_api.ps1
```

### 4.3 常见启动日志说明

- `GET /favicon.ico 404`：正常，不影响业务功能。
- `ops_scheduler_enabled bool parsing error`：`.env` 值后面有空格（例如 `true `），改成严格 `true`/`false`。

## 5. 工作台使用说明（按标签页）

### 5.1 策略与参数页

主要用于生成信号、回测、研究与组合调仓。

关键能力：

1. 选择策略与参数（6 策略）。
2. 配置事件增强、财报增强、小资金模式。
3. 运行：
   - `运行信号生成` -> `POST /signals/generate`
   - `运行回测` -> `POST /backtest/run`
   - `运行组合净值回测` -> `POST /backtest/portfolio-run`
   - `运行研究工作流` -> `POST /research/run`
4. 小资金模板：`2000/5000/8000` 一键套参。

### 5.2 自动调参页

主要用于参数搜索、过拟合抑制和画像管理。

关键能力：

1. 搜索空间 + 基线参数设置。
2. walk-forward 多窗口验证。
3. 候选排行榜（objective、overfit、stability、param drift、return variance）。
4. 参数画像管理：
   - 激活画像
   - 一键回滚上一个画像
   - 查看当前生效画像
5. 灰度规则：按策略/按标的启停。

### 5.3 跨策略挑战赛页

用于“同窗口、同约束”下比较六策略市场适配性。

关键能力：

1. 同时跑六策略（每个策略先调参再评测）。
2. 硬门槛筛选（回撤、夏普、交易数、walk-forward 稳定性）。
3. 综合评分排序（收益 + 稳定性 - 回撤 - 方差惩罚）。
4. 输出冠军/亚军与灰度上线计划。
5. 冠军参数可一键回填到策略页。

### 5.4 结果可视化页

用于看图看表并联动调仓。

关键能力：

1. KPI：信号数、阻断数、回测收益/回撤/夏普等。
2. 信号表 + 风控明细联动。
3. K 线叠加信号点位。
4. 组合权重图、行业暴露图。
5. 调仓计划联动：`POST /portfolio/rebalance/plan`。
6. 组合级回测结果展示。

### 5.5 持仓分析页

用于手工交易台账、持仓画像、次日建议和准确性复盘。

#### 5.5.1 手工成交录入（`POST /holdings/trades`）

字段说明：

1. `trade_date`：成交日期
2. `symbol` / `symbol_name`
3. `side`：BUY/SELL
4. `price`：成交价
5. `lots` / `lot_size`
6. `fee`：总费用
7. `reference_price`：建议/参考价（用于滑点评估）
8. `executed_at`：成交时间
9. `is_partial_fill`：是否部分成交
10. `unfilled_reason`：未成交或部分成交原因
11. `note`

#### 5.5.2 持仓快照与次日建议

- 持仓快照：`GET /holdings/positions`
- 次日分析：`POST /holdings/analyze`

建议动作包括：

1. `ADD`
2. `REDUCE`
3. `EXIT`
4. `HOLD`
5. `BUY_NEW`

每条建议含：`target_lots`、`delta_lots`、`confidence`、`risk_flags`、执行时段建议等。

#### 5.5.3 策略准确性看板

接口：`GET /reports/strategy-accuracy`

核心指标：

1. 样本外命中率（hit rate）
2. Brier 分数（概率校准误差）
3. 收益偏差（预测-实际）
4. 成本后动作收益
5. 执行覆盖率（建议是否被回写执行）

#### 5.5.4 上线准入门槛表

接口：`GET /reports/go-live-readiness`

输出内容：

1. 门槛检查（pass/fail）
2. Readiness 等级（`BLOCKED` / `GRAY_READY_WITH_WARNINGS` / `GRAY_READY`）
3. 自动回滚触发规则
4. 每日验收清单

### 5.6 交易准备单与执行回写页

关键能力：

1. 查看建议单并一键填入执行单。
2. 执行回写：`POST /replay/executions/record`。
3. 执行复盘：`GET /replay/report`。
4. 偏差归因：`GET /replay/attribution`。
5. closure 报告：`POST /reports/generate` (`report_type=closure`)。
6. 成本模型重估：`POST /replay/cost-model/calibrate`。

## 6. 从研究到执行的闭环（标准流程）

1. 在策略页生成信号。
2. 在结果页检查风控、回测、组合建议。
3. 手工下单后录入持仓交易台账。
4. 在执行页回写成交。
5. 查看复盘与归因，识别偏差来源。
6. 刷新准确性看板与上线准入报告。
7. 若不达标，回滚画像并调整参数，再进入下一轮。

## 7. 数据与算法说明

### 7.1 数据源

1. `tushare` 优先
2. `akshare` 回退
3. 本地缓存：时序增量补拉（减少重复请求）

### 7.2 频率支持

1. 日线：主策略与回测主频
2. 分钟线（1m/5m/15m/30m/60m）：用于持仓页执行时段建议与盘中风险辅助

### 7.3 算法原则

1. 不选“单段利润最高”，选“样本外更稳”。
2. 多指标综合评分而非单一收益。
3. 先过硬门槛，再做排名。

### 7.4 因子快照与模型漂移

1. 可用 `GET /factors/snapshot` 查看指定标的在指定区间的最新因子值（含基本面增强因子）。
2. 每次因子快照会登记数据快照哈希，便于回测与实盘对账追溯。
3. 可用 `POST /model-risk/drift-check` 做策略漂移检测，防止模型在市场切换后失效。

## 8. 风控、成本与可交易性

系统已覆盖：

1. T+1、ST、停牌、涨跌停等基础约束
2. 单票仓位上限、行业/主题集中度约束
3. 连续亏损、单日亏损、VaR/ES 等组合风险控制
4. 最低佣金、印花税、过户费
5. 拟真滑点与冲击成本建模
6. 小资金可交易过滤（能否买一手、边际是否覆盖成本）

## 9. 运维、告警与审计

### 9.1 运维作业

- 作业注册、触发、调度、SLA 检查：`/ops/jobs/*`

### 9.2 告警系统

- 订阅、去重、ACK、值班回调、对账：`/alerts/*`

### 9.3 合规证据包

- 导出、签名、复签、校验：`/compliance/evidence/*`

### 9.4 审计链

- 审计查询：`/audit/events`
- 链校验：`/audit/verify-chain`

## 10. 关键 API 分组索引

### 10.1 系统与鉴权

- `GET /system/config`
- `GET /system/auth/me`
- `GET /system/auth/permissions`

### 10.2 市场与数据接入

- `GET /market/bars`
- `GET /market/intraday`
- `GET /market/calendar`
- `GET /market/tushare/capabilities`
- `POST /market/tushare/prefetch`

### 10.3 数据治理与许可证

- `POST /data/quality/report`
- `POST /data/snapshots/register`
- `GET /data/snapshots`
- `POST /data/pit/validate`
- `POST /data/pit/validate-events`
- `POST /data/licenses/register`
- `GET /data/licenses`
- `POST /data/licenses/check`

### 10.4 事件与 NLP

- `POST /events/sources/register`
- `POST /events/ingest`
- `GET /events`
- `GET /events/connectors/overview`
- `POST /events/connectors/run`
- `GET /events/connectors/sla`
- `POST /events/nlp/normalize/ingest`
- `POST /events/nlp/drift-check`
- `GET /events/nlp/drift/slo/history`

### 10.5 因子、策略与治理

- `GET /factors/snapshot`
- `GET /strategies`
- `GET /strategies/{strategy_name}`
- `POST /strategy-governance/register`
- `POST /strategy-governance/submit-review`
- `POST /strategy-governance/decide`
- `GET /strategy-governance/policy`
- `POST /model-risk/drift-check`

### 10.6 信号、风控与组合

- `POST /signals/generate`
- `POST /risk/check`
- `POST /portfolio/risk/check`
- `POST /portfolio/optimize`
- `POST /portfolio/rebalance/plan`
- `POST /portfolio/stress-test`

### 10.7 回测、研究、调参与挑战赛

- `POST /backtest/run`
- `POST /backtest/portfolio-run`
- `POST /research/run`
- `POST /pipeline/daily-run`
- `POST /autotune/run`
- `GET /autotune/profiles`
- `POST /autotune/profiles/rollback`
- `POST /challenge/run`

### 10.8 持仓与上线闭环

- `POST /holdings/trades`
- `GET /holdings/trades`
- `GET /holdings/positions`
- `POST /holdings/analyze`
- `GET /reports/strategy-accuracy`
- `GET /reports/go-live-readiness`

### 10.9 执行回写与复盘

- `POST /replay/signals/record`
- `POST /replay/executions/record`
- `GET /replay/report`
- `GET /replay/attribution`
- `POST /replay/cost-model/calibrate`
- `GET /replay/cost-model/calibrations`

### 10.10 报告、合规与证据包

- `POST /reports/generate`
- `POST /compliance/preflight`
- `POST /compliance/evidence/export`
- `POST /compliance/evidence/verify`
- `POST /compliance/evidence/countersign`

### 10.11 运维调度与告警

- `GET /metrics/summary`
- `GET /metrics/ops-dashboard`
- `POST /ops/jobs/register`
- `POST /ops/jobs/{job_id}/run`
- `POST /ops/jobs/scheduler/tick`
- `GET /ops/jobs/scheduler/sla`
- `GET /ops/dashboard`
- `GET /alerts/recent`
- `POST /alerts/oncall/reconcile`

### 10.12 审计

- `GET /audit/events`
- `GET /audit/export`
- `GET /audit/verify-chain`

## 11. 每日操作 SOP（建议）

### 11.1 盘前

1. 看 `/ops/dashboard`，确认无 critical。
2. 跑挑战赛或确认当前冠军画像仍生效。
3. 刷新持仓分析，生成次日建议。
4. 刷新上线准入报告，确认是否允许继续灰度。

### 11.2 盘中

1. 参考持仓页推荐执行时段。
2. 手工下单后立即回填成交记录。
3. 若触发回滚条件，停止增仓并回滚画像。

### 11.3 盘后

1. 回写执行并刷新复盘归因。
2. 刷新准确性看板与上线准入看板。
3. 记录异常与第二天修正动作。

## 12. 上线准入建议（实盘灰度）

建议门槛（可按你的风险偏好微调）：

1. OOS 样本数 `>= 40`
2. OOS 命中率 `>= 55%`
3. Brier `<= 0.23`
4. 执行覆盖率 `>= 70%`
5. 成本后动作收益 `>= 0`
6. 执行跟随率 `>= 65%`
7. 平均滑点 `<= 35 bps`
8. 平均延迟 `<= 1.2 天`
9. 最近 30 天存在挑战赛验证结果

自动回滚触发器（示例）：

1. 连续 3 日亏损
2. 单日组合收益 `<= -2.5%`
3. 灰度窗口最大回撤 `> 6%`
4. 5 日执行覆盖率 `< 60%` 或 5 日平均滑点 `> 45 bps`

## 13. 常见问题

1. 标签页点不动
- 先看浏览器控制台是否有 JS 报错。
- 强刷缓存后重试。
- 确认 `/ui/trading-workbench/app.js` 能正常加载（HTTP 200）。

2. 文档或页面出现乱码
- 确认文件保存为 UTF-8。
- Windows 终端建议使用 `chcp 65001`。

3. 启动失败提示布尔解析错误
- `.env` 不要写 `true `（末尾空格），必须是 `true` 或 `false`。

4. 为什么收益和回测不同
- 实盘有执行偏差、滑点、延迟与漏单。
- 请以“成本后收益 + 准确性看板 + 上线准入报告”综合判断。

## 14. 结语

这份手册已经覆盖当前系统主功能面。若后续新增模块，请同步更新本文件和 `README.md` 的“能力范围”与“API 总览”。
