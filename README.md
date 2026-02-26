# A股半自动交易辅助系统

本仓库是面向生产落地的 **半自动** A股交易辅助系统基础平台。

系统边界：
- 仅用于投研与决策支持。
- 不集成券商交易执行。
- 不自动下单。
- 最终交易动作由人工完成。

## 应用统计改编说明（复试展示）

为匹配应用统计专业复试展示场景，项目新增了独立的应用统计研究模块，不要求评审老师理解量化交易工程细节，也可以直接展示统计方法能力。

- 新增 `applied_stats` 模块：描述统计、相关性分析、Jarque-Bera 正态性检验、双样本均值检验（Welch + 置换检验）、OLS 回归、VIF、多重诊断、自助法置信区间。
- 新增 `/applied-stats/*` API：支持通用数值数据集分析，以及“市场数据作为应用统计案例”的一键研究。
- 新增 CLI 子命令 `applied-stats-study`：可直接生成结构化统计报告（可导出 Markdown 到 `reports/`）。
- 研究输出强调统计学流程：数据清洗 -> 假设检验 -> 建模估计 -> 诊断与解释，而不是交易执行工程。

## 已实现能力范围

1. 数据层（多数据源回退）
- `tushare` 优先，`akshare` 回退（当 `tushare` token 缺失或接口失败时自动降级）。
- 统一日线数据结构。
- 交易日历接口。
- 财报快照接口（按标的 + as_of 获取最近可用财务指标）。
- 高级数据能力目录（按积分可用性）与批量预取接口（支持逐数据集成功/失败/跳过状态）。

2. 因子引擎
- 趋势、动量、波动率、ATR、z-score、流动性等因子。
- 基本面因子（ROE、营收同比、净利同比、毛利率、负债率、现金流质量）。
- 基本面综合评分（`fundamental_score`）与完整度评分（`fundamental_completeness`）。
- `tushare` 高级字段因子：估值/资金流/可交易性子分（`tushare_valuation_score`, `tushare_moneyflow_score`, `tushare_tradability_score`）。
- 高级综合分：`tushare_advanced_score`（含可用度 `tushare_advanced_completeness`）。

3. 策略引擎
- `trend_following`
- `mean_reversion`
- `multi_factor`
- `sector_rotation`
- `event_driven`
- 策略注册与策略元数据接口。

4. 风险引擎
- T+1 卖出约束。
- ST 过滤。
- 停牌过滤。
- 涨跌停执行风险提示。
- 单标的仓位上限。
- 流动性阈值。
- 组合回撤阈值。
- 行业集中度预警。
- 基本面质量门槛（低评分告警/阻断，PIT 时点违规阻断）。
- 小资金可交易过滤（最小手数可买性、成本覆盖边际检查）。

5. 信号中心
- 支持策略选择与参数配置的信号生成。
- 输出带风险解释与合规免责声明的交易准备单。

6. 回测引擎
- 单标的 long-only 基线回测。
- 滑点 + 手续费建模。
- A股费用细化（最低佣金、卖出印花税、过户费）。
- 拟真执行成本模型（分档滑点、冲击成本、停牌/涨跌停/一字板成交概率、部分成交）。
- T+1 可卖数量逻辑。
- 权益曲线、成交记录与关键指标。
- 多标的组合级净值回测（调仓周期、仓位上限、行业/主题约束、资金利用率）。

7. 审计链路
- 基于 SQLite 的审计事件存储。
- API 访问、信号生成、回测、风控检查均可审计。

8. 批处理 Pipeline
- 标的列表日批运行。
- 汇总阻断/预警统计。
- 输出每个标的的财报可用性、评分与来源信息。

9. 数据治理
- 数据质量报告（缺失字段、重复、OHLC 非法值检查）。
- 面向 PIT 的数据快照元数据（hash + provider + date range）。

10. 组合工作流
- 在标的/行业约束下做候选组合优化。
- 从当前持仓到目标权重的调仓计划生成。

11. 回放工作流
- 信号记录持久化。
- 人工执行回写。
- 回放报表：跟随率、延迟、执行追踪。
- 偏差归因报表：`NO_EXECUTION` / `ACTION_MISMATCH` / `EXECUTION_DELAY` / `HIGH_SLIPPAGE`。
- 研究到执行闭环报表（`closure`）：原因统计 + 参数修正建议。

12. 研究工作流编排
- 多标的批量信号生成。
- 同请求可选组合优化。

13. 告警中心
- 按严重级别与事件类型订阅。
- 去重窗口与频率控制。
- 通知收件箱与 ACK 流程。
- 支持真实通道派发（`email` / `im` / `dingtalk` / `wecom` / `pagerduty` / `oncall` 升级链路）。
- 投递审计日志 API。
- 值班回调闭环（`callback -> notification ACK -> callback history`）。
- 回调签名校验、供应商映射模板与对账流程。

14. 策略治理
- 策略版本草稿注册。
- 评审提交与多角色决策流程。
- 审批流程与已批准版本查询。
- 可选运行时强制（`ENFORCE_APPROVED_STRATEGY=true`）。

15. RBAC 访问控制
- 可选 API Key 鉴权与角色权限。
- 角色：`research`, `risk`, `portfolio`, `audit`, `readonly`, `admin`。

16. PIT 护栏
- 时间点一致性校验接口。
- 在信号/回测/pipeline 运行前做运行时校验。
- 事件时间戳 PIT 校验接口。

17. 报告中心
- 信号/风控/回放/closure 报告生成。
- 支持带水印 Markdown 导出及可选文件落盘。

18. 数据许可证治理
- 许可证台账（数据集/供应商/范围/到期/导出策略/水印）。
- 许可证校验 API 与可选运行时强制（`ENFORCE_DATA_LICENSE=true`）。
- market/signal/backtest/pipeline/research/report/audit 路径均接入许可证校验与审计元数据。

19. 运维作业中心
- 作业定义注册（`pipeline_daily` / `research_workflow` / `report_generate` / `event_connector_sync` / `event_connector_replay` / `compliance_evidence_export` / `alert_oncall_reconcile` / `auto_tune` / `execution_review`）。
- 手工触发、运行历史、结构化运行摘要。
- register/trigger 全链路审计。

20. 定时运行与运维看板
- 基于 cron 的定时调度 tick（同分钟去重）。
- SLA 检查：无效 cron、漏跑、最近运行失败、运行超时。
- 统一运维看板：作业健康、告警积压、执行偏差、事件治理统计。
- 可选后台 Worker（`OPS_SCHEDULER_ENABLED=true`）自动 tick。

21. 事件治理、连接器与 PIT 联接校验
- 事件源注册与入库元数据管理。
- 批量事件入库（upsert）+ 来源级审计。
- PIT 联接校验（`publish_time` / `effective_time`）。
- 事件特征增强（`event_score`, `negative_event_score`）。
- 真实公告连接器框架（`AKSHARE_ANNOUNCEMENT` / `TUSHARE_ANNOUNCEMENT` / `HTTP_JSON_ANNOUNCEMENT` / `FILE_ANNOUNCEMENT`）。
- 连接器增量检查点。
- 失败队列：重放/退避/死信流程。
- 连接器 SLA 状态机持久化（去重 + 冷却 + 恢复）。
- 自动升级策略（warning/critical 重复阈值 + 升级审计事件）。
- SLA 告警载荷包含 runbook URL。
- 批量失败修复 + 重放。
- 多源矩阵健康评分与自动切换。
- 来源预算治理 + 凭证别名轮换。
- 来源健康状态 API。
- 事件标准化 + NLP 打分管线。
- NLP 规则集版本治理 + 人工反馈标注闭环。
- 多标注仲裁、标注一致性 QA、标签快照。
- 漂移检查（命中率/分值/贡献变化 + 反馈质量漂移）。
- 漂移监控汇总 API。
- 连接器 + NLP 漂移 SLO burn-rate 历史 API。
- 事件特征回测对比报告（baseline vs event-enriched）。

22. 前端运维看板
- 页面入口：`GET /ops/dashboard`。
- 静态资源：`/ui/ops-dashboard/*`。
- 单页可视化：jobs/SLA/alerts/connectors/event coverage/NLP drift。
- 内置回放工作台：手工修复、选中重放、修复后重放。
- 包含来源矩阵健康面板（active source、健康分、切换状态）。
- 页头可跳主界面（`/ui/`）与投研工作台（`/trading/workbench`）。

23. 前端投研交易工作台
- 页面入口：`GET /trading/workbench`。
- 静态资源：`/ui/trading-workbench/*`。
- 四段工作流：策略参数、自动调参、结果可视化、执行回写。
- 集成 `/strategies`, `/signals/generate`, `/backtest/run`, `/research/run`, `/replay/*`。
- 集成 `/autotune/*` 完整闭环（运行、候选榜、基线对比、画像激活/回滚、灰度规则）。
- 增加 K 线/价格趋势叠加信号（`/market/bars`）。
- 增加组合权重 + 行业暴露可视化与调仓联动（`/portfolio/rebalance/plan`）。
- 增加组合净值回测展示（`/backtest/portfolio-run`）。
- 增加偏差归因与 closure 报表展示（`/replay/attribution`, `/reports/generate`）。
- 增加“财报评分”列（信号表 + 研究候选表），展示每个标的当前基本面评分。
- 新增“小资金模式”参数区（本金、安全边际、费用参数）与结果提示列。
- 页头可跳主界面（`/ui/`）与运维看板（`/ops/dashboard`）。

24. 前端主界面
- 页面入口：`GET /ui/`。
- 统一导航到投研与运维页面。
- 提供流程化引导与快速状态摘要。
- 新增应用统计展示页入口：`GET /applied-stats/showcase`（复试演示专用）。

25. 合规证据包导出
- 一键导出证据包 zip + SHA256 校验。
- 包含审计哈希链校验、策略治理快照、事件治理快照。
- 支持签名与验证。
- 支持双人复签与复签校验。
- 支持 immutable-vault 拷贝与定时自动导出。
- 支持外部 WORM/KMS 策略集成（端点归档 + key wrap 回执）。

26. 部署清单
- 单机 Docker 部署（`deploy/docker-compose.single-node.yml`）。
- 私有云 Kubernetes 基线（`deploy/k8s/private-cloud/trading-assistant.yaml`）。

27. 自动调参闭环
- 自动参数搜索（默认策略模板 + 自定义搜索空间）。
- 训练/验证分段回测评分（收益/回撤/夏普/成交活跃度综合目标函数）。
- walk-forward 多窗口稳定性评估（`walk_forward_slices`）。
- 过拟合抑制项：训练-验证 gap 惩罚、收益稳定性方差惩罚、参数漂移惩罚。
- 产出候选排行榜与最优参数，支持阈值门槛自动生效。
- 自动参数画像（global 或 symbol 作用域）持久化。
- 画像操作支持：一键激活、回滚到上一版本、按策略/按标的灰度启停规则。
- 运行时自动覆盖：`signals/backtest/research/pipeline` 可自动读取活动画像，再由请求参数做最终覆盖（显式参数优先）。
- 可选自动创建策略治理草稿（参数哈希 + 版本号），衔接审批流程。

28. 数据层增量缓存
- 本地时序缓存（SQLite）+ 增量补拉，避免重复拉取行情数据。
- 支持缓存覆盖查询与缺口补齐，提升批量回测和调参吞吐。
- 数据质量报告新增字段级评分与总体评分。

29. 合规证据包增强
- 证据包新增自动调参实验与生效记录（审计事件、画像快照、灰度规则）。
- 支持后续审计追踪“实验 -> 生效 -> 回滚”全链路证据。

30. 应用统计研究模块
- 通用数值数据描述统计与相关性分析。
- 双样本均值差异检验（参数法 + 置换检验）。
- 多元线性回归与诊断（系数显著性、VIF、残差正态性、DW）。
- 市场数据应用统计案例报告（可导出 Markdown）。

## 快速开始

```bash
python -m venv .venv
. .venv/Scripts/activate
pip install -e .[dev]
copy .env.example .env
uvicorn trading_assistant.main:app --reload
```

PowerShell 快捷方式：

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\bootstrap.ps1
powershell -ExecutionPolicy Bypass -File .\scripts\run_api.ps1
powershell -ExecutionPolicy Bypass -File .\scripts\run_pipeline.ps1 -Symbols "000001,000002"
```

应用统计案例（CLI）：

```powershell
python -m trading_assistant.cli applied-stats-study --symbol 000001 --start-date 2025-01-01 --end-date 2025-06-30 --export-markdown
```

Windows 一键启动：

```bat
start_system_windows.bat
```
中文使用文档：
- `docs/system_user_guide_zh.md`
- `docs/system_quick_start_10min_zh.md`（10 分钟快速上手）
- `docs/system_pretrade_checklist_zh.md`（开盘前/盘中/收盘后检查清单）
- `docs/tushare_2120_capability_map_zh.md`（2120 积分数据能力映射与接入说明）
- `docs/runbooks/autotune_experiment_rollout.md`（自动调参与灰度生效 runbook）
- `docs/applied_statistics_research_guide_zh.md`（应用统计研究模块说明）
- `docs/applied_statistics_retest_pitch_zh.md`（应用统计复试展示话术）

一键启动行为：
- 自动切换到项目根目录（`%~dp0`）
- 缺失时自动创建 `.venv` / `.env`
- 检测导入失败时自动安装依赖
- 新开终端启动 API，并设置 `OPS_SCHEDULER_ENABLED=true`
- 自动打开：
  - `http://127.0.0.1:8000/ui/`
  - `http://127.0.0.1:8000/trading/workbench`
  - `http://127.0.0.1:8000/ops/dashboard`

可选参数：
- `start_system_windows.bat --dry-run`（只打印动作，不执行）
- `start_system_windows.bat --no-browser`（只启动后端，不打开浏览器）

## 测试（Windows）

为避免系统 `%TEMP%` 权限问题，建议使用测试包装脚本：

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\run_tests.ps1
```

运行指定测试：

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\run_tests.ps1 tests\test_event_connector_service.py tests\test_event_nlp_governance.py
```

说明：
- 测试临时目录重定向到 `%LOCALAPPDATA%\Temp\codex-pytest-temp`
- 每次测试后自动清理 pytest 缓存/字节码/临时目录
- 包装脚本中禁用 pytest cache plugin（`-p no:cacheprovider`）
- pytest case 临时目录通过 `--basetemp=%LOCALAPPDATA%\Temp\codex-pytest-cases` 重定向
- 项目 fixture 的 case 目录也写入 `%LOCALAPPDATA%\Temp\codex-pytest-cases`（会在会话结束及包装脚本中清理）
- 若历史缓存目录 ACL 异常，可执行：
  `powershell -ExecutionPolicy Bypass -File .\scripts\fix_test_cache_acl.ps1 -IncludeSourceBytecode`

## 部署

单机：
```bash
cd deploy
docker compose -f docker-compose.single-node.yml up -d --build
```

私有云：
```bash
kubectl apply -f deploy/k8s/private-cloud/trading-assistant.yaml
```

前端页面入口：
```text
http://127.0.0.1:8000/ui/
http://127.0.0.1:8000/ops/dashboard
http://127.0.0.1:8000/trading/workbench
http://127.0.0.1:8000/applied-stats/showcase
```

## 可选鉴权（RBAC）

当 `AUTH_ENABLED=true` 时，请在请求头中带上 API Key：

```text
X-API-Key: <your-key>
```

在 `.env` 中配置 key-role 映射：

```text
AUTH_API_KEYS=research_key:research,risk_key:risk,audit_key:audit,admin_key:admin
```

## API 总览

- `GET /health`
- `GET /market/bars`
- `GET /market/calendar`
- `GET /market/tushare/capabilities`
- `POST /market/tushare/prefetch`
- `POST /applied-stats/descriptive`
- `POST /applied-stats/tests/two-sample-mean`
- `POST /applied-stats/model/ols`
- `POST /applied-stats/cases/market-factor-study`
- `POST /data/quality/report`
- `POST /data/pit/validate`
- `POST /data/pit/validate-events`
- `POST /data/snapshots/register`
- `GET /data/snapshots`
- `GET /data/snapshots/latest`
- `POST /data/licenses/register`
- `GET /data/licenses`
- `POST /data/licenses/check`
- `POST /events/sources/register`
- `GET /events/sources`
- `POST /events/ingest`
- `GET /events`
- `POST /events/pit/join-validate`
- `POST /events/features/preview`
- `POST /events/connectors/register`
- `GET /events/connectors`
- `GET /events/connectors/overview`
- `GET /events/connectors/source-health`
- `POST /events/connectors/run`
- `GET /events/connectors/runs`
- `GET /events/connectors/failures`
- `POST /events/connectors/failures/repair`
- `POST /events/connectors/replay`
- `POST /events/connectors/replay/manual`
- `POST /events/connectors/replay/repair`
- `GET /events/connectors/sla`
- `POST /events/connectors/sla/sync-alerts`
- `GET /events/connectors/sla/states`
- `GET /events/connectors/sla/states/summary`
- `GET /events/connectors/slo/history`
- `GET /events/ops/coverage`
- `POST /events/nlp/normalize/preview`
- `POST /events/nlp/normalize/ingest`
- `POST /events/nlp/rulesets`
- `POST /events/nlp/rulesets/activate`
- `GET /events/nlp/rulesets`
- `GET /events/nlp/rulesets/active`
- `POST /events/nlp/drift-check`
- `GET /events/nlp/drift/snapshots`
- `GET /events/nlp/drift/monitor`
- `GET /events/nlp/drift/slo/history`
- `POST /events/nlp/feedback`
- `GET /events/nlp/feedback`
- `GET /events/nlp/feedback/summary`
- `POST /events/nlp/labels`
- `GET /events/nlp/labels`
- `POST /events/nlp/labels/adjudicate`
- `GET /events/nlp/labels/consensus`
- `GET /events/nlp/labels/consistency`
- `POST /events/nlp/labels/snapshots`
- `GET /events/nlp/labels/snapshots`
- `POST /events/features/backtest-compare`
- `GET /factors/snapshot`
- `GET /strategies`
- `GET /strategies/{strategy_name}`
- `POST /strategy-governance/register`
- `POST /strategy-governance/submit-review`
- `POST /strategy-governance/approve`
- `POST /strategy-governance/decide`
- `GET /strategy-governance/versions`
- `GET /strategy-governance/latest-approved`
- `GET /strategy-governance/decisions`
- `GET /strategy-governance/policy`
- `POST /autotune/run`
- `GET /autotune/profiles`
- `GET /autotune/profiles/active`
- `POST /autotune/profiles/{profile_id}/activate`
- `POST /autotune/profiles/rollback`
- `POST /autotune/rollout/rules/upsert`
- `GET /autotune/rollout/rules`
- `DELETE /autotune/rollout/rules/{rule_id}`
- `POST /signals/generate`
- `POST /risk/check`
- `POST /portfolio/risk/check`
- `POST /portfolio/optimize`
- `POST /portfolio/rebalance/plan`
- `POST /portfolio/stress-test`
- `POST /backtest/run`
- `POST /backtest/portfolio-run`
- `POST /pipeline/daily-run`
- `POST /replay/signals/record`
- `GET /replay/signals`
- `POST /replay/executions/record`
- `GET /replay/report`
- `GET /replay/attribution`
- `POST /research/run`
- `GET /audit/events`
- `GET /audit/export`
- `GET /audit/verify-chain`
- `GET /alerts/recent`
- `POST /alerts/subscriptions`
- `GET /alerts/subscriptions`
- `GET /alerts/notifications`
- `GET /alerts/deliveries`
- `POST /alerts/notifications/{notification_id}/ack`
- `POST /alerts/oncall/callback`
- `GET /alerts/oncall/events`
- `POST /alerts/oncall/reconcile`
- `GET /metrics/summary`
- `GET /metrics/ops-dashboard`
- `POST /model-risk/drift-check`
- `POST /reports/generate`
- `POST /compliance/preflight`
- `POST /compliance/evidence/export`
- `POST /compliance/evidence/verify`
- `POST /compliance/evidence/countersign`
- `POST /ops/jobs/register`
- `GET /ops/jobs`
- `POST /ops/jobs/{job_id}/run`
- `GET /ops/jobs/{job_id}/runs`
- `GET /ops/jobs/runs/{run_id}`
- `POST /ops/jobs/scheduler/tick`
- `GET /ops/jobs/scheduler/sla`
- `GET /ops/dashboard`
- `GET /system/config`
- `GET /system/auth/me`
- `GET /system/auth/permissions`

## 调度配置

```text
OPS_SCHEDULER_ENABLED=false
OPS_SCHEDULER_TICK_SECONDS=30
OPS_SCHEDULER_TIMEZONE=Asia/Shanghai
OPS_SCHEDULER_SLA_LOG_COOLDOWN_SECONDS=1800
OPS_SCHEDULER_SYNC_ALERTS_FROM_AUDIT=true
OPS_JOB_SLA_GRACE_MINUTES=15
OPS_JOB_RUNNING_TIMEOUT_MINUTES=120
COMPLIANCE_EVIDENCE_SIGNING_SECRET=
COMPLIANCE_EVIDENCE_VAULT_DIR=reports/compliance_vault
COMPLIANCE_EVIDENCE_EXTERNAL_WORM_ENDPOINT=
COMPLIANCE_EVIDENCE_EXTERNAL_KMS_WRAP_ENDPOINT=
COMPLIANCE_EVIDENCE_EXTERNAL_AUTH_TOKEN=
COMPLIANCE_EVIDENCE_EXTERNAL_TIMEOUT_SECONDS=10
COMPLIANCE_EVIDENCE_EXTERNAL_REQUIRE_SUCCESS=false
EVENT_DB_PATH=data/event.db
```

## 财报因子配置

```text
ENABLE_FUNDAMENTAL_ENRICHMENT=true
FUNDAMENTAL_MAX_STALENESS_DAYS=540
FUNDAMENTAL_BUY_WARNING_SCORE=0.50
FUNDAMENTAL_BUY_CRITICAL_SCORE=0.35
FUNDAMENTAL_REQUIRE_DATA_FOR_BUY=false
```

说明：
- `ENABLE_FUNDAMENTAL_ENRICHMENT`：全局开关。开启后，`signals/backtest/pipeline/research`默认会注入财报快照并计算基本面评分。
- `FUNDAMENTAL_MAX_STALENESS_DAYS`：财报陈旧度阈值，超过后会在评分中施加衰减。
- `FUNDAMENTAL_BUY_WARNING_SCORE` / `FUNDAMENTAL_BUY_CRITICAL_SCORE`：买入信号的财报质量分级门槛。
- `FUNDAMENTAL_REQUIRE_DATA_FOR_BUY`：若开启且取不到财报快照，买入信号会进入人工确认路径（WARNING）。

## 市场数据缓存配置

```text
MARKET_DATA_CACHE_ENABLED=true
MARKET_DATA_CACHE_DB_PATH=data/market_cache.db
```

说明：
- `MARKET_DATA_CACHE_ENABLED=true` 时，数据层会先命中本地缓存，再按缺失日期区间增量补拉。
- 对同一 `symbol + date range` 的回测/调参可显著减少重复外部请求。

## 小资金模式与费用模型配置

```text
SMALL_CAPITAL_MODE_ENABLED=false
SMALL_CAPITAL_PRINCIPAL_CNY=2000
SMALL_CAPITAL_CASH_BUFFER_RATIO=0.10
SMALL_CAPITAL_MIN_EXPECTED_EDGE_BPS=80
SMALL_CAPITAL_LOT_SIZE=100
DEFAULT_COMMISSION_RATE=0.0003
DEFAULT_SLIPPAGE_RATE=0.0005
FEE_MIN_COMMISSION_CNY=5
FEE_STAMP_DUTY_SELL_RATE=0.0005
FEE_TRANSFER_RATE=0.00001
AUTOTUNE_RUNTIME_OVERRIDE_ENABLED=true
AUTOTUNE_DB_PATH=data/autotune.db
```

说明：
- 小资金模式开启后，`signals/pipeline/research/backtest`会对 BUY 信号增加“可买一手 + 成本覆盖边际”过滤。
- 即使使用其他策略，小资金模式也会在 BUY 信号前执行“仓位覆写”：买不起一手或一手集中度过高时自动降级为 `WATCH`。
- 前端策略页新增三档一键模板：`2000 / 5000 / 8000`，自动填充小资金策略与关键参数。
- 回测中费用模型会计入最低佣金、卖出印花税、过户费，避免小本金回测被过度乐观高估。
- 前端“策略与参数页”可按请求覆盖全局设置（`enable_small_capital_mode` / `small_capital_principal` / `small_capital_min_expected_edge_bps`）。
- `AUTOTUNE_RUNTIME_OVERRIDE_ENABLED=true` 时，系统会自动读取当前策略的活动调参画像；请求里显式传入的 `strategy_params` 优先级更高，会覆盖自动画像的同名参数。

## 告警派发配置

```text
ALERT_EMAIL_ENABLED=false
ALERT_SMTP_HOST=
ALERT_SMTP_PORT=465
ALERT_SMTP_USERNAME=
ALERT_SMTP_PASSWORD=
ALERT_SMTP_USE_TLS=false
ALERT_SMTP_USE_SSL=true
ALERT_EMAIL_FROM=
ALERT_IM_ENABLED=false
ALERT_IM_DEFAULT_WEBHOOK=
ALERT_NOTIFY_TIMEOUT_SECONDS=10
ALERT_RUNBOOK_BASE_URL=
ONCALL_CALLBACK_SIGNING_SECRET=
ONCALL_CALLBACK_REQUIRE_SIGNATURE=false
ONCALL_CALLBACK_SIGNATURE_TTL_SECONDS=600
ONCALL_CALLBACK_MAPPING_JSON=
ONCALL_RECONCILE_DEFAULT_ENDPOINT=
ONCALL_RECONCILE_TIMEOUT_SECONDS=10
```

## 调用示例

```bash
curl "http://127.0.0.1:8000/market/bars?symbol=000001&start_date=2025-01-01&end_date=2025-12-31&limit=5"
```

```bash
curl "http://127.0.0.1:8000/strategies"
```

```bash
curl -X POST "http://127.0.0.1:8000/signals/generate" ^
  -H "Content-Type: application/json" ^
  -d "{\"symbol\":\"000001\",\"start_date\":\"2025-01-01\",\"end_date\":\"2025-12-31\",\"strategy_name\":\"multi_factor\",\"enable_fundamental_enrichment\":true,\"fundamental_max_staleness_days\":540}"
```

```bash
curl -X POST "http://127.0.0.1:8000/backtest/run" ^
  -H "Content-Type: application/json" ^
  -d "{\"symbol\":\"000001\",\"start_date\":\"2025-01-01\",\"end_date\":\"2025-12-31\",\"strategy_name\":\"trend_following\",\"enable_fundamental_enrichment\":true}"
```

## 目录结构

```text
src/trading_assistant/
  alerts/     # 告警订阅 + 通知收件箱
  api/        # FastAPI 路由
  audit/      # 审计存储/服务（SQLite）
  backtest/   # 回测引擎
  core/       # 配置、模型、依赖注入容器
  data/       # 数据源：akshare/tushare + 回退路由
  factors/    # 因子引擎
  governance/ # 快照/质量/license + 事件连接器/NLP/重放治理
  ops/        # 作业编排 + 调度 + 运维看板服务
  pipeline/   # 批处理运行器
  portfolio/  # 优化器 + 调仓规划
  replay/     # 信号-执行回放存储/报表
  risk/       # 风控规则与评估
  signal/     # 交易准备单生成
  strategy/   # 策略模板与注册中心
  web/        # 前端静态资源
  main.py     # 应用入口
tests/
docs/
```

## 说明

- 本仓库是基础平台，不是完整投资顾问产品。
- 用于商业场景时，仍需完成合规流程与法律评估。
- 模块级设计见 `docs/architecture.md`。
- 部署手册见 `docs/deployment.md`。
- 版本记录见 `CHANGELOG.md`。
- API 请求示例见 `docs/examples/`。
- 配置加载器支持在缺少 `pydantic-settings` 时从 `.env` 回退，但仍建议安装完整依赖。
