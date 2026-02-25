# 更新日志

## 0.8.7

- 自动调参与过拟合防护升级：
  - walk-forward 多窗口验证 + 稳定性惩罚。
  - 新增参数漂移惩罚（objective_weight_param_drift）。
  - 自动调参请求接入拟真执行模型参数（冲击成本、成交概率底线）。
- 参数画像与灰度发布能力增强：
  - 新增回滚接口：POST /autotune/profiles/rollback。
  - 新增灰度规则接口：
    - POST /autotune/rollout/rules/upsert
    - GET /autotune/rollout/rules
    - DELETE /autotune/rollout/rules/{rule_id}
  - 运行时参数解析支持按策略/按标的灰度启停画像。
- 回测引擎升级：
  - 单标的回测接入拟真成本模型（冲击成本、分档滑点、涨跌停/停牌成交概率、部分成交）。
  - 新增多标的组合净值回测接口：POST /backtest/portfolio-run。
- 风控升级：
  - 组合风控新增连续亏损熔断、单日最大亏损、VaR/ES、主题集中度约束。
- 研究到执行闭环：
  - 新增执行偏差归因接口：GET /replay/attribution。
  - 报告中心新增 closure 固定报表。
  - 新增运维作业类型：execution_review。
- 数据层与性能：
  - 新增本地时序缓存与增量补拉。
  - 数据质量新增字段级评分与总体评分。
  - 因子引擎减少 DataFrame fragmented 风险（批量赋值）。
- 前端工作台升级：
  - 新增“自动调参”独立页签，支持任务运行、候选榜、基线对比、画像回滚、灰度规则管理。
  - 新增组合净值回测结果展示。
  - 新增偏差归因与 closure 报表展示。
- 证据包增强：
  - 合规证据包新增自动调参实验与生效记录导出。
- 运维文档：
  - 新增 runbook：docs/runbooks/autotune_experiment_rollout.md。

## 0.8.6

- 新增自动调参闭环：
  - 新增 `autotune` 服务与参数画像存储（`global/symbol` 两级作用域）。
  - 支持策略参数网格搜索（自定义 `search_space` + 内置默认模板）。
  - 支持训练/验证分段回测评分（收益、年化、夏普、回撤、阻断比例、成交活跃度综合目标函数）。
  - 输出候选排行榜、最佳参数、相对基线提升、应用结果。
- 新增自动调参 API：
  - `POST /autotune/run`
  - `GET /autotune/profiles`
  - `GET /autotune/profiles/active`
  - `POST /autotune/profiles/{profile_id}/activate`
- 新增运行时参数自动覆盖能力：
  - `signals/backtest/research/pipeline` 支持自动读取活动调参画像。
  - 合并规则：自动画像先加载，请求显式 `strategy_params` 后覆盖（显式优先）。
  - 新增请求级开关：`use_autotune_profile`（默认 `true`）。
- 新增运维作业类型：
  - `auto_tune`（可纳入定时任务体系运行自动调参）。
- 新增配置：
  - `AUTOTUNE_RUNTIME_OVERRIDE_ENABLED`
  - `AUTOTUNE_DB_PATH`
- 新增测试：
  - `test_autotune_service.py`
  - `test_job_service.py` 增加 `auto_tune` 作业执行覆盖。

## 0.8.5

- 数据源优先级调整为 `tushare -> akshare`（配置默认值与 `.env.example` 同步更新）。
- `tushare` 高级数据能力升级：
  - 新增高级数据能力目录（按积分可用性 + API 可用性 + 系统接入目标）。
  - 新增批量预取能力（逐数据集返回 success/failed/skipped、参数、行列数）。
  - 日线自动融合高级字段（`daily_basic` / `moneyflow` / `stk_limit` / `adj_factor`）。
- 新增市场 API：
  - `GET /market/tushare/capabilities`
  - `POST /market/tushare/prefetch`
- 因子与策略升级：
  - 因子引擎新增 `tushare_valuation_score`、`tushare_moneyflow_score`、`tushare_tradability_score`、`tushare_advanced_score`。
- 新增测试：
  - `test_tushare_provider_advanced.py`
  - `test_market_tushare_endpoints.py`
  - `test_strategy_multi_factor_tushare.py`
  - 既有 `factor/small_capital` 测试补充 `tushare` 高级分值断言。
## 0.8.4

  - 目标场景：本金长期低于 1 万元
  - 低换手、可执行优先：一手可买性、集中度、波动、流动性联合约束
  - 动态仓位：按本金、最小手数、可用现金缓冲、波动风险自适应计算 `suggested_position`
- 新增“小资金信号覆写器”并接入全链路：
  - `signals` / `backtest` / `pipeline` / `research` 在 BUY 前统一执行“小资金可执行修正”
  - 买不起一手或一手集中度过高时，自动从 `BUY` 降级为 `WATCH`
  - 可买时自动把仓位抬升到“至少一手可执行”并限制在风险上限内
- 策略上下文增强：
  - `StrategyContext.market_state` 传入小资金与费用参数（本金、手数、佣金、过户费、滑点等）
  - 支持策略在生成阶段做账户规模感知
- 前端工作台增强：
  - 小资金提示区在本金 `<=10000` 时给出策略切换建议
  - 新增三档一键模板按钮（`2000/5000/8000`）自动填充小资金参数
- 新增测试：
  - `test_strategy_registry.py` 更新策略覆盖断言

## 0.8.3

- 新增“小资金模式”：
  - 请求级开关：`enable_small_capital_mode`
  - 请求级本金：`small_capital_principal`
  - 请求级边际阈值：`small_capital_min_expected_edge_bps`
  - 全局配置：`SMALL_CAPITAL_*`
- 新增可交易过滤规则：
  - BUY 前检查“是否可买一手（含费用）”
  - 检查“预期边际是否覆盖成本+安全边”
  - 风控规则名：`small_capital_tradability`
- 升级费用模型：
  - 新增最低佣金、卖出印花税、过户费参数
  - 回测执行与成本核算接入细化费用模型
- 前端工作台增强：
  - 新增小资金模式参数区与提示文案
  - 新增费用参数输入（最低佣金、印花税、过户费）
  - 信号表/研究表新增“小资金提示”列
- 新增测试：
  - `test_trading_costs.py`
  - 风控/流水线/研究流程的小资金约束测试

## 0.8.2

- 新增“财报因子接入主链路”：
  - `akshare` / `tushare` 提供器增加财报快照读取能力（按 `symbol + as_of`）。
  - 新增 `fundamentals/service.py`，统一做财报快照注入、PIT 可用性判断与陈旧度标记。
  - `signals/backtest/pipeline/research` 全链路支持财报增强开关与陈旧度阈值。
- 升级因子引擎：
  - 新增财报字段标准化评分：盈利能力、成长性、质量、杠杆四个子评分。
  - 输出 `fundamental_score` 与 `fundamental_completeness`，并对陈旧数据与 PIT 违规做惩罚。
- 升级策略与风控：
  - `trend/mean_reversion/multi_factor/sector_rotation/event_driven` 在 BUY 侧都纳入财报评分影响。
  - 风控新增 `fundamental_quality` 规则：低评分 warning/critical，PIT 违规直接阻断。
  - 研究工作流优化候选收益时加入“技术动量 + 财报评分”混合估计。
- 前端与可视化：
  - 投研工作台“信号表”“研究候选表”新增“财报评分”列，便于人工复核。
- 新增/更新测试：
  - `test_fundamental_service.py`
  - `test_factor_engine_fundamental.py`
  - `test_strategy_fundamental_overlay.py`
  - provider/risk/pipeline/research 相关回归测试同步更新。

## 0.8.1

- 将事件连接器治理升级到生产级 SLA 自动化：
  - 新鲜度 / 积压 / 死信阈值策略，支持按连接器覆盖（`config.sla`）
  - 连接器 SLA 报告 API（`GET /events/connectors/sla`）
  - 周期性 SLA 审计告警发送 + 冷却去重（`POST /events/connectors/sla/sync-alerts`）
  - 调度 Worker 每次 tick 自动执行连接器 SLA 同步
- 新增回放工作台后端流程：
  - 失败 payload 修复 API（`POST /events/connectors/failures/repair`）
  - 按失败 ID 手动重放 API（`POST /events/connectors/replay/manual`）
  - 逐条重放结果与运行链路持久化
- 新增 NLP 规则集版本治理：
  - 规则集存储 / 激活 / 查询 API（`/events/nlp/rulesets*`）
  - 活跃规则集热加载到事件标准化器
  - 在事件元数据中持久化 `nlp_ruleset_version` 追踪
- 新增 NLP 在线漂移监控：
  - 命中率 / 分值分布 / 极性混合监控
  - 基于事件特征回测对比的贡献变化跟踪
  - 漂移快照持久化与查询（`POST /events/nlp/drift-check`, `GET /events/nlp/drift/snapshots`）
- 扩展运维前端看板：
  - 连接器 SLA 违约表（支持 warning / critical / escalated 可视化）
  - 回放工作台 UI（人工修复 + 选中重放）
  - 暴露连接器新鲜度和 SLA 严重告警 KPI
- 新增测试覆盖：
  - 连接器 SLA 告警同步 + 失败修复 / 重放流程
  - NLP 规则集版本治理 + 漂移快照流程

## 0.8.0

- 新增生产级事件连接器框架：
  - 连接器注册中心与类型化源适配器（`TUSHARE_ANNOUNCEMENT` / `FILE_ANNOUNCEMENT`）
  - 增量同步检查点状态
  - 连接器运行历史与单次运行指标
  - 失败入库队列：重放 / 退避重试 / 死信状态
- 新增事件标准化 + NLP 打分管线：
  - 原始公告标准化预览 API
  - 基于规则的事件类型识别、情绪识别、分值与置信度生成
  - 标准化结果入库 API（自动生成事件因子）
- 新增事件特征回测对比报告：
  - 基线（无事件特征） vs 增强（启用事件特征）
  - 指标差值汇总 + 事件覆盖诊断
  - 可选 Markdown 报告落盘
- 新增前端运维看板：
  - `/ops/dashboard` 页面与 `/ui/ops-dashboard/*` 静态资源
  - 可视化作业 / SLA / 告警 / 连接器状态 / 事件覆盖
- 扩展运维看板后端：
  - 在 `/metrics/ops-dashboard` 增加事件治理统计
- 扩展运维作业类型：
  - `event_connector_sync`
  - `event_connector_replay`
- 新增测试：
  - `test_event_connector_service.py`
  - `test_event_feature_compare.py`
  - 作业/运维看板对连接器与事件统计的更新

## 0.7.0

- 新增事件治理层：
  - 事件源注册（元数据 + 可靠性画像）
  - 批量事件入库（按 source upsert）
  - 事件查询 API（支持来源 / 标的 / 时间过滤）
- 新增事件 PIT 联接校验：
  - `/events/pit/join-validate`
  - 检查事件缺失、来源歧义、标的不匹配、发布时间/生效时间违规
- 新增事件特征增强：
  - 事件因子生成（`event_score`, `negative_event_score`）
  - 已接入 signal/backtest/pipeline/research 主路径
  - `event_driven` 策略自动启用，其他策略可选启用
- 新增 API：
  - `/events/sources/register`
  - `/events/sources`
  - `/events/ingest`
  - `/events`
  - `/events/features/preview`
- 新增部署资产：
  - 单机 Docker Compose 清单
  - 私有云 Kubernetes 基线清单
  - 部署手册（`docs/deployment.md`）
- 新增配置：
  - `EVENT_DB_PATH`
- 新增测试：
  - `test_event_service.py`
  - `test_event_enrichment_workflow.py`

## 0.6.0

- 新增运维调度运行时：
  - cron 解析器（`ops/cron.py`），支持 list/range/step
  - 调度 tick API（`POST /ops/jobs/scheduler/tick`）
  - 环境变量控制的可选后台 worker
  - 同分钟去重，避免重复触发
- 新增定时作业 SLA 监控：
  - 无效 cron、漏跑、最近运行失败、运行超时检查
  - SLA 报告 API（`GET /ops/jobs/scheduler/sla`）
  - 调度与 SLA 事件审计日志
- 新增运维看板聚合：
  - 指标 API（`GET /metrics/ops-dashboard`）
  - 汇总作业健康、告警积压、执行偏差与 SLA 状态
- 新增配置项：
  - `OPS_SCHEDULER_ENABLED`
  - `OPS_SCHEDULER_TICK_SECONDS`
  - `OPS_SCHEDULER_TIMEZONE`
  - `OPS_SCHEDULER_SLA_LOG_COOLDOWN_SECONDS`
  - `OPS_SCHEDULER_SYNC_ALERTS_FROM_AUDIT`
  - `OPS_JOB_SLA_GRACE_MINUTES`
  - `OPS_JOB_RUNNING_TIMEOUT_MINUTES`
- 新增测试：
  - `test_job_scheduler.py`
  - `test_ops_dashboard.py`

## 0.5.0

- 新增数据许可证治理：
  - 授权台账（`/data/licenses/register`, `/data/licenses`, `/data/licenses/check`）
  - 使用范围、导出许可、行数上限、水印策略校验
  - 可通过 `ENFORCE_DATA_LICENSE` 开启运行时强制
  - market/signal/backtest/pipeline/research/report/audit-export 增加 license 审计元数据
- 新增告警中心 v2：
  - 基于事件过滤和最小严重级别的订阅模型
  - 去重窗口与频率控制
  - 通知收件箱与 ACK 接口
- 新增运维作业中心：
  - 支持 `pipeline_daily`, `research_workflow`, `report_generate` 作业定义
  - 手动触发接口与持久化运行历史
  - 结构化运行摘要，便于下游看板消费
- 新增运行/存储配置：
  - `LICENSE_DB_PATH`, `JOB_DB_PATH`, `ALERT_DB_PATH`, `ENFORCE_DATA_LICENSE`
- 新增测试：
  - `test_data_license.py`
  - `test_job_service.py`
  - `test_alert_service.py`

## 0.4.0

- 新增 RBAC 鉴权基础：
  - 可选 API Key 鉴权
  - 基于角色的路由守卫
  - 可配置请求头与 key-role 映射
  - 认证身份接口 `/system/auth/me`
- 新增策略治理流程：
  - 策略版本草稿注册
  - 提交评审接口
  - 带评审角色的决策接口
  - 审批接口
  - 决策历史查询
  - 最新审批版本查询
  - 可选运行时强制（signal/backtest/pipeline/research）
- 新增 PIT 防前视护栏：
  - 独立 PIT 校验模块
  - `/data/pit/validate`
  - `/data/pit/validate-events`
  - signal/backtest/pipeline/research 运行时 PIT 校验
- 新增报告中心：
  - signal/risk/replay Markdown 报告生成
  - 可选文件导出与水印
  - `/reports/generate`
- 新增合规预检接口：
  - 策略可用性检查
  - 可选审批版本强制检查
  - 一次请求完成数据质量 + PIT 检查
- 新增审计导出接口：
  - `/audit/export` 支持 `csv` 与 `jsonl`
  - `/audit/verify-chain` 支持防篡改校验
- 新增模型风险漂移接口：
  - `/model-risk/drift-check`
  - 结合回测漂移与执行跟随率进行比较
- 新增测试：
  - 策略治理
  - PIT 校验器
  - 安全 key 解析
  - 报告服务

## 0.3.0

- 新增数据治理模块：
  - 数据质量检查
  - 数据集快照注册（hash + provider + range）
- 新增组合构建模块：
  - 标的/行业约束下的优化器
  - 调仓计划生成器
  - 情景压力测试接口
- 新增回放模块：
  - 信号持久化
  - 执行回写
  - 回放报告与信号列表 API
- 新增研究工作流编排接口：
  - 批量信号生成
  - 风控过滤
  - 可选组合优化
- 新增告警与服务指标接口。
- 升级回测指标：
  - 年化收益
  - 夏普
- 新增 CLI 入口与 pipeline 脚本。
- 新增回退配置加载器（无 `pydantic-settings` 环境可用）。

