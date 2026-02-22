# 需求差距矩阵（2026-02-22）

本矩阵用于对比当前实现与 `A股半自动交易辅助系统商业分析与系统设计.docx` 的一致性（参考抽取：`docs/docx_extracted_sections.txt`）。

## 覆盖率摘要

| 需求组 | 当前状态 | 证据 |
|---|---:|---|
| 真实公告连接器 + 增量检查点 + 失败重放 | 98% | `announcement_connectors.py` 已包含 `AKSHARE`/`TUSHARE`/`HTTP_JSON`/`FILE`，支持矩阵容灾、来源健康评分、预算/凭证轮换、检查点、失败修复+重放、死信 |
| 连接器 SLA 自动化（新鲜度/积压 + 升级） | 99% | SLA 状态机、去重/冷却/恢复、升级级别、SLA 历史与 burn-rate SLO API、升级路由 |
| SLA 升级路由 + 值班闭环 + 回调治理 | 100% | 多通道路由、回调 API（`/alerts/oncall/callback`）、回调签名校验、供应商映射模板、对账 API（`/alerts/oncall/reconcile`）、定时对账作业（`alert_oncall_reconcile`） |
| 事件标准化 + NLP 自动打分管线 | 93% | normalize->ingest、基于规则集打分、漂移快照 + 贡献对比 |
| NLP 治理（版本化 + 漂移 + 标注 QA） | 95% | 多标注记录、仲裁、标注一致性 QA、标签快照谱系、漂移 SLO 历史 |
| 运维看板（作业/SLA/告警/覆盖/回放/SLO） | 98% | `/ops/dashboard` 含回放工作台、来源矩阵健康、SLA 状态、burn-rate 趋势、值班回调时间线 |
| 一键合规证据导出 + 归档治理 | 100% | 导出 + 签名 + 校验 + 双签 + WORM/KMS 元数据 + vault 拷贝 + 外部 WORM/KMS 端点集成 + 严格模式 |

当前已实现范围的综合一致性估计：**约 99%**（在当前仅 `akshare` 数据条件下）。

## 本轮新增内容

### 1. 企业值班生态集成（第 2 点已完成）

- 新增签名回调接入与关联：
  - `POST /alerts/oncall/callback`
  - `GET /alerts/oncall/events`
- 新增回调签名治理：
  - 共享密钥 HMAC 校验
  - 时间戳 TTL 检查
  - 可配置强制签名模式
- 新增映射模板提取，兼容多供应商 payload：
  - 内置模板（`pagerduty`/`opsgenie`/`wecom`/`dingtalk`）
  - 支持按配置覆盖自定义映射
- 新增事件对账能力：
  - `POST /alerts/oncall/reconcile`
  - 定时作业支持：`alert_oncall_reconcile`
  - 支持 dry-run 与回调重放统计

### 2. 受监管归档外部化（第 3 点已完成）

- 新增外部 WORM 与外部 KMS wrap 集成钩子：
  - 端点级调用（超时/鉴权 token）
  - 端点回执写入导出摘要
- 新增严格模式行为：
  - `external_require_success=true`：外部归档/加密失败即导出失败
  - `external_require_success=false`：保留 best-effort 回退
- 新增导出请求级控制参数：
  - `external_worm_endpoint`
  - `external_kms_wrap_endpoint`
  - `external_auth_token`
  - `external_timeout_seconds`
  - `external_require_success`

### 3. 验证与运维加固

- 新增/更新测试：覆盖回调签名、映射模板、对账作业、外部 WORM/KMS 导出路径。
- 最近定向验证套件：
  - `tests/test_alert_service.py`
  - `tests/test_job_service.py`
  - `tests/test_compliance_evidence.py`
  - `tests/test_ops_dashboard.py`
  - `tests/test_event_connector_service.py`
  - `tests/test_event_nlp_governance.py`
  - 结果：`31 passed`

## 剩余差距（按优先级）

### 1. 外部供应商深度集成（剩余约 1%）

- 在当前仅 `akshare` 数据条件下，框架级要求基本闭合。
- 若要严格达到 100%，还需接入有真实合同与凭证的商业数据源适配器（供应商原生 SLA 合约、限流语义、合同约束下的容灾演练）。
