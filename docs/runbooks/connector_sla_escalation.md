# 连接器 SLA 升级处置操作手册

## 范围
- 事件源新鲜度滞后。
- 待重放积压增长。
- 死信积压增长。

## 触发条件
- `event_connector_sla` 预警 / 严重事件。
- `event_connector_sla_escalation` 升级事件。
- `event_connector_sla_recovery` 恢复关闭事件。

## 升级链路（示例）
1. `L1`（warning）：值班 IM 群确认状态并开始排查。
2. `L2`（critical 重复）：通知风控负责人 + 数据平台负责人（邮件）。
3. `L3`（升级阶段）：拉起值班经理会议，要求给出恢复 ETA。

## 诊断清单
1. 在 `/events/connectors/source-health` 检查当前 active source。
2. 在 `/events/connectors/overview` 检查检查点新鲜度。
3. 在 `/events/connectors/failures` 检查失败 payload 与重试次数。
4. 若为解析/映射失败：在回放工作台修复后，重放选中行。
5. 若为上游源故障：确认矩阵容灾是否已切换到备源。
6. 若值班回调未 ACK：检查 `/alerts/oncall/events?incident_id=<id>`。

## 值班回调闭环
1. 网关/值班平台回调应调用 `POST /alerts/oncall/callback`。
2. 至少包含一个关联键：`notification_id` 或 `delivery_id`（或映射后的 custom_details 字段）。
3. 若启用签名校验，回调必须带 `timestamp` + `signature`（`sha256=<hmac_hex>`）。
4. 类 ACK 状态（`acknowledged`, `resolved`, `closed`）会自动更新通知 ACK 状态。
5. 用 `GET /alerts/notifications?only_unacked=true` 验证（关联告警应消失）。
6. 用 `GET /alerts/oncall/events?incident_id=<id>` 验证（应有回调历史）。

## 事件对账作业
1. 注册定时作业类型 `alert_oncall_reconcile`。
2. 作业 payload 包含：
- `provider`
- `endpoint`（远端事件列表 API 或本地/文件端点）
- `mapping_template`
- `limit`
3. 先 dry-run，再切换 `dry_run=false`。
4. 在 `/ops/jobs/{job_id}/runs` 观察运行结果，并在 `/ops/dashboard` 的回调时间线面板核对闭环。

## 恢复判定标准
1. 新鲜度回落到预警阈值以内。
2. 待重放积压回到策略允许区间。
3. 连续两个轮询窗口无新增死信。

## 证据留存
1. 保留 `/alerts/deliveries` 的投递日志。
2. 保留 `/events/connectors/sla/states` 的状态迁移记录。
3. 保留 `/alerts/oncall/events` 的回调记录。
4. 通过 `/compliance/evidence/export` 导出合规证据包。
