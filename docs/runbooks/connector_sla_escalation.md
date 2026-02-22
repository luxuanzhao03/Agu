# Connector SLA Escalation Runbook

## Scope
- Event source freshness lag.
- Pending replay backlog growth.
- Dead-letter backlog growth.

## Trigger
- `event_connector_sla` warning/critical events.
- `event_connector_sla_escalation` level-up events.
- `event_connector_sla_recovery` close events.

## Escalation Chain (Example)
1. `L1` (warning): on-call IM group confirms status and starts diagnosis.
2. `L2` (critical repeat): risk owner + data platform owner notified by email.
3. `L3` (escalated stage): duty manager bridge opened, recovery ETA required.

## Diagnostic Checklist
1. Check active source in `/events/connectors/source-health`.
2. Check connector checkpoint freshness in `/events/connectors/overview`.
3. Check failure payload and retry count in `/events/connectors/failures`.
4. For parse/mapping failure, patch in Replay Workbench then replay selected rows.
5. For upstream source outage, verify matrix failover picked backup source.
6. For on-call callback missing ACK, check `/alerts/oncall/events?incident_id=<id>`.

## On-call Callback Closure
1. Gateway/provider callback should call `POST /alerts/oncall/callback`.
2. Include at least one correlation key: `notification_id` or `delivery_id` (or mapped custom_details fields).
3. If signature is enabled, callback must include `timestamp` + `signature` (`sha256=<hmac_hex>`).
4. ACK-like status (`acknowledged`, `resolved`, `closed`) auto-updates notification ACK state.
5. Validate by `GET /alerts/notifications?only_unacked=true` (linked rows should disappear).
6. Validate by `GET /alerts/oncall/events?incident_id=<id>` (callback history should exist).

## Incident Reconcile Job
1. Register scheduled job type `alert_oncall_reconcile`.
2. Job payload includes:
- `provider`
- `endpoint` (remote incident list API or local/file endpoint)
- `mapping_template`
- `limit`
3. Dry-run first, then switch `dry_run=false`.
4. Monitor run result in `/ops/jobs/{job_id}/runs` and check callback timeline panel in `/ops/dashboard`.

## Recovery Criteria
1. Freshness back under warning threshold.
2. Pending backlog drains to policy range.
3. No new dead-letter growth for two poll windows.

## Evidence
1. Keep alert delivery logs from `/alerts/deliveries`.
2. Keep SLA state transitions from `/events/connectors/sla/states`.
3. Keep on-call callback records from `/alerts/oncall/events`.
4. Export compliance evidence bundle from `/compliance/evidence/export`.
