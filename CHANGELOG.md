# Changelog

## 0.8.1

- Upgraded event connector governance to production-grade SLA automation:
  - freshness/backlog/dead-letter threshold policy with per-connector override (`config.sla`)
  - connector SLA report API (`GET /events/connectors/sla`)
  - periodic SLA audit emission + cooldown dedupe (`POST /events/connectors/sla/sync-alerts`)
  - scheduler worker now auto-runs connector SLA sync each tick
- Added replay workbench backend flow:
  - failure payload repair API (`POST /events/connectors/failures/repair`)
  - manual replay by selected failure IDs (`POST /events/connectors/replay/manual`)
  - per-item replay results and run lineage persisted
- Added NLP ruleset version governance:
  - ruleset storage/activation/list APIs (`/events/nlp/rulesets*`)
  - active ruleset hot-reload into event standardizer
  - `nlp_ruleset_version` trace persisted into event metadata
- Added NLP online drift monitoring:
  - hit-rate / score distribution / polarity mix monitoring
  - contribution-delta tracking using event-feature backtest compare
  - drift snapshot persistence and query (`POST /events/nlp/drift-check`, `GET /events/nlp/drift/snapshots`)
- Extended ops frontend dashboard:
  - connector SLA breach table (warning/critical/escalated visibility)
  - replay workbench UI for manual fix + selected-item replay
  - connector freshness and SLA critical KPI exposure
- Added test coverage:
  - connector SLA alert sync + manual failure repair/replay flow
  - NLP ruleset versioning + drift monitoring snapshot flow

## 0.8.0

- Added production-grade event connector framework:
  - connector registry with typed source adapter (`TUSHARE_ANNOUNCEMENT` / `FILE_ANNOUNCEMENT`)
  - incremental sync checkpoint state
  - connector run history with per-run metrics
  - failed ingest queue with replay / retry-backoff / dead-letter status
- Added event standardization + NLP scoring pipeline:
  - raw announcement normalization API
  - rule-based NLP event typing, polarity detection, score/confidence generation
  - normalized ingest API for auto-generated event factors
- Added event feature backtest comparison report:
  - baseline (no event feature) vs enriched (event feature enabled)
  - metric delta summary + event coverage diagnostics
  - optional markdown report persistence
- Added frontend ops board:
  - `/ops/dashboard` web UI + `/ui/ops-dashboard/*` static assets
  - visualized jobs/SLA/alerts/connector status/event coverage
- Extended ops dashboard backend:
  - event governance stats in `/metrics/ops-dashboard`
- Extended ops job types:
  - `event_connector_sync`
  - `event_connector_replay`
- Added tests:
  - `test_event_connector_service.py`
  - `test_event_feature_compare.py`
  - job/ops dashboard updates for connector and event stats

## 0.7.0

- Added event governance layer:
  - event source registry with metadata and reliability profile
  - batch event ingest with source-level upsert behavior
  - event query API with source/symbol/time filters
- Added PIT join validation for events:
  - `/events/pit/join-validate`
  - checks event existence, source ambiguity, symbol mismatch, publish/effective timing violations
- Added event feature enrichment:
  - event-factor generation (`event_score`, `negative_event_score`)
  - integrated into signal/backtest/pipeline/research paths
  - auto-enabled for `event_driven` strategy, optional for others
- Added APIs:
  - `/events/sources/register`
  - `/events/sources`
  - `/events/ingest`
  - `/events`
  - `/events/features/preview`
- Added deployment assets:
  - single-node Docker compose manifest
  - private-cloud Kubernetes baseline manifest
  - deployment runbook (`docs/deployment.md`)
- Added config:
  - `EVENT_DB_PATH`
- Added tests:
  - `test_event_service.py`
  - `test_event_enrichment_workflow.py`

## 0.6.0

- Added ops scheduler runtime:
  - cron parser (`ops/cron.py`) with list/range/step support
  - scheduler tick API (`POST /ops/jobs/scheduler/tick`)
  - optional background worker controlled by env flags
  - same-minute dedupe for scheduled runs
- Added SLA monitoring for scheduled jobs:
  - invalid cron, missed run, latest-run failure, running-timeout checks
  - SLA report API (`GET /ops/jobs/scheduler/sla`)
  - audit emission for scheduler and SLA events
- Added ops dashboard aggregation:
  - metrics API (`GET /metrics/ops-dashboard`)
  - combines job run health, alert backlog, replay execution variance, and SLA status
- Added config fields:
  - `OPS_SCHEDULER_ENABLED`
  - `OPS_SCHEDULER_TICK_SECONDS`
  - `OPS_SCHEDULER_TIMEZONE`
  - `OPS_SCHEDULER_SLA_LOG_COOLDOWN_SECONDS`
  - `OPS_SCHEDULER_SYNC_ALERTS_FROM_AUDIT`
  - `OPS_JOB_SLA_GRACE_MINUTES`
  - `OPS_JOB_RUNNING_TIMEOUT_MINUTES`
- Added tests:
  - `test_job_scheduler.py`
  - `test_ops_dashboard.py`

## 0.5.0

- Added data license governance:
  - authorization ledger (`/data/licenses/register`, `/data/licenses`, `/data/licenses/check`)
  - policy checks for usage scope, export permission, row limits, and watermark
  - optional runtime enforcement via `ENFORCE_DATA_LICENSE`
  - market/signal/backtest/pipeline/research/report/audit-export flows now emit license audit metadata
- Added alert center v2:
  - subscription model with event filters and minimum severity
  - dedupe window/frequency control
  - notification inbox and ACK endpoint
- Added ops job center:
  - job definitions for `pipeline_daily`, `research_workflow`, `report_generate`
  - manual trigger endpoint and persistent run history
  - structured run summary for downstream dashboarding
- Added runtime/storage config:
  - `LICENSE_DB_PATH`, `JOB_DB_PATH`, `ALERT_DB_PATH`, `ENFORCE_DATA_LICENSE`
- Added tests:
  - `test_data_license.py`
  - `test_job_service.py`
  - `test_alert_service.py`

## 0.4.0

- Added RBAC auth foundation:
  - optional API-key auth
  - role-based route guards
  - configurable header and key-role mapping
  - authenticated identity endpoint `/system/auth/me`
- Added strategy governance workflow:
  - strategy version draft registration
  - review submission endpoint
  - decision endpoint with reviewer roles
  - approval endpoint
  - decision history query
  - latest approved lookup
  - optional runtime enforcement for signal/backtest/pipeline/research
- Added PIT anti-lookahead guard rails:
  - dedicated PIT validator module
  - `/data/pit/validate` endpoint
  - `/data/pit/validate-events` endpoint
  - runtime PIT checks in signal/backtest/pipeline/research
- Added reporting center:
  - signal/risk/replay markdown report generation
  - optional file export with watermark
  - `/reports/generate` endpoint
- Added compliance preflight endpoint:
  - strategy availability check
  - optional approved-version enforcement check
  - data quality + PIT checks in one call
- Added audit export endpoint:
  - `/audit/export` with `csv` and `jsonl`
  - `/audit/verify-chain` for tamper-evidence checks
- Added model risk drift endpoint:
  - `/model-risk/drift-check`
  - compares backtest drift with replay follow rate
- Added additional tests:
  - strategy governance
  - PIT validator
  - security key parsing
  - reporting service

## 0.3.0

- Added data governance module:
  - data quality checks
  - dataset snapshot registry (hash + provider + range)
- Added portfolio construction module:
  - optimizer under symbol/industry caps
  - rebalance plan generator
  - scenario stress test endpoint
- Added replay module:
  - signal persistence
  - execution write-back
  - replay report + signal list API
- Added research workflow orchestration endpoint:
  - batch signal generation
  - risk filtering
  - optional portfolio optimization
- Added alert and service metrics endpoints.
- Upgraded backtest metrics:
  - annualized return
  - sharpe
- Added CLI entrypoint and pipeline script.
- Added fallback settings loader for environments without `pydantic-settings`.
