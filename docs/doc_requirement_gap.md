# Doc Requirement Gap Matrix (2026-02-22)

This matrix compares current implementation against `A股半自动交易辅助系统商业分析与系统设计.docx` (reference extraction: `docs/docx_extracted_sections.txt`).

## Coverage Summary

| Requirement Group | Current Status | Evidence |
|---|---:|---|
| Real announcement connectors + incremental checkpoint + failure replay | 98% | `announcement_connectors.py` includes `AKSHARE`/`TUSHARE`/`HTTP_JSON`/`FILE`, matrix failover, source health scoring, budget/credential rotation, checkpoint, failure repair+replay, dead-letter |
| Connector SLA automation (freshness/backlog + escalation) | 99% | SLA state machine, dedupe/cooldown/recovery, escalation levels, SLA history + burn-rate SLO APIs + escalation routing |
| SLA escalation routing + on-call closure + callback governance | 100% | outbound multi-channel routing, inbound callback API (`/alerts/oncall/callback`), callback signature verification, provider mapping templates, reconcile API (`/alerts/oncall/reconcile`), scheduled reconcile job (`alert_oncall_reconcile`) |
| Event standardization + NLP auto scoring pipeline | 93% | normalize->ingest, ruleset-driven scoring, drift snapshot + contribution comparison |
| NLP governance (versioning + drift + labeling QA) | 95% | multi-label entries, adjudication, consistency QA, label snapshot lineage, drift SLO history |
| Ops dashboard (jobs/SLA/alerts/coverage/replay/SLO) | 98% | `/ops/dashboard` with replay workbench, source matrix health, SLA states, burn-rate trends, on-call callback timeline |
| One-click compliance evidence export + archive governance | 100% | export + sign + verify + countersign + WORM/KMS metadata + vault copy + external WORM/KMS endpoint integration + strict mode |

Estimated total alignment for currently implemented scope: **~99%** (under current `akshare` data-only condition).

## Added In This Iteration

1. Enterprise on-call ecosystem integration (point 2 completed)
- Added signed callback ingestion and linkage:
  - `POST /alerts/oncall/callback`
  - `GET /alerts/oncall/events`
- Added callback signature governance:
  - shared-secret HMAC verification
  - timestamp TTL check
  - configurable mandatory signature mode
- Added mapping template extraction for provider payload variants:
  - built-in templates (`pagerduty`/`opsgenie`/`wecom`/`dingtalk`)
  - custom mapping override by config
- Added incident reconciliation capability:
  - `POST /alerts/oncall/reconcile`
  - scheduled job support: `alert_oncall_reconcile`
  - dry-run support and callback replay metrics

2. Regulated archive externalization (point 3 completed)
- Added external WORM and external KMS wrap integration hooks:
  - endpoint-level invocation with timeout/auth token
  - endpoint payload receipts persisted into export summary
- Added strict-mode behavior:
  - `external_require_success=true` turns external archive/wrap failures into hard failures
  - `external_require_success=false` keeps best-effort fallback
- Added evidence export request-level controls:
  - `external_worm_endpoint`
  - `external_kms_wrap_endpoint`
  - `external_auth_token`
  - `external_timeout_seconds`
  - `external_require_success`

3. Validation and operations hardening
- Added/updated tests for callback signature + mapping + reconcile jobs + external WORM/KMS export path.
- Recent targeted verification suite:
  - `tests/test_alert_service.py`
  - `tests/test_job_service.py`
  - `tests/test_compliance_evidence.py`
  - `tests/test_ops_dashboard.py`
  - `tests/test_event_connector_service.py`
  - `tests/test_event_nlp_governance.py`
  - result: `31 passed`

## Remaining Gaps (Priority Order)

1. External vendor depth (remaining ~1%)
- In current `akshare` data-only condition, framework-level requirements are closed.
- For strict 100%, still needs production commercial provider adapters with real contracts/credentials (vendor-native SLA contract, throttling semantics, and contract-backed failover drills).
