# Doc Requirement Gap Matrix (2026-02-21)

This matrix compares the current implementation with the requirement document (`A股半自动交易辅助系统商业分析与系统设计.docx`) using the parsed sections in `docs/docx_extracted_sections.txt`.

## Coverage Summary

| Requirement Group | Current Status | Evidence |
|---|---:|---|
| Real announcement connectors + incremental checkpoint + failure replay | 90% | `event_connector_service.py`, `announcement_connectors.py`, replay/dead-letter flow, repair+replay API |
| Connector SLA automation (freshness/backlog alert + escalation) | 88% | persistent SLA alert states, cooldown dedupe, recovery events, escalation levels, summary endpoint |
| Event standardization + NLP scoring auto pipeline | 85% | `event_nlp.py` normalization/scoring, ingest path, ruleset governance |
| NLP ruleset versioning + drift monitor (hit/score/contribution) | 90% | ruleset activate/list, drift snapshots, monitor summary, feedback quality deltas |
| NLP online feedback/label loop | 86% | feedback upsert/list/summary APIs and storage |
| Ops dashboard for jobs/SLA/alerts/event coverage | 92% | `/ops/dashboard` frontend + `/metrics/ops-dashboard` + connector coverage views |
| Replay workbench (manual repair then replay) | 90% | failure list filter, edit payload, replay selected, repair+replay selected, result table |

Estimated total alignment for current staged scope: **~89%**.

## What Was Added In This Iteration

1. Connector SLA productionization
- Persistent SLA alert state machine now stores escalation level/reason/time.
- SLA sync now emits escalation events (`event_connector_sla_escalation`) and recovery events.
- Added open-state summary API:
  - `GET /events/connectors/sla/states/summary`
- Added escalation-aware sync params:
  - `warning_repeat_escalate`
  - `critical_repeat_escalate`

2. Failure repair-replay operations
- Added batch repair-and-replay API:
  - `POST /events/connectors/replay/repair`
- Added server-side failure keyword filter:
  - `GET /events/connectors/failures?error_keyword=...`

3. NLP governance deepening
- Added drift monitor summary API:
  - `GET /events/nlp/drift/monitor`
- Added monitor model output with trend metrics and risk level.

4. Frontend ops dashboard rebuild
- Rebuilt `index.html` and `app.js` to remove corruption and connect all new APIs.
- Added connector SLA open-state and escalation visuals.
- Added NLP drift monitor cards + snapshot table.
- Added replay workbench enhancements:
  - keyword filtering
  - select all
  - replay selected
  - save + replay selected
  - replay result table

5. Test/runtime hardening
- Added tests for:
  - SLA escalation events and summary
  - batch repair+replay workflow
  - NLP drift monitor summary
- `run_tests.ps1` now uses writable `--basetemp` and post-run cleanup for test temp/caches.
- `pyproject.toml` now scopes pytest collection to `tests/` and excludes noisy directories.

## Remaining Gaps (Highest Priority)

1. Multi-source production connector set
- More real-world connector adapters (exchange/news/vendor diversity), rate-limit governance, and source failover policies are still shallow.

2. SLA auto-routing and channel escalation
- Escalation events exist, but downstream channel routing (email/IM/on-call rotation) and runbook links are not yet wired.

3. NLP data governance depth
- Need richer label QA governance (labeler consistency, adjudication workflow, and dataset version snapshots for model audit).

4. Dashboard observability depth
- No dedicated SLO burn-rate chart/history yet for connector and NLP governance metrics.

5. Compliance evidence packaging
- Requirement doc emphasizes strong evidence trails; exportable compliance bundles are still basic and should be expanded.
