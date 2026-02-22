import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pandas as pd

from trading_assistant.core.models import (
    AnnouncementRawRecord,
    AlertSubscriptionCreateRequest,
    EventConnectorFailureRepairRequest,
    EventConnectorManualReplayRequest,
    EventConnectorRepairReplayItemRequest,
    EventConnectorRepairReplayRequest,
    EventConnectorRegisterRequest,
    EventConnectorReplayRequest,
    EventConnectorRunRequest,
    EventConnectorType,
    EventNormalizeIngestRequest,
    EventSourceRegisterRequest,
    SignalLevel,
)
from trading_assistant.alerts.service import AlertService
from trading_assistant.alerts.store import AlertStore
from trading_assistant.audit.service import AuditService
from trading_assistant.audit.store import AuditStore
from trading_assistant.governance.event_connector_service import EventConnectorService
from trading_assistant.governance.event_connector_store import EventConnectorStore
from trading_assistant.governance.event_nlp import EventStandardizer
from trading_assistant.governance.event_service import EventService
from trading_assistant.governance.event_store import EventStore


def _services(tmp_path: Path) -> tuple[EventService, EventConnectorService]:
    db_path = str(tmp_path / "event.db")
    event_service = EventService(store=EventStore(db_path))
    connector_service = EventConnectorService(
        event_service=event_service,
        connector_store=EventConnectorStore(db_path),
        standardizer=EventStandardizer(),
    )
    return event_service, connector_service


def test_normalize_ingest_generates_nlp_events(tmp_path: Path) -> None:
    events, connectors = _services(tmp_path)
    _ = events.register_source(
        EventSourceRegisterRequest(
            source_name="ann_source",
            source_type="ANNOUNCEMENT",
            provider="mock",
            created_by="qa",
        )
    )

    req = EventNormalizeIngestRequest(
        source_name="ann_source",
        records=[
            AnnouncementRawRecord(
                source_event_id="r1",
                symbol="000001",
                title="Earnings beat guidance",
                summary="profit growth and guidance up",
                publish_time_text="2025-01-10 08:30:00",
            ),
            AnnouncementRawRecord(
                source_event_id="r2",
                symbol="000001",
                title="Regulatory investigation notice",
                summary="received investigation and penalty warning",
                publish_time_text="2025-01-11 08:30:00",
            ),
        ],
    )
    result = connectors.normalize_and_ingest(req)
    assert result.ingest is not None
    assert result.ingest.inserted == 2
    rows = events.list_events(source_name="ann_source", symbol="000001", limit=20)
    assert len(rows) == 2
    assert any(r.polarity.value == "POSITIVE" for r in rows)
    assert any(r.polarity.value == "NEGATIVE" for r in rows)


def test_file_connector_run_updates_checkpoint(tmp_path: Path) -> None:
    events, connectors = _services(tmp_path)
    _ = events.register_source(
        EventSourceRegisterRequest(
            source_name="feed_source",
            source_type="ANNOUNCEMENT",
            provider="mock",
            created_by="qa",
        )
    )
    payload_file = tmp_path / "feed.json"
    payload_file.write_text(
        json.dumps(
            [
                {
                    "source_event_id": "a1",
                    "symbol": "000001",
                    "title": "Share buyback announcement",
                    "summary": "board approved buyback plan",
                    "publish_time_text": "2025-01-08 20:00:00",
                },
                {
                    "source_event_id": "a2",
                    "symbol": "000001",
                    "title": "Winning bid for major contract",
                    "summary": "signed major contract order",
                    "publish_time_text": "2025-01-09 20:00:00",
                },
            ],
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    _ = connectors.register_connector(
        EventConnectorRegisterRequest(
            connector_name="file_feed",
            source_name="feed_source",
            connector_type=EventConnectorType.FILE_ANNOUNCEMENT,
            config={"file_path": str(payload_file)},
            created_by="qa",
        )
    )

    first = connectors.run_connector(EventConnectorRunRequest(connector_name="file_feed", triggered_by="qa"))
    assert first.run.status.value in {"SUCCESS", "PARTIAL"}
    assert first.run.inserted_count == 2
    second = connectors.run_connector(EventConnectorRunRequest(connector_name="file_feed", triggered_by="qa"))
    assert second.run.pulled_count == 0
    assert second.run.inserted_count == 0
    rows = events.list_events(source_name="feed_source", symbol="000001", limit=20)
    assert len(rows) == 2


def test_replay_failures_reingest_event(tmp_path: Path) -> None:
    events, connectors = _services(tmp_path)
    _ = events.register_source(
        EventSourceRegisterRequest(
            source_name="replay_source",
            source_type="ANNOUNCEMENT",
            provider="mock",
            created_by="qa",
        )
    )
    _ = connectors.register_connector(
        EventConnectorRegisterRequest(
            connector_name="replay_connector",
            source_name="replay_source",
            connector_type=EventConnectorType.FILE_ANNOUNCEMENT,
            config={"file_path": str(tmp_path / "empty.json")},
            created_by="qa",
        )
    )

    _ = connectors.store.append_failures(
        connector_name="replay_connector",
        source_name="replay_source",
        run_id="seed-run",
        payloads=[
            {
                "phase": "ingest",
                "event": {
                    "event_id": "retry-evt-1",
                    "symbol": "000001",
                    "event_type": "share_buyback",
                    "publish_time": datetime(2025, 1, 12, 8, 0, tzinfo=timezone.utc).isoformat(),
                    "polarity": "POSITIVE",
                    "score": 0.85,
                    "confidence": 0.9,
                    "title": "Buyback progress",
                    "summary": "buyback ratio increased",
                    "raw_ref": None,
                    "tags": ["buyback"],
                    "metadata": {"from": "replay"},
                },
            }
        ],
        error_message="seed failure",
    )

    replay = connectors.replay_failures(
        EventConnectorReplayRequest(connector_name="replay_connector", limit=10, triggered_by="qa")
    )
    assert replay.picked >= 1
    assert replay.replayed >= 1
    rows = events.list_events(source_name="replay_source", symbol="000001", limit=20)
    assert any(r.event_id == "retry-evt-1" for r in rows)


def test_connector_sla_sync_generates_alert_events(tmp_path: Path) -> None:
    events, connectors = _services(tmp_path)
    _ = events.register_source(
        EventSourceRegisterRequest(
            source_name="sla_source",
            source_type="ANNOUNCEMENT",
            provider="mock",
            created_by="qa",
        )
    )
    _ = connectors.register_connector(
        EventConnectorRegisterRequest(
            connector_name="sla_connector",
            source_name="sla_source",
            connector_type=EventConnectorType.FILE_ANNOUNCEMENT,
            config={"file_path": str(tmp_path / "empty.json"), "sla": {"pending_warning": 1, "dead_warning": 1}},
            created_by="qa",
        )
    )
    _ = connectors.store.append_failures(
        connector_name="sla_connector",
        source_name="sla_source",
        run_id="seed-run",
        payloads=[{"phase": "ingest", "error": "x"}, {"phase": "ingest", "error": "y"}],
        error_message="seed failure",
    )
    pending = connectors.store.list_failures(connector_name="sla_connector", limit=10)
    assert pending
    connectors.store.mark_failure_dead(pending[0].id, error_message="forced-dead")

    report = connectors.evaluate_sla()
    assert report.connector_count >= 1
    assert report.warning_count + report.critical_count >= 1

    audit = AuditService(AuditStore(str(tmp_path / "audit.db")))
    sync = connectors.sync_sla_alerts(audit=audit, cooldown_seconds=600)
    assert sync.emitted >= 1
    assert sync.open_states >= 1
    second = connectors.sync_sla_alerts(audit=audit, cooldown_seconds=600)
    assert second.skipped >= 1

    # Resolve current breaches and ensure recovery events are emitted.
    for row in connectors.store.list_failures(connector_name="sla_connector", limit=100):
        connectors.store.mark_failure_replayed(row.id)
    recovered = connectors.sync_sla_alerts(audit=audit, cooldown_seconds=600)
    assert recovered.recovered >= 1
    recovery_events = [e for e in audit.query(limit=200) if e.event_type == "event_connector_sla_recovery"]
    assert recovery_events

    alerts = AlertService(store=AlertStore(str(tmp_path / "alert.db")), audit=audit)
    _ = alerts.create_subscription(
        AlertSubscriptionCreateRequest(
            name="connector-sla",
            owner="ops",
            event_types=["event_connector_sla"],
            min_severity=SignalLevel.WARNING,
            dedupe_window_sec=1,
            enabled=True,
        )
    )
    inserted = alerts.sync_from_audit(limit=500)
    assert inserted >= 1
    notifications = alerts.list_notifications(only_unacked=True, limit=20)
    assert any(n.source == "event_connector_sla" for n in notifications)


def test_manual_repair_then_replay_selected_failure(tmp_path: Path) -> None:
    events, connectors = _services(tmp_path)
    _ = events.register_source(
        EventSourceRegisterRequest(
            source_name="manual_source",
            source_type="ANNOUNCEMENT",
            provider="mock",
            created_by="qa",
        )
    )
    _ = connectors.register_connector(
        EventConnectorRegisterRequest(
            connector_name="manual_connector",
            source_name="manual_source",
            connector_type=EventConnectorType.FILE_ANNOUNCEMENT,
            config={"file_path": str(tmp_path / "empty.json")},
            created_by="qa",
        )
    )
    _ = connectors.store.append_failures(
        connector_name="manual_connector",
        source_name="manual_source",
        run_id="seed-run",
        payloads=[
            {
                "phase": "normalize",
                "raw_record": {
                    "source_event_id": "manual-evt-1",
                    "title": "buyback progress",
                    "summary": "board approved buyback ratio increase",
                    "publish_time_text": "2025-01-15 08:30:00",
                },
            }
        ],
        error_message="symbol missing",
    )
    rows = connectors.store.list_failures(connector_name="manual_connector", limit=10)
    assert rows
    failure_id = rows[0].id

    repaired = connectors.repair_failure(
        EventConnectorFailureRepairRequest(
            connector_name="manual_connector",
            failure_id=failure_id,
            patch_raw_record={"symbol": "000001"},
            reset_retry_count=True,
            triggered_by="ops_qa",
            note="fill missing symbol",
        )
    )
    assert repaired.updated is True
    assert repaired.failure is not None

    replay = connectors.replay_selected_failures(
        EventConnectorManualReplayRequest(
            connector_name="manual_connector",
            failure_ids=[failure_id],
            triggered_by="ops_qa",
        )
    )
    assert replay.picked == 1
    assert replay.replayed == 1
    events_rows = events.list_events(source_name="manual_source", symbol="000001", limit=20)
    assert any(r.event_id.startswith("manual_source-") or r.event_id == "manual-evt-1" for r in events_rows)


def test_sla_sync_escalation_events_and_summary(tmp_path: Path) -> None:
    events, connectors = _services(tmp_path)
    _ = events.register_source(
        EventSourceRegisterRequest(
            source_name="sla_escalation_source",
            source_type="ANNOUNCEMENT",
            provider="mock",
            created_by="qa",
        )
    )
    _ = connectors.register_connector(
        EventConnectorRegisterRequest(
            connector_name="sla_escalation_connector",
            source_name="sla_escalation_source",
            connector_type=EventConnectorType.FILE_ANNOUNCEMENT,
            config={
                "file_path": str(tmp_path / "empty.json"),
                "sla": {
                    "pending_warning": 1,
                    "pending_critical": 3,
                    "pending_escalation": 9,
                },
            },
            created_by="qa",
        )
    )
    _ = connectors.store.append_failures(
        connector_name="sla_escalation_connector",
        source_name="sla_escalation_source",
        run_id="seed-run",
        payloads=[{"phase": "ingest", "error": "seed pending"}],
        error_message="seed pending",
    )
    audit = AuditService(AuditStore(str(tmp_path / "audit.db")))

    first = connectors.sync_sla_alerts(
        audit=audit,
        cooldown_seconds=0,
        warning_repeat_escalate=2,
        critical_repeat_escalate=2,
    )
    assert first.escalated == 0

    second = connectors.sync_sla_alerts(
        audit=audit,
        cooldown_seconds=0,
        warning_repeat_escalate=2,
        critical_repeat_escalate=2,
    )
    assert second.escalated >= 1
    assert second.open_escalated >= 1

    escalation_events = [e for e in audit.query(limit=200) if e.event_type == "event_connector_sla_escalation"]
    assert escalation_events
    summary = connectors.sla_alert_state_summary(connector_name="sla_escalation_connector")
    assert summary.open_states >= 1
    assert summary.escalated_open_states >= 1
    assert any(int(level) >= 1 for level in summary.open_by_escalation_level)


def test_batch_repair_and_replay_failures(tmp_path: Path) -> None:
    events, connectors = _services(tmp_path)
    _ = events.register_source(
        EventSourceRegisterRequest(
            source_name="batch_manual_source",
            source_type="ANNOUNCEMENT",
            provider="mock",
            created_by="qa",
        )
    )
    _ = connectors.register_connector(
        EventConnectorRegisterRequest(
            connector_name="batch_manual_connector",
            source_name="batch_manual_source",
            connector_type=EventConnectorType.FILE_ANNOUNCEMENT,
            config={"file_path": str(tmp_path / "empty.json")},
            created_by="qa",
        )
    )
    _ = connectors.store.append_failures(
        connector_name="batch_manual_connector",
        source_name="batch_manual_source",
        run_id="seed-run",
        payloads=[
            {
                "phase": "normalize",
                "raw_record": {
                    "source_event_id": "manual-batch-1",
                    "title": "buyback progress",
                    "summary": "board approved buyback ratio increase",
                    "publish_time_text": "2025-01-15 08:30:00",
                },
            },
            {
                "phase": "normalize",
                "raw_record": {
                    "source_event_id": "manual-batch-2",
                    "title": "earnings beat guidance",
                    "summary": "profit and revenue improved",
                    "publish_time_text": "2025-01-16 08:30:00",
                },
            },
        ],
        error_message="symbol missing",
    )
    failures = connectors.store.list_failures(connector_name="batch_manual_connector", limit=10)
    failure_ids = [row.id for row in failures]
    assert len(failure_ids) == 2

    result = connectors.repair_and_replay_failures(
        EventConnectorRepairReplayRequest(
            connector_name="batch_manual_connector",
            triggered_by="ops_qa",
            items=[
                EventConnectorRepairReplayItemRequest(
                    failure_id=failure_ids[0],
                    patch_raw_record={"symbol": "000001"},
                    reset_retry_count=True,
                    note="bulk patch symbol",
                ),
                EventConnectorRepairReplayItemRequest(
                    failure_id=failure_ids[1],
                    patch_raw_record={"symbol": "000001"},
                    reset_retry_count=True,
                    note="bulk patch symbol",
                ),
            ],
        )
    )
    assert result.repaired == 2
    assert result.picked == 2
    assert result.replayed == 2
    assert result.failed == 0

    event_rows = events.list_events(source_name="batch_manual_source", symbol="000001", limit=20)
    assert len(event_rows) >= 2


def test_source_matrix_failover_and_health_scoring(tmp_path: Path) -> None:
    events, connectors = _services(tmp_path)
    _ = events.register_source(
        EventSourceRegisterRequest(
            source_name="matrix_source",
            source_type="ANNOUNCEMENT",
            provider="mock",
            created_by="qa",
        )
    )
    backup_file = tmp_path / "backup.json"
    backup_file.write_text(
        json.dumps(
            [
                {
                    "source_event_id": "mx-1",
                    "symbol": "000001",
                    "title": "Backup source announcement",
                    "summary": "fallback source healthy",
                    "publish_time_text": "2025-02-01 08:30:00",
                }
            ],
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    _ = connectors.register_connector(
        EventConnectorRegisterRequest(
            connector_name="matrix_connector",
            source_name="matrix_source",
            connector_type=EventConnectorType.FILE_ANNOUNCEMENT,
            config={
                "failover": {"enabled": True, "health_threshold": 40, "max_candidates_per_run": 3},
                "source_matrix": [
                    {
                        "source_key": "primary_dead",
                        "connector_type": "FILE_ANNOUNCEMENT",
                        "priority": 10,
                        "enabled": True,
                        "config": {"file_path": str(tmp_path / "missing_primary.json")},
                    },
                    {
                        "source_key": "backup_ok",
                        "connector_type": "FILE_ANNOUNCEMENT",
                        "priority": 20,
                        "enabled": True,
                        "config": {"file_path": str(backup_file)},
                    },
                ],
            },
            created_by="qa",
        )
    )

    result = connectors.run_connector(EventConnectorRunRequest(connector_name="matrix_connector", triggered_by="qa"))
    assert result.run.status.value in {"SUCCESS", "PARTIAL"}
    assert result.run.inserted_count >= 1
    assert result.run.details.get("selected_source_key") == "backup_ok"
    assert any("source=primary_dead" in err for err in result.errors)

    states = connectors.list_source_states(connector_name="matrix_connector", limit=20)
    assert len(states) == 2
    by_key = {x.source_key: x for x in states}
    assert by_key["backup_ok"].is_active is True
    assert by_key["primary_dead"].consecutive_failures >= 1
    assert by_key["primary_dead"].health_score < by_key["backup_ok"].health_score


def test_http_json_connector_from_local_file_url(tmp_path: Path) -> None:
    events, connectors = _services(tmp_path)
    _ = events.register_source(
        EventSourceRegisterRequest(
            source_name="http_json_source",
            source_type="ANNOUNCEMENT",
            provider="mock",
            created_by="qa",
        )
    )

    payload_file = tmp_path / "http_payload.json"
    payload_file.write_text(
        json.dumps(
            {
                "data": {
                    "items": [
                        {
                            "id": "http-1",
                            "symbol": "000001",
                            "title": "HTTP json announcement",
                            "summary": "connected by http-json connector",
                            "publish_time_text": "2025-02-05 09:15:00",
                        }
                    ]
                }
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    file_url = payload_file.resolve().as_uri()

    _ = connectors.register_connector(
        EventConnectorRegisterRequest(
            connector_name="http_json_connector",
            source_name="http_json_source",
            connector_type=EventConnectorType.HTTP_JSON_ANNOUNCEMENT,
            config={
                "url": file_url,
                "method": "GET",
                "records_path": "data.items",
                "limit_param": "limit",
                "cursor_param": "cursor",
            },
            created_by="qa",
        )
    )

    result = connectors.run_connector(EventConnectorRunRequest(connector_name="http_json_connector", triggered_by="qa"))
    assert result.run.status.value in {"SUCCESS", "PARTIAL"}
    assert result.run.inserted_count == 1
    rows = events.list_events(source_name="http_json_source", symbol="000001", limit=20)
    assert any(r.event_id == "http-1" for r in rows)


def test_source_budget_governance_skips_exhausted_source(tmp_path: Path) -> None:
    events, connectors = _services(tmp_path)
    _ = events.register_source(
        EventSourceRegisterRequest(
            source_name="budget_source",
            source_type="ANNOUNCEMENT",
            provider="mock",
            created_by="qa",
        )
    )
    primary_file = tmp_path / "budget_primary.json"
    backup_file = tmp_path / "budget_backup.json"
    payload = [
        {
            "source_event_id": "budget-1",
            "symbol": "000001",
            "title": "Budget source announcement",
            "summary": "source budget test",
            "publish_time_text": "2025-02-20 08:30:00",
        }
    ]
    primary_file.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    backup_file.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")

    _ = connectors.register_connector(
        EventConnectorRegisterRequest(
            connector_name="budget_connector",
            source_name="budget_source",
            connector_type=EventConnectorType.FILE_ANNOUNCEMENT,
            config={
                "failover": {"enabled": True, "max_candidates_per_run": 2},
                "source_matrix": [
                    {
                        "source_key": "primary_budget_1h",
                        "connector_type": "FILE_ANNOUNCEMENT",
                        "priority": 10,
                        "enabled": True,
                        "request_budget_per_hour": 1,
                        "config": {"file_path": str(primary_file)},
                    },
                    {
                        "source_key": "backup_unlimited",
                        "connector_type": "FILE_ANNOUNCEMENT",
                        "priority": 20,
                        "enabled": True,
                        "config": {"file_path": str(backup_file)},
                    },
                ],
            },
            created_by="qa",
        )
    )

    first = connectors.run_connector(
        EventConnectorRunRequest(connector_name="budget_connector", triggered_by="qa", force_full_sync=True)
    )
    assert first.run.status.value in {"SUCCESS", "PARTIAL"}
    assert first.run.details.get("selected_source_key") == "primary_budget_1h"

    second = connectors.run_connector(
        EventConnectorRunRequest(connector_name="budget_connector", triggered_by="qa", force_full_sync=True)
    )
    assert second.run.status.value in {"SUCCESS", "PARTIAL"}
    assert second.run.details.get("selected_source_key") == "backup_unlimited"
    attempts = second.run.details.get("source_attempts") or []
    assert any(x.get("status") == "SKIPPED_BUDGET" and x.get("source_key") == "primary_budget_1h" for x in attempts)


def test_source_credential_alias_rotates_between_runs(tmp_path: Path) -> None:
    events, connectors = _services(tmp_path)
    _ = events.register_source(
        EventSourceRegisterRequest(
            source_name="credential_source",
            source_type="ANNOUNCEMENT",
            provider="mock",
            created_by="qa",
        )
    )
    payload_file = tmp_path / "credential_payload.json"
    payload_file.write_text(
        json.dumps(
            [
                {
                    "source_event_id": "cred-1",
                    "symbol": "000001",
                    "title": "Credential rotation announcement",
                    "summary": "rotation",
                    "publish_time_text": "2025-02-20 09:30:00",
                }
            ],
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    _ = connectors.register_connector(
        EventConnectorRegisterRequest(
            connector_name="credential_connector",
            source_name="credential_source",
            connector_type=EventConnectorType.FILE_ANNOUNCEMENT,
            config={
                "source_matrix": [
                    {
                        "source_key": "primary",
                        "connector_type": "FILE_ANNOUNCEMENT",
                        "priority": 10,
                        "enabled": True,
                        "credential_aliases": ["cred_a", "cred_b"],
                        "config": {
                            "file_path": str(payload_file),
                            "credentials": {
                                "cred_a": {"api_token": "token-a"},
                                "cred_b": {"api_token": "token-b"},
                            },
                        },
                    }
                ]
            },
            created_by="qa",
        )
    )

    first = connectors.run_connector(
        EventConnectorRunRequest(connector_name="credential_connector", triggered_by="qa", force_full_sync=True)
    )
    second = connectors.run_connector(
        EventConnectorRunRequest(connector_name="credential_connector", triggered_by="qa", force_full_sync=True)
    )
    a1 = (first.run.details.get("source_attempts") or [{}])[0].get("credential_alias")
    a2 = (second.run.details.get("source_attempts") or [{}])[0].get("credential_alias")
    assert a1 in {"cred_a", "cred_b"}
    assert a2 in {"cred_a", "cred_b"}
    assert a1 != a2


def test_connector_slo_history_tracks_burn_rate(tmp_path: Path) -> None:
    events, connectors = _services(tmp_path)
    _ = events.register_source(
        EventSourceRegisterRequest(
            source_name="slo_source",
            source_type="ANNOUNCEMENT",
            provider="mock",
            created_by="qa",
        )
    )
    _ = connectors.register_connector(
        EventConnectorRegisterRequest(
            connector_name="slo_connector",
            source_name="slo_source",
            connector_type=EventConnectorType.FILE_ANNOUNCEMENT,
            config={"file_path": str(tmp_path / "empty.json"), "sla": {"pending_warning": 1}},
            created_by="qa",
        )
    )
    _ = connectors.store.append_failures(
        connector_name="slo_connector",
        source_name="slo_source",
        run_id="seed-run",
        payloads=[{"phase": "ingest", "error": "seed pending"}],
        error_message="seed failure",
    )
    audit = AuditService(AuditStore(str(tmp_path / "audit.db")))
    _ = connectors.sync_sla_alerts(audit=audit, cooldown_seconds=0)
    _ = connectors.sync_sla_alerts(audit=audit, cooldown_seconds=0)

    history = connectors.slo_history(connector_name="slo_connector", lookback_days=1, bucket_hours=1)
    assert history.total_points >= 1
    assert any((p.warning_breaches + p.critical_breaches) > 0 for p in history.points)
    assert any(p.burn_rate_warning >= 0 for p in history.points)


def test_akshare_connector_handles_cn_columns_and_api_fallback(tmp_path: Path, monkeypatch) -> None:
    events, connectors = _services(tmp_path)
    _ = events.register_source(
        EventSourceRegisterRequest(
            source_name="akshare_source",
            source_type="ANNOUNCEMENT",
            provider="akshare",
            created_by="qa",
        )
    )

    def stock_notice_report(**kwargs):
        _ = kwargs
        return pd.DataFrame(
            [
                {
                    "公告编号": "ak-1",
                    "代码": "000001",
                    "公告标题": "回购进展公告",
                    "公告摘要": "公司继续推进股份回购",
                    "公告内容": "董事会通过回购上限调整",
                    "公告日期": "2025-02-22 09:30:00",
                    "公告链接": "https://example.com/ak-1",
                    "额外字段": "extra",
                }
            ]
        )

    fake_ak = SimpleNamespace(stock_notice_report=stock_notice_report)
    monkeypatch.setitem(sys.modules, "akshare", fake_ak)

    _ = connectors.register_connector(
        EventConnectorRegisterRequest(
            connector_name="akshare_connector",
            source_name="akshare_source",
            connector_type=EventConnectorType.AKSHARE_ANNOUNCEMENT,
            config={
                "api_name": "stock_notice_report",
                "api_candidates": ["missing_api", "stock_notice_report"],
                "request_kwargs": {"symbol": "000001"},
                "column_map": {
                    "event_id": ["公告编号", "id"],
                    "symbol": ["代码"],
                    "title": ["公告标题"],
                    "summary": ["公告摘要"],
                    "content": ["公告内容"],
                    "publish_time": ["公告日期"],
                    "url": ["公告链接"],
                },
            },
            created_by="qa",
        )
    )

    result = connectors.run_connector(
        EventConnectorRunRequest(connector_name="akshare_connector", triggered_by="qa")
    )
    assert result.run.status.value in {"SUCCESS", "PARTIAL"}
    assert result.run.inserted_count == 1

    rows = events.list_events(source_name="akshare_source", symbol="000001", limit=20)
    assert len(rows) == 1
    assert rows[0].event_id == "ak-1"
    assert rows[0].title == "回购进展公告"
    assert rows[0].summary == "公司继续推进股份回购"
