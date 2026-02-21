import json
from datetime import datetime, timezone
from pathlib import Path

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
