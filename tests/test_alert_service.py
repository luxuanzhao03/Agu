from datetime import datetime, timezone
import hashlib
import hmac
import json
from pathlib import Path

from trading_assistant.alerts.service import AlertService
from trading_assistant.alerts.store import AlertStore
from trading_assistant.audit.service import AuditService
from trading_assistant.audit.store import AuditStore
from trading_assistant.core.models import (
    AlertEscalationStage,
    OncallCallbackRequest,
    OncallReconcileRequest,
    AlertSubscriptionCreateRequest,
    SignalLevel,
)


class FakeDispatcher:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str, str, dict]] = []

    def send(self, *, channel: str, target: str, subject: str, message: str, payload: dict):
        _ = message
        self.calls.append((channel, target, subject, payload))
        return type("DispatchResult", (), {"success": True, "error_message": "", "provider_status": "200"})()


def test_alert_subscription_dedupe_and_ack(tmp_path: Path) -> None:
    audit = AuditService(AuditStore(str(tmp_path / "audit.db")))
    service = AlertService(store=AlertStore(str(tmp_path / "alert.db")), audit=audit)

    sub_id = service.create_subscription(
        AlertSubscriptionCreateRequest(
            name="risk-monitor",
            owner="risk_team",
            event_types=["risk_check"],
            min_severity=SignalLevel.WARNING,
            dedupe_window_sec=3600,
            enabled=True,
        )
    )
    assert sub_id > 0

    audit.log("risk_check", "evaluate", {"symbol": "000001", "blocked": True})
    audit.log("risk_check", "evaluate", {"symbol": "000002", "blocked": True})

    inserted = service.sync_from_audit(limit=100)
    assert inserted == 1

    notes = service.list_notifications(subscription_id=sub_id, only_unacked=True, limit=10)
    assert len(notes) == 1
    assert notes[0].severity == SignalLevel.WARNING

    acked = service.ack(notes[0].id)
    assert acked is True
    notes_after = service.list_notifications(subscription_id=sub_id, only_unacked=True, limit=10)
    assert notes_after == []


def test_sla_oncall_escalation_dispatch_and_delivery_log(tmp_path: Path) -> None:
    audit = AuditService(AuditStore(str(tmp_path / "audit.db")))
    dispatcher = FakeDispatcher()
    service = AlertService(
        store=AlertStore(str(tmp_path / "alert.db")),
        audit=audit,
        dispatcher=dispatcher,
        default_runbook_base_url="https://runbook.example.com/connectors",
    )

    _ = service.create_subscription(
        AlertSubscriptionCreateRequest(
            name="sla-oncall",
            owner="ops",
            event_types=["event_connector_sla_escalation"],
            min_severity=SignalLevel.WARNING,
            dedupe_window_sec=1,
            enabled=True,
            channel="oncall",
            channel_config={
                "webhooks": ["https://im.example.com/webhook/ops"],
                "email_to": ["risk@example.com"],
            },
            escalation_chain=[
                AlertEscalationStage(level_threshold=1, channel="im", targets=["https://im.example.com/webhook/ops"]),
                AlertEscalationStage(level_threshold=2, channel="email", targets=["risk@example.com"]),
            ],
            runbook_url="https://runbook.example.com/sla",
        )
    )

    audit.log(
        "event_connector_sla_escalation",
        "level_2",
        {
            "connector_name": "ann_matrix_cn",
            "severity": "CRITICAL",
            "escalation_level": 2,
            "escalation_reason": "critical repeated",
            "message": "pending backlog exceeded",
            "runbook_url": "https://runbook.example.com/sla",
        },
        status="ERROR",
    )
    inserted = service.sync_from_audit(limit=200)
    assert inserted == 1
    assert len(dispatcher.calls) == 2
    assert {c[0] for c in dispatcher.calls} == {"im", "email"}

    notifications = service.list_notifications(limit=20)
    assert notifications
    deliveries = service.list_deliveries(notification_id=notifications[0].id, limit=20)
    assert len(deliveries) == 2
    assert all(d.status.value == "SENT" for d in deliveries)


def test_pagerduty_channel_payload_includes_routing_key(tmp_path: Path) -> None:
    audit = AuditService(AuditStore(str(tmp_path / "audit.db")))
    dispatcher = FakeDispatcher()
    service = AlertService(
        store=AlertStore(str(tmp_path / "alert.db")),
        audit=audit,
        dispatcher=dispatcher,
    )

    _ = service.create_subscription(
        AlertSubscriptionCreateRequest(
            name="pagerduty-oncall",
            owner="ops",
            event_types=["event_connector_sla"],
            min_severity=SignalLevel.WARNING,
            dedupe_window_sec=1,
            enabled=True,
            channel="pagerduty",
            channel_config={
                "pagerduty_events_api": ["https://events.pagerduty.com/v2/enqueue"],
                "pagerduty_routing_key": "rk-demo",
            },
        )
    )
    audit.log(
        "event_connector_sla",
        "freshness",
        {
            "connector_name": "ann_primary",
            "severity": "CRITICAL",
            "message": "freshness lag exceeded",
        },
        status="ERROR",
    )
    inserted = service.sync_from_audit(limit=200)
    assert inserted == 1
    assert len(dispatcher.calls) == 1
    call = dispatcher.calls[0]
    assert call[0] == "pagerduty"
    assert call[1] == "https://events.pagerduty.com/v2/enqueue"
    assert call[3]["pagerduty_routing_key"] == "rk-demo"


def test_oncall_callback_ack_and_idempotent_upsert(tmp_path: Path) -> None:
    audit = AuditService(AuditStore(str(tmp_path / "audit.db")))
    dispatcher = FakeDispatcher()
    service = AlertService(
        store=AlertStore(str(tmp_path / "alert.db")),
        audit=audit,
        dispatcher=dispatcher,
    )

    _ = service.create_subscription(
        AlertSubscriptionCreateRequest(
            name="oncall-callback",
            owner="ops",
            event_types=["event_connector_sla_escalation"],
            min_severity=SignalLevel.WARNING,
            dedupe_window_sec=1,
            enabled=True,
            channel="oncall",
            channel_config={"webhooks": ["https://im.example.com/webhook/ops"]},
            escalation_chain=[
                AlertEscalationStage(
                    level_threshold=1,
                    channel="im",
                    targets=["https://im.example.com/webhook/ops"],
                    note="L1",
                )
            ],
        )
    )

    audit.log(
        "event_connector_sla_escalation",
        "level_1",
        {
            "connector_name": "ann_source_a",
            "severity": "WARNING",
            "escalation_level": 1,
            "message": "pending backlog warning",
        },
        status="ERROR",
    )
    inserted = service.sync_from_audit(limit=100)
    assert inserted == 1

    notifications = service.list_notifications(only_unacked=True, limit=20)
    assert len(notifications) == 1
    notification_id = notifications[0].id
    deliveries = service.list_deliveries(notification_id=notification_id, limit=20)
    assert len(deliveries) == 1
    delivery_id = deliveries[0].id

    callback = OncallCallbackRequest(
        provider="pagerduty",
        incident_id="INC-1001",
        status="acknowledged",
        delivery_id=delivery_id,
        external_ticket_id="PD-CASE-01",
        ack_by="oncall_user_a",
        note="triaged",
        raw_payload={"ticket_id": "PD-CASE-01"},
    )
    first = service.process_oncall_callback(callback)
    assert first.linked_notification_ids == [notification_id]
    assert first.acked_notifications == 1
    assert first.stored_events == 1

    still_unacked = service.list_notifications(only_unacked=True, limit=20)
    assert still_unacked == []

    second = service.process_oncall_callback(callback)
    assert second.linked_notification_ids == [notification_id]
    assert second.acked_notifications == 0

    oncall_events = service.list_oncall_events(provider="pagerduty", incident_id="INC-1001", limit=20)
    assert len(oncall_events) == 1
    assert oncall_events[0].notification_id == notification_id
    assert oncall_events[0].external_ticket_id == "PD-CASE-01"
    assert oncall_events[0].acked is True


def test_oncall_callback_signature_verify_and_mapping(tmp_path: Path) -> None:
    audit = AuditService(AuditStore(str(tmp_path / "audit.db")))
    dispatcher = FakeDispatcher()
    service = AlertService(
        store=AlertStore(str(tmp_path / "alert.db")),
        audit=audit,
        dispatcher=dispatcher,
        oncall_callback_signing_secret="cb-secret",
        oncall_callback_require_signature=True,
    )
    _ = service.create_subscription(
        AlertSubscriptionCreateRequest(
            name="sig-callback",
            owner="ops",
            event_types=["event_connector_sla_escalation"],
            min_severity=SignalLevel.WARNING,
            dedupe_window_sec=1,
            enabled=True,
            channel="oncall",
            channel_config={"webhooks": ["https://im.example.com/webhook/ops"]},
            escalation_chain=[
                AlertEscalationStage(
                    level_threshold=1,
                    channel="im",
                    targets=["https://im.example.com/webhook/ops"],
                    note="L1",
                )
            ],
        )
    )
    audit.log(
        "event_connector_sla_escalation",
        "level_1",
        {
            "connector_name": "ann_source_sig",
            "severity": "WARNING",
            "escalation_level": 1,
            "message": "pending backlog warning",
        },
        status="ERROR",
    )
    _ = service.sync_from_audit(limit=100)
    notifications = service.list_notifications(only_unacked=True, limit=20)
    assert len(notifications) == 1
    notification_id = notifications[0].id

    payload = {
        "incident": {"id": "INC-SIG-1", "status": "acknowledged"},
        "event_action": "acknowledged",
        "payload": {"custom_details": {"notification_id": notification_id}},
        "ticket_id": "PD-T-100",
    }
    timestamp = datetime.now(timezone.utc).isoformat()
    canonical = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    expected = hmac.new(
        b"cb-secret",
        f"{timestamp}|pagerduty|{canonical}".encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    result = service.process_oncall_callback(
        OncallCallbackRequest(
            provider="pagerduty",
            incident_id=None,
            status=None,
            mapping_template="pagerduty",
            timestamp=timestamp,
            signature=expected,
            raw_payload=payload,
            ack_by="oncall_sig",
        )
    )
    assert result.signature_checked is True
    assert result.signature_valid is True
    assert result.incident_id == "INC-SIG-1"
    assert result.acked_notifications == 1
    assert service.list_notifications(only_unacked=True, limit=20) == []

    failed = False
    try:
        _ = service.process_oncall_callback(
            OncallCallbackRequest(
                provider="pagerduty",
                incident_id="INC-SIG-1",
                status="acknowledged",
                timestamp=timestamp,
                signature="bad-signature",
                raw_payload=payload,
            )
        )
    except ValueError:
        failed = True
    assert failed is True


def test_oncall_reconcile_from_local_endpoint(tmp_path: Path) -> None:
    audit = AuditService(AuditStore(str(tmp_path / "audit.db")))
    dispatcher = FakeDispatcher()
    service = AlertService(
        store=AlertStore(str(tmp_path / "alert.db")),
        audit=audit,
        dispatcher=dispatcher,
    )
    _ = service.create_subscription(
        AlertSubscriptionCreateRequest(
            name="reconcile-callback",
            owner="ops",
            event_types=["event_connector_sla_escalation"],
            min_severity=SignalLevel.WARNING,
            dedupe_window_sec=1,
            enabled=True,
            channel="oncall",
            channel_config={"webhooks": ["https://im.example.com/webhook/ops"]},
            escalation_chain=[
                AlertEscalationStage(level_threshold=1, channel="im", targets=["https://im.example.com/webhook/ops"])
            ],
        )
    )
    audit.log(
        "event_connector_sla_escalation",
        "level_1",
        {
            "connector_name": "ann_source_reconcile",
            "severity": "WARNING",
            "escalation_level": 1,
            "message": "pending backlog warning",
        },
        status="ERROR",
    )
    _ = service.sync_from_audit(limit=100)
    notifications = service.list_notifications(only_unacked=True, limit=20)
    assert len(notifications) == 1
    notification_id = notifications[0].id

    _ = service.process_oncall_callback(
        OncallCallbackRequest(
            provider="pagerduty",
            incident_id="INC-RECON-1",
            status="triggered",
            notification_id=notification_id,
            note="seed-open-incident",
            raw_payload={"incident_id": "INC-RECON-1", "status": "triggered"},
        )
    )
    assert len(service.list_notifications(only_unacked=True, limit=20)) == 1

    remote_file = tmp_path / "oncall_remote.json"
    remote_file.write_text(
        json.dumps(
            {
                "incidents": [
                    {
                        "incident": {"id": "INC-RECON-1"},
                        "event_action": "resolved",
                        "ticket_id": "PD-T-200",
                    },
                    {
                        "incident": {"id": "INC-IGNORED"},
                        "event_action": "resolved",
                    },
                ]
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    result = service.reconcile_oncall(
        OncallReconcileRequest(
            provider="pagerduty",
            endpoint=remote_file.resolve().as_uri(),
            mapping_template="pagerduty",
            limit=100,
            dry_run=False,
        )
    )
    assert result.pulled == 2
    assert result.matched == 1
    assert result.callbacks == 1
    assert result.acked_notifications >= 1
    assert service.list_notifications(only_unacked=True, limit=20) == []


def test_alert_noise_reduction_rules(tmp_path: Path) -> None:
    audit = AuditService(AuditStore(str(tmp_path / "audit.db")))
    service = AlertService(store=AlertStore(str(tmp_path / "alert.db")), audit=audit)
    _ = service.create_subscription(
        AlertSubscriptionCreateRequest(
            name="noise-filter",
            owner="ops",
            event_types=["event_connector_sla"],
            min_severity=SignalLevel.WARNING,
            dedupe_window_sec=0,
            enabled=True,
            channel="inbox",
            channel_config={
                "noise_reduction": {
                    "min_repeat_count": 2,
                }
            },
        )
    )
    audit.log(
        "event_connector_sla",
        "freshness",
        {"connector_name": "c1", "severity": "WARNING", "repeat_count": 1, "message": "lag warning"},
    )
    first = service.sync_from_audit(limit=100)
    assert first == 0

    audit.log(
        "event_connector_sla",
        "freshness",
        {"connector_name": "c1", "severity": "WARNING", "repeat_count": 2, "message": "lag warning"},
    )
    second = service.sync_from_audit(limit=100)
    assert second == 1
