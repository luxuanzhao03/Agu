from pathlib import Path

from trading_assistant.alerts.service import AlertService
from trading_assistant.alerts.store import AlertStore
from trading_assistant.audit.service import AuditService
from trading_assistant.audit.store import AuditStore
from trading_assistant.core.models import AlertSubscriptionCreateRequest, SignalLevel


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
