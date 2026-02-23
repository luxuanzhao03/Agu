from pathlib import Path

from trading_assistant.audit.store import AuditStore
from trading_assistant.core.models import AuditEventCreate


def test_audit_store_write_and_query(tmp_path: Path) -> None:
    db = tmp_path / "audit.db"
    store = AuditStore(str(db))
    event_id = store.write(
        AuditEventCreate(
            event_type="unit_test",
            action="write",
            payload={"k": "v"},
            status="OK",
        )
    )
    assert event_id > 0

    rows = store.list_events(event_type="unit_test", limit=10)
    assert len(rows) == 1
    assert rows[0].payload["k"] == "v"


def test_audit_store_accepts_nested_payload(tmp_path: Path) -> None:
    db = tmp_path / "audit_nested.db"
    store = AuditStore(str(db))
    payload = {
        "run_status": "PARTIAL_FAILED",
        "failed_strategies": ["mean_reversion"],
        "rollout_plan": {"enabled": True, "gray_days": 10},
    }
    event_id = store.write(
        AuditEventCreate(
            event_type="strategy_challenge",
            action="run",
            payload=payload,
            status="OK",
        )
    )
    assert event_id > 0
    rows = store.list_events(event_type="strategy_challenge", limit=10)
    assert len(rows) == 1
    assert rows[0].payload["run_status"] == "PARTIAL_FAILED"
    assert rows[0].payload["failed_strategies"] == ["mean_reversion"]
    assert rows[0].payload["rollout_plan"]["enabled"] is True
