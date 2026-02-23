from __future__ import annotations

from pathlib import Path
import sqlite3

from trading_assistant.audit.service import AuditService
from trading_assistant.audit.store import AuditStore
import trading_assistant.core.container as container


def test_get_audit_service_fallbacks_to_recovered_copy_on_disk_io_error(tmp_path, monkeypatch) -> None:
    source = tmp_path / "audit.db"
    writer = AuditService(AuditStore(str(source)))
    event_id = writer.log(
        event_type="ops_sla",
        action="health_check",
        payload={"component": "ops_dashboard", "ok": True},
        status="OK",
    )
    assert event_id > 0

    recovered = source.with_name(f"{source.stem}_recovered{source.suffix}")

    monkeypatch.setenv("AUDIT_DB_PATH", str(source))
    container.get_settings.cache_clear()
    container.get_audit_service.cache_clear()

    real_factory = container.AuditStore

    def flaky_factory(db_path: str):
        if Path(db_path) == source:
            raise sqlite3.OperationalError("disk I/O error")
        return real_factory(db_path)

    monkeypatch.setattr(container, "AuditStore", flaky_factory)
    try:
        service = container.get_audit_service()
        rows = service.query(limit=10)
    finally:
        container.get_audit_service.cache_clear()
        container.get_settings.cache_clear()

    assert recovered.exists()
    assert len(rows) == 1
    assert rows[0].event_type == "ops_sla"
