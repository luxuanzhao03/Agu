from pathlib import Path

from trading_assistant.audit.service import AuditService
from trading_assistant.audit.store import AuditStore


def test_audit_hash_chain_verify(tmp_path: Path) -> None:
    service = AuditService(AuditStore(str(tmp_path / "audit.db")))
    service.log("x", "a", {"k": 1})
    service.log("y", "b", {"k": 2})
    result = service.verify_chain(limit=100)
    assert result.valid is True
    assert result.checked_rows >= 2
