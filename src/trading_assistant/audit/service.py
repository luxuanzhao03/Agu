from __future__ import annotations

import io
import csv
import json
import logging

from trading_assistant.audit.store import AuditStore
from trading_assistant.core.models import AuditChainVerifyResult, AuditEventCreate, AuditEventRecord

logger = logging.getLogger(__name__)


class AuditService:
    def __init__(self, store: AuditStore) -> None:
        self.store = store

    def log(self, event_type: str, action: str, payload: dict, status: str = "OK") -> int:
        try:
            event = AuditEventCreate(event_type=event_type, action=action, payload=payload, status=status)
            return self.store.write(event)
        except Exception as exc:  # noqa: BLE001
            logger.warning("Failed to write audit event %s/%s: %s", event_type, action, exc)
            return -1

    def query(self, event_type: str | None = None, limit: int = 100) -> list[AuditEventRecord]:
        return self.store.list_events(event_type=event_type, limit=limit)

    def export_csv(self, event_type: str | None = None, limit: int = 1000) -> str:
        rows = self.query(event_type=event_type, limit=limit)
        buf = io.StringIO()
        writer = csv.writer(buf)
        writer.writerow(["id", "event_time", "event_type", "action", "status", "payload"])
        for r in rows:
            writer.writerow(
                [
                    r.id,
                    r.event_time.isoformat(),
                    r.event_type,
                    r.action,
                    r.status,
                    json.dumps(r.payload, ensure_ascii=False),
                ]
            )
        return buf.getvalue()

    def export_jsonl(self, event_type: str | None = None, limit: int = 1000) -> str:
        rows = self.query(event_type=event_type, limit=limit)
        lines = []
        for r in rows:
            lines.append(
                json.dumps(
                    {
                        "id": r.id,
                        "event_time": r.event_time.isoformat(),
                        "event_type": r.event_type,
                        "action": r.action,
                        "status": r.status,
                        "payload": r.payload,
                    },
                    ensure_ascii=False,
                )
            )
        return "\n".join(lines)

    def verify_chain(self, limit: int = 5000) -> AuditChainVerifyResult:
        valid, broken_id, checked = self.store.verify_hash_chain(limit=limit)
        return AuditChainVerifyResult(
            valid=valid,
            checked_rows=checked,
            broken_event_id=broken_id,
            message="hash chain verified" if valid else "hash chain broken",
        )
