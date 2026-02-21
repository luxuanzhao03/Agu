from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from trading_assistant.core.models import AuditEventCreate, AuditEventRecord


class AuditStore:
    def __init__(self, db_path: str) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_schema()

    def _conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_schema(self) -> None:
        with self._conn() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS audit_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_time TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    action TEXT NOT NULL,
                    status TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    prev_hash TEXT,
                    event_hash TEXT
                )
                """
            )
            cols = {str(r["name"]) for r in conn.execute("PRAGMA table_info(audit_events)").fetchall()}
            if "prev_hash" not in cols:
                conn.execute("ALTER TABLE audit_events ADD COLUMN prev_hash TEXT")
            if "event_hash" not in cols:
                conn.execute("ALTER TABLE audit_events ADD COLUMN event_hash TEXT")
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_audit_event_time ON audit_events(event_time DESC)"
            )
            conn.execute("CREATE INDEX IF NOT EXISTS idx_audit_event_type ON audit_events(event_type)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_audit_event_hash ON audit_events(event_hash)")

    def write(self, event: AuditEventCreate) -> int:
        now = datetime.now(timezone.utc).isoformat()
        payload = json.dumps(event.payload, ensure_ascii=False)
        with self._conn() as conn:
            prev = conn.execute(
                "SELECT event_hash FROM audit_events ORDER BY id DESC LIMIT 1"
            ).fetchone()
            prev_hash = str(prev["event_hash"]) if prev and prev["event_hash"] else ""
            raw = f"{prev_hash}|{now}|{event.event_type}|{event.action}|{event.status}|{payload}".encode("utf-8")
            event_hash = hashlib.sha256(raw).hexdigest()
            cur = conn.execute(
                """
                INSERT INTO audit_events(event_time, event_type, action, status, payload, prev_hash, event_hash)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (now, event.event_type, event.action, event.status, payload, prev_hash, event_hash),
            )
            return int(cur.lastrowid)

    def list_events(self, event_type: str | None = None, limit: int = 100) -> list[AuditEventRecord]:
        limit = max(1, min(limit, 1000))
        sql = """
            SELECT id, event_time, event_type, action, status, payload, prev_hash, event_hash
            FROM audit_events
        """
        params: list[str | int] = []
        if event_type:
            sql += " WHERE event_type = ? "
            params.append(event_type)
        sql += " ORDER BY id DESC LIMIT ? "
        params.append(limit)

        with self._conn() as conn:
            rows = conn.execute(sql, params).fetchall()
        return [
            AuditEventRecord(
                id=int(row["id"]),
                event_time=datetime.fromisoformat(row["event_time"]),
                event_type=str(row["event_type"]),
                action=str(row["action"]),
                status=str(row["status"]),
                payload=json.loads(str(row["payload"])),
                prev_hash=str(row["prev_hash"]) if row["prev_hash"] is not None else None,
                event_hash=str(row["event_hash"]) if row["event_hash"] is not None else None,
            )
            for row in rows
        ]

    def verify_hash_chain(self, limit: int = 5000) -> tuple[bool, int | None, int]:
        with self._conn() as conn:
            rows = conn.execute(
                """
                SELECT id, event_time, event_type, action, status, payload, prev_hash, event_hash
                FROM audit_events
                ORDER BY id ASC
                LIMIT ?
                """,
                (max(1, min(limit, 50000)),),
            ).fetchall()

        previous_hash = ""
        checked = 0
        for row in rows:
            payload = str(row["payload"])
            event_time = str(row["event_time"])
            event_type = str(row["event_type"])
            action = str(row["action"])
            status = str(row["status"])
            prev_hash = str(row["prev_hash"] or "")
            event_hash = str(row["event_hash"] or "")

            # Legacy rows before hash-chain migration.
            if not event_hash:
                checked += 1
                continue

            expected_raw = f"{previous_hash}|{event_time}|{event_type}|{action}|{status}|{payload}".encode("utf-8")
            expected_hash = hashlib.sha256(expected_raw).hexdigest()
            checked += 1
            if prev_hash != previous_hash or event_hash != expected_hash:
                return False, int(row["id"]), checked
            previous_hash = event_hash

        return True, None, checked
