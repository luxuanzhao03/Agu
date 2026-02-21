from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

from trading_assistant.core.models import (
    AlertDeliveryRecord,
    AlertDeliveryStatus,
    AlertEscalationStage,
    AlertNotificationRecord,
    AlertSubscriptionCreateRequest,
    AlertSubscriptionRecord,
    SignalLevel,
)


class AlertStore:
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
                CREATE TABLE IF NOT EXISTS alert_subscriptions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    name TEXT NOT NULL,
                    owner TEXT NOT NULL,
                    event_types TEXT NOT NULL,
                    min_severity TEXT NOT NULL,
                    dedupe_window_sec INTEGER NOT NULL,
                    enabled INTEGER NOT NULL,
                    channel TEXT NOT NULL
                )
                """
            )
            self._ensure_columns(
                conn=conn,
                table_name="alert_subscriptions",
                columns={
                    "channel_config": "TEXT NOT NULL DEFAULT '{}'",
                    "escalation_chain": "TEXT NOT NULL DEFAULT '[]'",
                    "runbook_url": "TEXT",
                },
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS alert_notifications (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    subscription_id INTEGER NOT NULL,
                    event_id INTEGER NOT NULL,
                    created_at TEXT NOT NULL,
                    severity TEXT NOT NULL,
                    source TEXT NOT NULL,
                    message TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    acked INTEGER NOT NULL DEFAULT 0,
                    acked_at TEXT,
                    dedupe_key TEXT NOT NULL,
                    FOREIGN KEY(subscription_id) REFERENCES alert_subscriptions(id)
                )
                """
            )
            conn.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS idx_alert_unique_event
                ON alert_notifications(subscription_id, event_id)
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_alert_lookup
                ON alert_notifications(subscription_id, created_at DESC)
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_alert_unacked
                ON alert_notifications(acked, created_at DESC)
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS alert_deliveries (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    notification_id INTEGER NOT NULL,
                    subscription_id INTEGER NOT NULL,
                    created_at TEXT NOT NULL,
                    channel TEXT NOT NULL,
                    target TEXT NOT NULL,
                    status TEXT NOT NULL,
                    error_message TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    FOREIGN KEY(notification_id) REFERENCES alert_notifications(id),
                    FOREIGN KEY(subscription_id) REFERENCES alert_subscriptions(id)
                )
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_alert_delivery_notification
                ON alert_deliveries(notification_id, created_at DESC)
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_alert_delivery_subscription
                ON alert_deliveries(subscription_id, created_at DESC)
                """
            )

    @staticmethod
    def _ensure_columns(
        *,
        conn: sqlite3.Connection,
        table_name: str,
        columns: dict[str, str],
    ) -> None:
        rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
        existing = {str(row["name"]) for row in rows}
        for col_name, col_type in columns.items():
            if col_name in existing:
                continue
            conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {col_name} {col_type}")

    def create_subscription(self, req: AlertSubscriptionCreateRequest) -> int:
        now = datetime.now(timezone.utc).isoformat()
        with self._conn() as conn:
            cur = conn.execute(
                """
                INSERT INTO alert_subscriptions(
                    created_at, updated_at, name, owner, event_types, min_severity,
                    dedupe_window_sec, enabled, channel, channel_config, escalation_chain, runbook_url
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    now,
                    now,
                    req.name,
                    req.owner,
                    json.dumps(req.event_types, ensure_ascii=False),
                    req.min_severity.value,
                    req.dedupe_window_sec,
                    1 if req.enabled else 0,
                    req.channel,
                    json.dumps(req.channel_config, ensure_ascii=False),
                    json.dumps([x.model_dump(mode="json") for x in req.escalation_chain], ensure_ascii=False),
                    req.runbook_url,
                ),
            )
            return int(cur.lastrowid)

    def list_subscriptions(
        self,
        owner: str | None = None,
        enabled_only: bool = False,
        limit: int = 200,
    ) -> list[AlertSubscriptionRecord]:
        sql = """
            SELECT
                id, created_at, updated_at, name, owner, event_types, min_severity, dedupe_window_sec, enabled, channel,
                channel_config, escalation_chain, runbook_url
            FROM alert_subscriptions
        """
        conditions: list[str] = []
        params: list[str | int] = []
        if owner:
            conditions.append("owner = ?")
            params.append(owner)
        if enabled_only:
            conditions.append("enabled = 1")
        if conditions:
            sql += " WHERE " + " AND ".join(conditions)
        sql += " ORDER BY id DESC LIMIT ?"
        params.append(max(1, min(limit, 1000)))
        with self._conn() as conn:
            rows = conn.execute(sql, params).fetchall()
        return [self._to_subscription(row) for row in rows]

    def exists_recent_notification(self, subscription_id: int, dedupe_key: str, window_sec: int) -> bool:
        if window_sec <= 0:
            return False
        threshold = (datetime.now(timezone.utc) - timedelta(seconds=window_sec)).isoformat()
        with self._conn() as conn:
            row = conn.execute(
                """
                SELECT 1
                FROM alert_notifications
                WHERE subscription_id = ? AND dedupe_key = ? AND created_at >= ?
                LIMIT 1
                """,
                (subscription_id, dedupe_key, threshold),
            ).fetchone()
        return row is not None

    def create_notification(
        self,
        subscription_id: int,
        event_id: int,
        severity: SignalLevel,
        source: str,
        message: str,
        payload: dict[str, str | int | float | bool | None],
        dedupe_key: str,
    ) -> int | None:
        now = datetime.now(timezone.utc).isoformat()
        with self._conn() as conn:
            try:
                cur = conn.execute(
                    """
                    INSERT INTO alert_notifications(
                        subscription_id, event_id, created_at, severity, source, message, payload, acked, acked_at, dedupe_key
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, 0, NULL, ?)
                    """,
                    (
                        subscription_id,
                        event_id,
                        now,
                        severity.value,
                        source,
                        message,
                        json.dumps(payload, ensure_ascii=False),
                        dedupe_key,
                    ),
                )
            except sqlite3.IntegrityError:
                return None
            return int(cur.lastrowid)

    def list_notifications(
        self,
        subscription_id: int | None = None,
        only_unacked: bool = False,
        limit: int = 200,
    ) -> list[AlertNotificationRecord]:
        sql = """
            SELECT id, subscription_id, event_id, created_at, severity, source, message, payload, acked, acked_at
            FROM alert_notifications
        """
        conditions: list[str] = []
        params: list[int] = []
        if subscription_id is not None:
            conditions.append("subscription_id = ?")
            params.append(subscription_id)
        if only_unacked:
            conditions.append("acked = 0")
        if conditions:
            sql += " WHERE " + " AND ".join(conditions)
        sql += " ORDER BY created_at DESC LIMIT ?"
        params.append(max(1, min(limit, 2000)))
        with self._conn() as conn:
            rows = conn.execute(sql, params).fetchall()
        return [self._to_notification(row) for row in rows]

    def count_notifications(
        self,
        *,
        only_unacked: bool = False,
        severity: SignalLevel | None = None,
    ) -> int:
        sql = "SELECT COUNT(1) AS c FROM alert_notifications"
        conditions: list[str] = []
        params: list[str | int] = []
        if only_unacked:
            conditions.append("acked = 0")
        if severity is not None:
            conditions.append("severity = ?")
            params.append(severity.value)
        if conditions:
            sql += " WHERE " + " AND ".join(conditions)
        with self._conn() as conn:
            row = conn.execute(sql, params).fetchone()
        return int(row["c"]) if row is not None else 0

    def ack_notification(self, notification_id: int) -> bool:
        now = datetime.now(timezone.utc).isoformat()
        with self._conn() as conn:
            cur = conn.execute(
                """
                UPDATE alert_notifications
                SET acked = 1, acked_at = ?
                WHERE id = ? AND acked = 0
                """,
                (now, notification_id),
            )
        return cur.rowcount > 0

    def create_delivery(
        self,
        *,
        notification_id: int,
        subscription_id: int,
        channel: str,
        target: str,
        status: AlertDeliveryStatus,
        error_message: str = "",
        payload: dict[str, object] | None = None,
    ) -> int:
        now = datetime.now(timezone.utc).isoformat()
        with self._conn() as conn:
            cur = conn.execute(
                """
                INSERT INTO alert_deliveries(
                    notification_id, subscription_id, created_at, channel, target, status, error_message, payload
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    notification_id,
                    subscription_id,
                    now,
                    channel,
                    target,
                    status.value,
                    error_message,
                    json.dumps(payload or {}, ensure_ascii=False),
                ),
            )
        return int(cur.lastrowid)

    def list_deliveries(
        self,
        *,
        notification_id: int | None = None,
        subscription_id: int | None = None,
        status: AlertDeliveryStatus | None = None,
        limit: int = 200,
    ) -> list[AlertDeliveryRecord]:
        sql = """
            SELECT
                id, notification_id, subscription_id, created_at, channel, target, status, error_message, payload
            FROM alert_deliveries
        """
        conditions: list[str] = []
        params: list[str | int] = []
        if notification_id is not None:
            conditions.append("notification_id = ?")
            params.append(notification_id)
        if subscription_id is not None:
            conditions.append("subscription_id = ?")
            params.append(subscription_id)
        if status is not None:
            conditions.append("status = ?")
            params.append(status.value)
        if conditions:
            sql += " WHERE " + " AND ".join(conditions)
        sql += " ORDER BY created_at DESC, id DESC LIMIT ?"
        params.append(max(1, min(limit, 5000)))
        with self._conn() as conn:
            rows = conn.execute(sql, params).fetchall()
        return [self._to_delivery(row) for row in rows]

    def _to_subscription(self, row: sqlite3.Row) -> AlertSubscriptionRecord:
        raw_chain = row["escalation_chain"] if "escalation_chain" in row.keys() else "[]"
        chain_items = json.loads(str(raw_chain or "[]"))
        escalation_chain = [AlertEscalationStage.model_validate(item) for item in chain_items]
        raw_cfg = row["channel_config"] if "channel_config" in row.keys() else "{}"
        channel_config = dict(json.loads(str(raw_cfg or "{}")))
        return AlertSubscriptionRecord(
            id=int(row["id"]),
            created_at=datetime.fromisoformat(str(row["created_at"])),
            updated_at=datetime.fromisoformat(str(row["updated_at"])),
            name=str(row["name"]),
            owner=str(row["owner"]),
            event_types=list(json.loads(str(row["event_types"]))),
            min_severity=SignalLevel(str(row["min_severity"])),
            dedupe_window_sec=int(row["dedupe_window_sec"]),
            enabled=bool(int(row["enabled"])),
            channel=str(row["channel"]),
            channel_config=channel_config,
            escalation_chain=escalation_chain,
            runbook_url=str(row["runbook_url"]) if row["runbook_url"] else None,
        )

    def _to_notification(self, row: sqlite3.Row) -> AlertNotificationRecord:
        return AlertNotificationRecord(
            id=int(row["id"]),
            subscription_id=int(row["subscription_id"]),
            event_id=int(row["event_id"]),
            created_at=datetime.fromisoformat(str(row["created_at"])),
            severity=SignalLevel(str(row["severity"])),
            source=str(row["source"]),
            message=str(row["message"]),
            payload=dict(json.loads(str(row["payload"]))),
            acked=bool(int(row["acked"])),
            acked_at=datetime.fromisoformat(str(row["acked_at"])) if row["acked_at"] else None,
        )

    def _to_delivery(self, row: sqlite3.Row) -> AlertDeliveryRecord:
        return AlertDeliveryRecord(
            id=int(row["id"]),
            notification_id=int(row["notification_id"]),
            subscription_id=int(row["subscription_id"]),
            created_at=datetime.fromisoformat(str(row["created_at"])),
            channel=str(row["channel"]),
            target=str(row["target"]),
            status=AlertDeliveryStatus(str(row["status"])),
            error_message=str(row["error_message"] or ""),
            payload=dict(json.loads(str(row["payload"] or "{}"))),
        )
