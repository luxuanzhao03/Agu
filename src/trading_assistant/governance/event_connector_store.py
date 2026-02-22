from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

from trading_assistant.core.models import (
    EventConnectorCheckpointRecord,
    EventConnectorFailureRecord,
    EventConnectorFailureStatus,
    EventConnectorSLAAlertStateRecord,
    EventConnectorSLABreach,
    EventConnectorOverviewItem,
    EventConnectorRecord,
    EventConnectorRegisterRequest,
    EventConnectorRunRecord,
    EventConnectorRunStatus,
    EventConnectorSourceStateRecord,
    EventConnectorType,
    EventCoverageDailyPoint,
    EventCoverageSourceItem,
    EventOpsCoverageSummary,
)


def _to_iso(dt: datetime | None) -> str | None:
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc).isoformat()
    return dt.astimezone(timezone.utc).isoformat()


def _from_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(value)


class EventConnectorStore:
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
                CREATE TABLE IF NOT EXISTS event_connectors (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    connector_name TEXT NOT NULL UNIQUE,
                    source_name TEXT NOT NULL,
                    connector_type TEXT NOT NULL,
                    enabled INTEGER NOT NULL,
                    fetch_limit INTEGER NOT NULL,
                    poll_interval_minutes INTEGER NOT NULL,
                    replay_backoff_seconds INTEGER NOT NULL,
                    max_retry INTEGER NOT NULL,
                    config TEXT NOT NULL,
                    created_by TEXT NOT NULL,
                    note TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS event_connector_checkpoints (
                    connector_name TEXT PRIMARY KEY,
                    checkpoint_cursor TEXT,
                    checkpoint_publish_time TEXT,
                    updated_at TEXT NOT NULL,
                    last_run_at TEXT,
                    last_success_at TEXT
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS event_connector_runs (
                    run_id TEXT PRIMARY KEY,
                    connector_name TEXT NOT NULL,
                    source_name TEXT NOT NULL,
                    started_at TEXT NOT NULL,
                    finished_at TEXT,
                    status TEXT NOT NULL,
                    triggered_by TEXT NOT NULL,
                    pulled_count INTEGER NOT NULL,
                    normalized_count INTEGER NOT NULL,
                    inserted_count INTEGER NOT NULL,
                    updated_count INTEGER NOT NULL,
                    failed_count INTEGER NOT NULL,
                    replayed_count INTEGER NOT NULL,
                    checkpoint_before TEXT,
                    checkpoint_after TEXT,
                    error_message TEXT,
                    details TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS event_connector_failures (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    connector_name TEXT NOT NULL,
                    source_name TEXT NOT NULL,
                    run_id TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    status TEXT NOT NULL,
                    retry_count INTEGER NOT NULL,
                    next_retry_at TEXT,
                    last_error TEXT NOT NULL,
                    payload TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_event_connector_runs_conn_time
                ON event_connector_runs(connector_name, started_at DESC)
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_event_connector_failures_lookup
                ON event_connector_failures(connector_name, status, next_retry_at)
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS event_connector_sla_alert_states (
                    dedupe_key TEXT PRIMARY KEY,
                    connector_name TEXT NOT NULL,
                    source_name TEXT NOT NULL,
                    breach_type TEXT NOT NULL,
                    stage TEXT NOT NULL,
                    severity TEXT NOT NULL,
                    first_seen_at TEXT NOT NULL,
                    last_seen_at TEXT NOT NULL,
                    last_emitted_at TEXT,
                    last_recovered_at TEXT,
                    last_escalated_at TEXT,
                    repeat_count INTEGER NOT NULL,
                    escalation_level INTEGER NOT NULL,
                    escalation_reason TEXT NOT NULL,
                    is_open INTEGER NOT NULL,
                    message TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_event_connector_sla_states_lookup
                ON event_connector_sla_alert_states(connector_name, is_open, last_seen_at DESC)
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS event_connector_sla_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    observed_at TEXT NOT NULL,
                    connector_name TEXT NOT NULL,
                    source_name TEXT NOT NULL,
                    breach_type TEXT NOT NULL,
                    severity TEXT NOT NULL,
                    stage TEXT NOT NULL,
                    freshness_minutes INTEGER,
                    pending_failures INTEGER NOT NULL,
                    dead_failures INTEGER NOT NULL,
                    message TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_event_connector_sla_history_lookup
                ON event_connector_sla_history(connector_name, observed_at DESC)
                """
            )
            self._ensure_columns(
                conn=conn,
                table_name="event_connector_sla_alert_states",
                columns={
                    "last_escalated_at": "TEXT",
                    "escalation_level": "INTEGER NOT NULL DEFAULT 0",
                    "escalation_reason": "TEXT NOT NULL DEFAULT ''",
                },
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS event_connector_source_states (
                    connector_name TEXT NOT NULL,
                    source_key TEXT NOT NULL,
                    connector_type TEXT NOT NULL,
                    priority INTEGER NOT NULL,
                    enabled INTEGER NOT NULL,
                    health_score REAL NOT NULL,
                    consecutive_failures INTEGER NOT NULL,
                    total_success INTEGER NOT NULL,
                    total_failures INTEGER NOT NULL,
                    last_latency_ms INTEGER,
                    last_error TEXT NOT NULL,
                    last_attempt_at TEXT,
                    last_success_at TEXT,
                    last_failure_at TEXT,
                    checkpoint_cursor TEXT,
                    checkpoint_publish_time TEXT,
                    is_active INTEGER NOT NULL,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY(connector_name, source_key)
                )
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_event_connector_source_states_lookup
                ON event_connector_source_states(connector_name, enabled, is_active, priority ASC)
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS event_connector_source_budgets (
                    connector_name TEXT NOT NULL,
                    source_key TEXT NOT NULL,
                    window_hour TEXT NOT NULL,
                    request_count INTEGER NOT NULL,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY(connector_name, source_key, window_hour)
                )
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_event_connector_source_budgets_lookup
                ON event_connector_source_budgets(connector_name, source_key, window_hour DESC)
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS event_connector_source_credentials (
                    connector_name TEXT NOT NULL,
                    source_key TEXT NOT NULL,
                    cursor INTEGER NOT NULL,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY(connector_name, source_key)
                )
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

    def register_connector(self, req: EventConnectorRegisterRequest) -> int:
        now = datetime.now(timezone.utc).isoformat()
        with self._conn() as conn:
            conn.execute(
                """
                INSERT INTO event_connectors(
                    created_at, updated_at, connector_name, source_name, connector_type, enabled, fetch_limit,
                    poll_interval_minutes, replay_backoff_seconds, max_retry, config, created_by, note
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(connector_name) DO UPDATE SET
                    updated_at = excluded.updated_at,
                    source_name = excluded.source_name,
                    connector_type = excluded.connector_type,
                    enabled = excluded.enabled,
                    fetch_limit = excluded.fetch_limit,
                    poll_interval_minutes = excluded.poll_interval_minutes,
                    replay_backoff_seconds = excluded.replay_backoff_seconds,
                    max_retry = excluded.max_retry,
                    config = excluded.config,
                    created_by = excluded.created_by,
                    note = excluded.note
                """,
                (
                    now,
                    now,
                    req.connector_name,
                    req.source_name,
                    req.connector_type.value,
                    1 if req.enabled else 0,
                    req.fetch_limit,
                    req.poll_interval_minutes,
                    req.replay_backoff_seconds,
                    req.max_retry,
                    json.dumps(req.config, ensure_ascii=False),
                    req.created_by,
                    req.note,
                ),
            )
            row = conn.execute(
                "SELECT id FROM event_connectors WHERE connector_name = ? LIMIT 1",
                (req.connector_name,),
            ).fetchone()
            conn.execute(
                """
                INSERT INTO event_connector_checkpoints(
                    connector_name, checkpoint_cursor, checkpoint_publish_time, updated_at, last_run_at, last_success_at
                )
                VALUES (?, ?, ?, ?, NULL, NULL)
                ON CONFLICT(connector_name) DO UPDATE SET
                    checkpoint_cursor = COALESCE(excluded.checkpoint_cursor, event_connector_checkpoints.checkpoint_cursor),
                    checkpoint_publish_time = COALESCE(
                        excluded.checkpoint_publish_time,
                        event_connector_checkpoints.checkpoint_publish_time
                    ),
                    updated_at = excluded.updated_at
                """,
                (
                    req.connector_name,
                    req.checkpoint_cursor,
                    _to_iso(req.checkpoint_publish_time),
                    now,
                ),
            )
        return int(row["id"]) if row else -1

    def get_connector(self, connector_name: str) -> EventConnectorRecord | None:
        with self._conn() as conn:
            row = conn.execute(
                """
                SELECT
                    id, created_at, updated_at, connector_name, source_name, connector_type, enabled, fetch_limit,
                    poll_interval_minutes, replay_backoff_seconds, max_retry, config, created_by, note
                FROM event_connectors
                WHERE connector_name = ?
                LIMIT 1
                """,
                (connector_name,),
            ).fetchone()
        return self._to_connector(row) if row else None

    def list_connectors(self, limit: int = 200, enabled_only: bool = False) -> list[EventConnectorRecord]:
        sql = """
            SELECT
                id, created_at, updated_at, connector_name, source_name, connector_type, enabled, fetch_limit,
                poll_interval_minutes, replay_backoff_seconds, max_retry, config, created_by, note
            FROM event_connectors
        """
        params: list[int] = []
        if enabled_only:
            sql += " WHERE enabled = 1"
        sql += " ORDER BY id DESC LIMIT ?"
        params.append(max(1, min(limit, 2000)))
        with self._conn() as conn:
            rows = conn.execute(sql, params).fetchall()
        return [self._to_connector(row) for row in rows]

    def upsert_source_state(
        self,
        *,
        connector_name: str,
        source_key: str,
        connector_type: EventConnectorType,
        priority: int,
        enabled: bool,
        default_health: float = 100.0,
    ) -> None:
        now = datetime.now(timezone.utc).isoformat()
        with self._conn() as conn:
            conn.execute(
                """
                INSERT INTO event_connector_source_states(
                    connector_name, source_key, connector_type, priority, enabled, health_score,
                    consecutive_failures, total_success, total_failures, last_latency_ms, last_error,
                    last_attempt_at, last_success_at, last_failure_at, checkpoint_cursor, checkpoint_publish_time,
                    is_active, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, 0, 0, 0, NULL, '', NULL, NULL, NULL, NULL, NULL, 0, ?)
                ON CONFLICT(connector_name, source_key) DO UPDATE SET
                    connector_type = excluded.connector_type,
                    priority = excluded.priority,
                    enabled = excluded.enabled,
                    updated_at = excluded.updated_at
                """,
                (
                    connector_name,
                    source_key,
                    connector_type.value,
                    max(0, priority),
                    1 if enabled else 0,
                    max(0.0, min(default_health, 100.0)),
                    now,
                ),
            )

    def list_source_states(
        self,
        *,
        connector_name: str | None = None,
        limit: int = 500,
    ) -> list[EventConnectorSourceStateRecord]:
        sql = """
            SELECT
                connector_name, source_key, connector_type, priority, enabled, health_score,
                consecutive_failures, total_success, total_failures, last_latency_ms, last_error,
                last_attempt_at, last_success_at, last_failure_at, checkpoint_cursor, checkpoint_publish_time,
                is_active, updated_at
            FROM event_connector_source_states
        """
        params: list[str | int] = []
        if connector_name:
            sql += " WHERE connector_name = ?"
            params.append(connector_name)
        sql += " ORDER BY connector_name ASC, is_active DESC, enabled DESC, priority ASC, source_key ASC LIMIT ?"
        params.append(max(1, min(limit, 5000)))
        with self._conn() as conn:
            rows = conn.execute(sql, params).fetchall()
        return [self._to_source_state(row) for row in rows]

    def get_source_state(self, *, connector_name: str, source_key: str) -> EventConnectorSourceStateRecord | None:
        with self._conn() as conn:
            row = conn.execute(
                """
                SELECT
                    connector_name, source_key, connector_type, priority, enabled, health_score,
                    consecutive_failures, total_success, total_failures, last_latency_ms, last_error,
                    last_attempt_at, last_success_at, last_failure_at, checkpoint_cursor, checkpoint_publish_time,
                    is_active, updated_at
                FROM event_connector_source_states
                WHERE connector_name = ? AND source_key = ?
                LIMIT 1
                """,
                (connector_name, source_key),
            ).fetchone()
        return self._to_source_state(row) if row else None

    def mark_source_attempt_success(
        self,
        *,
        connector_name: str,
        source_key: str,
        checkpoint_cursor: str | None,
        checkpoint_publish_time: datetime | None,
        latency_ms: int | None,
    ) -> EventConnectorSourceStateRecord | None:
        now = datetime.now(timezone.utc)
        current = self.get_source_state(connector_name=connector_name, source_key=source_key)
        if current is None:
            return None
        latency_penalty = 0.0
        if latency_ms is not None and latency_ms > 0:
            latency_penalty = min(6.0, float(latency_ms) / 2000.0)
        next_health = min(100.0, max(35.0, current.health_score) + 8.0 - latency_penalty)
        with self._conn() as conn:
            conn.execute(
                """
                UPDATE event_connector_source_states
                SET
                    health_score = ?,
                    consecutive_failures = 0,
                    total_success = total_success + 1,
                    last_latency_ms = ?,
                    last_error = '',
                    last_attempt_at = ?,
                    last_success_at = ?,
                    checkpoint_cursor = ?,
                    checkpoint_publish_time = ?,
                    is_active = CASE WHEN enabled = 1 THEN 1 ELSE 0 END,
                    updated_at = ?
                WHERE connector_name = ? AND source_key = ?
                """,
                (
                    next_health,
                    latency_ms,
                    _to_iso(now),
                    _to_iso(now),
                    checkpoint_cursor,
                    _to_iso(checkpoint_publish_time),
                    _to_iso(now),
                    connector_name,
                    source_key,
                ),
            )
            conn.execute(
                """
                UPDATE event_connector_source_states
                SET is_active = 0
                WHERE connector_name = ? AND source_key <> ?
                """,
                (connector_name, source_key),
            )
        return self.get_source_state(connector_name=connector_name, source_key=source_key)

    def mark_source_attempt_failure(
        self,
        *,
        connector_name: str,
        source_key: str,
        error_message: str,
        latency_ms: int | None,
    ) -> EventConnectorSourceStateRecord | None:
        now = datetime.now(timezone.utc)
        current = self.get_source_state(connector_name=connector_name, source_key=source_key)
        if current is None:
            return None
        next_failures = current.consecutive_failures + 1
        penalty = 12.0 + min(30.0, float(next_failures) * 4.0)
        if latency_ms is not None and latency_ms > 5000:
            penalty += min(15.0, float(latency_ms - 5000) / 1000.0)
        next_health = max(0.0, current.health_score - penalty)
        with self._conn() as conn:
            conn.execute(
                """
                UPDATE event_connector_source_states
                SET
                    health_score = ?,
                    consecutive_failures = ?,
                    total_failures = total_failures + 1,
                    last_latency_ms = ?,
                    last_error = ?,
                    last_attempt_at = ?,
                    last_failure_at = ?,
                    is_active = 0,
                    updated_at = ?
                WHERE connector_name = ? AND source_key = ?
                """,
                (
                    next_health,
                    next_failures,
                    latency_ms,
                    error_message[:500],
                    _to_iso(now),
                    _to_iso(now),
                    _to_iso(now),
                    connector_name,
                    source_key,
                ),
            )
        return self.get_source_state(connector_name=connector_name, source_key=source_key)

    def try_consume_source_budget(
        self,
        *,
        connector_name: str,
        source_key: str,
        budget_per_hour: int | None,
        as_of: datetime | None = None,
    ) -> tuple[bool, int, int, str]:
        now = as_of or datetime.now(timezone.utc)
        window = now.replace(minute=0, second=0, microsecond=0).isoformat()
        budget = max(0, int(budget_per_hour or 0))
        if budget <= 0:
            return True, 0, 0, window

        with self._conn() as conn:
            row = conn.execute(
                """
                SELECT request_count
                FROM event_connector_source_budgets
                WHERE connector_name = ? AND source_key = ? AND window_hour = ?
                LIMIT 1
                """,
                (connector_name, source_key, window),
            ).fetchone()
            if row is None:
                conn.execute(
                    """
                    INSERT INTO event_connector_source_budgets(
                        connector_name, source_key, window_hour, request_count, updated_at
                    )
                    VALUES (?, ?, ?, 1, ?)
                    """,
                    (connector_name, source_key, window, _to_iso(now)),
                )
                return True, 1, budget, window

            used = int(row["request_count"] or 0)
            if used >= budget:
                return False, used, budget, window
            next_used = used + 1
            conn.execute(
                """
                UPDATE event_connector_source_budgets
                SET request_count = ?, updated_at = ?
                WHERE connector_name = ? AND source_key = ? AND window_hour = ?
                """,
                (next_used, _to_iso(now), connector_name, source_key, window),
            )
            return True, next_used, budget, window

    def next_source_credential_alias(
        self,
        *,
        connector_name: str,
        source_key: str,
        aliases: list[str],
    ) -> str | None:
        cleaned = [x.strip() for x in aliases if x and x.strip()]
        if not cleaned:
            return None

        now = datetime.now(timezone.utc)
        with self._conn() as conn:
            row = conn.execute(
                """
                SELECT cursor
                FROM event_connector_source_credentials
                WHERE connector_name = ? AND source_key = ?
                LIMIT 1
                """,
                (connector_name, source_key),
            ).fetchone()
            cursor = int(row["cursor"]) if row is not None else -1
            next_cursor = (cursor + 1) % len(cleaned)
            conn.execute(
                """
                INSERT INTO event_connector_source_credentials(
                    connector_name, source_key, cursor, updated_at
                )
                VALUES (?, ?, ?, ?)
                ON CONFLICT(connector_name, source_key) DO UPDATE SET
                    cursor = excluded.cursor,
                    updated_at = excluded.updated_at
                """,
                (connector_name, source_key, next_cursor, _to_iso(now)),
            )
        return cleaned[next_cursor]

    def get_checkpoint(self, connector_name: str) -> EventConnectorCheckpointRecord | None:
        with self._conn() as conn:
            row = conn.execute(
                """
                SELECT connector_name, checkpoint_cursor, checkpoint_publish_time, updated_at, last_run_at, last_success_at
                FROM event_connector_checkpoints
                WHERE connector_name = ?
                LIMIT 1
                """,
                (connector_name,),
            ).fetchone()
        return self._to_checkpoint(row) if row else None

    def update_checkpoint(
        self,
        connector_name: str,
        *,
        checkpoint_cursor: str | None,
        checkpoint_publish_time: datetime | None,
        mark_run_at: datetime | None = None,
        mark_success_at: datetime | None = None,
    ) -> None:
        now = datetime.now(timezone.utc).isoformat()
        with self._conn() as conn:
            conn.execute(
                """
                INSERT INTO event_connector_checkpoints(
                    connector_name, checkpoint_cursor, checkpoint_publish_time, updated_at, last_run_at, last_success_at
                )
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(connector_name) DO UPDATE SET
                    checkpoint_cursor = excluded.checkpoint_cursor,
                    checkpoint_publish_time = excluded.checkpoint_publish_time,
                    updated_at = excluded.updated_at,
                    last_run_at = COALESCE(excluded.last_run_at, event_connector_checkpoints.last_run_at),
                    last_success_at = COALESCE(excluded.last_success_at, event_connector_checkpoints.last_success_at)
                """,
                (
                    connector_name,
                    checkpoint_cursor,
                    _to_iso(checkpoint_publish_time),
                    now,
                    _to_iso(mark_run_at),
                    _to_iso(mark_success_at),
                ),
            )

    def create_run(
        self,
        run: EventConnectorRunRecord,
    ) -> None:
        with self._conn() as conn:
            conn.execute(
                """
                INSERT INTO event_connector_runs(
                    run_id, connector_name, source_name, started_at, finished_at, status, triggered_by, pulled_count,
                    normalized_count, inserted_count, updated_count, failed_count, replayed_count, checkpoint_before,
                    checkpoint_after, error_message, details
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run.run_id,
                    run.connector_name,
                    run.source_name,
                    _to_iso(run.started_at),
                    _to_iso(run.finished_at),
                    run.status.value,
                    run.triggered_by,
                    run.pulled_count,
                    run.normalized_count,
                    run.inserted_count,
                    run.updated_count,
                    run.failed_count,
                    run.replayed_count,
                    run.checkpoint_before,
                    run.checkpoint_after,
                    run.error_message,
                    json.dumps(run.details, ensure_ascii=False),
                ),
            )

    def update_run(self, run: EventConnectorRunRecord) -> None:
        with self._conn() as conn:
            conn.execute(
                """
                UPDATE event_connector_runs
                SET
                    finished_at = ?,
                    status = ?,
                    pulled_count = ?,
                    normalized_count = ?,
                    inserted_count = ?,
                    updated_count = ?,
                    failed_count = ?,
                    replayed_count = ?,
                    checkpoint_before = ?,
                    checkpoint_after = ?,
                    error_message = ?,
                    details = ?
                WHERE run_id = ?
                """,
                (
                    _to_iso(run.finished_at),
                    run.status.value,
                    run.pulled_count,
                    run.normalized_count,
                    run.inserted_count,
                    run.updated_count,
                    run.failed_count,
                    run.replayed_count,
                    run.checkpoint_before,
                    run.checkpoint_after,
                    run.error_message,
                    json.dumps(run.details, ensure_ascii=False),
                    run.run_id,
                ),
            )

    def list_runs(self, connector_name: str | None = None, limit: int = 200) -> list[EventConnectorRunRecord]:
        sql = """
            SELECT
                run_id, connector_name, source_name, started_at, finished_at, status, triggered_by, pulled_count,
                normalized_count, inserted_count, updated_count, failed_count, replayed_count, checkpoint_before,
                checkpoint_after, error_message, details
            FROM event_connector_runs
        """
        params: list[str | int] = []
        if connector_name:
            sql += " WHERE connector_name = ?"
            params.append(connector_name)
        sql += " ORDER BY started_at DESC LIMIT ?"
        params.append(max(1, min(limit, 500000)))
        with self._conn() as conn:
            rows = conn.execute(sql, params).fetchall()
        return [self._to_run(row) for row in rows]

    def latest_run(self, connector_name: str) -> EventConnectorRunRecord | None:
        rows = self.list_runs(connector_name=connector_name, limit=1)
        return rows[0] if rows else None

    def append_failures(
        self,
        connector_name: str,
        source_name: str,
        run_id: str,
        payloads: list[dict],
        *,
        error_message: str,
        retry_count: int = 0,
        next_retry_at: datetime | None = None,
    ) -> int:
        if not payloads:
            return 0
        now = datetime.now(timezone.utc).isoformat()
        inserted = 0
        with self._conn() as conn:
            for payload in payloads:
                conn.execute(
                    """
                    INSERT INTO event_connector_failures(
                        connector_name, source_name, run_id, created_at, updated_at, status, retry_count, next_retry_at,
                        last_error, payload
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        connector_name,
                        source_name,
                        run_id,
                        now,
                        now,
                        EventConnectorFailureStatus.PENDING.value,
                        max(0, retry_count),
                        _to_iso(next_retry_at),
                        error_message,
                        json.dumps(payload, ensure_ascii=False),
                    ),
                )
                inserted += 1
        return inserted

    def list_failures(
        self,
        connector_name: str | None = None,
        status: EventConnectorFailureStatus | None = None,
        error_keyword: str | None = None,
        limit: int = 200,
    ) -> list[EventConnectorFailureRecord]:
        sql = """
            SELECT
                id, connector_name, source_name, run_id, created_at, updated_at, status, retry_count, next_retry_at,
                last_error, payload
            FROM event_connector_failures
        """
        conditions: list[str] = []
        params: list[str | int] = []
        if connector_name:
            conditions.append("connector_name = ?")
            params.append(connector_name)
        if status:
            conditions.append("status = ?")
            params.append(status.value)
        if error_keyword:
            conditions.append("LOWER(last_error) LIKE ?")
            params.append(f"%{error_keyword.lower()}%")
        if conditions:
            sql += " WHERE " + " AND ".join(conditions)
        sql += " ORDER BY id DESC LIMIT ?"
        params.append(max(1, min(limit, 5000)))
        with self._conn() as conn:
            rows = conn.execute(sql, params).fetchall()
        return [self._to_failure(row) for row in rows]

    def get_failure(self, row_id: int) -> EventConnectorFailureRecord | None:
        with self._conn() as conn:
            row = conn.execute(
                """
                SELECT
                    id, connector_name, source_name, run_id, created_at, updated_at, status, retry_count, next_retry_at,
                    last_error, payload
                FROM event_connector_failures
                WHERE id = ?
                LIMIT 1
                """,
                (row_id,),
            ).fetchone()
        return self._to_failure(row) if row else None

    def update_failure_payload(
        self,
        row_id: int,
        *,
        payload: dict,
        last_error: str,
        status: EventConnectorFailureStatus = EventConnectorFailureStatus.PENDING,
        next_retry_at: datetime | None = None,
        reset_retry_count: bool = False,
    ) -> bool:
        now = datetime.now(timezone.utc).isoformat()
        with self._conn() as conn:
            cur = conn.execute(
                """
                UPDATE event_connector_failures
                SET
                    updated_at = ?,
                    status = ?,
                    next_retry_at = ?,
                    last_error = ?,
                    payload = ?,
                    retry_count = CASE WHEN ? = 1 THEN 0 ELSE retry_count END
                WHERE id = ?
                """,
                (
                    now,
                    status.value,
                    _to_iso(next_retry_at),
                    last_error,
                    json.dumps(payload, ensure_ascii=False),
                    1 if reset_retry_count else 0,
                    row_id,
                ),
            )
        return cur.rowcount > 0

    def claim_failures_by_ids(
        self,
        *,
        connector_name: str,
        failure_ids: list[int],
    ) -> list[EventConnectorFailureRecord]:
        ids = sorted(set(int(x) for x in failure_ids if int(x) > 0))
        if not ids:
            return []
        placeholders = ", ".join(["?"] * len(ids))
        sql = f"""
            SELECT
                id, connector_name, source_name, run_id, created_at, updated_at, status, retry_count, next_retry_at,
                last_error, payload
            FROM event_connector_failures
            WHERE connector_name = ? AND id IN ({placeholders})
            ORDER BY id ASC
        """
        params: list[str | int] = [connector_name]
        params.extend(ids)
        with self._conn() as conn:
            rows = conn.execute(sql, params).fetchall()
        return [self._to_failure(row) for row in rows]

    def claim_pending_failures(
        self,
        connector_name: str,
        limit: int,
        max_retry: int,
        as_of: datetime | None = None,
    ) -> list[EventConnectorFailureRecord]:
        now = as_of or datetime.now(timezone.utc)
        with self._conn() as conn:
            rows = conn.execute(
                """
                SELECT
                    id, connector_name, source_name, run_id, created_at, updated_at, status, retry_count, next_retry_at,
                    last_error, payload
                FROM event_connector_failures
                WHERE connector_name = ?
                  AND status = ?
                  AND retry_count < ?
                  AND (next_retry_at IS NULL OR next_retry_at <= ?)
                ORDER BY id ASC
                LIMIT ?
                """,
                (
                    connector_name,
                    EventConnectorFailureStatus.PENDING.value,
                    max(1, max_retry),
                    now.isoformat(),
                    max(1, min(limit, 5000)),
                ),
            ).fetchall()
        return [self._to_failure(row) for row in rows]

    def mark_failure_replayed(self, row_id: int) -> None:
        now = datetime.now(timezone.utc).isoformat()
        with self._conn() as conn:
            conn.execute(
                """
                UPDATE event_connector_failures
                SET status = ?, updated_at = ?, next_retry_at = NULL
                WHERE id = ?
                """,
                (EventConnectorFailureStatus.REPLAYED.value, now, row_id),
            )

    def mark_failure_retry(
        self,
        row_id: int,
        *,
        next_retry_at: datetime,
        error_message: str,
    ) -> None:
        now = datetime.now(timezone.utc).isoformat()
        with self._conn() as conn:
            conn.execute(
                """
                UPDATE event_connector_failures
                SET
                    updated_at = ?,
                    retry_count = retry_count + 1,
                    next_retry_at = ?,
                    last_error = ?,
                    status = ?
                WHERE id = ?
                """,
                (
                    now,
                    _to_iso(next_retry_at),
                    error_message,
                    EventConnectorFailureStatus.PENDING.value,
                    row_id,
                ),
            )

    def mark_failure_dead(self, row_id: int, *, error_message: str) -> None:
        now = datetime.now(timezone.utc).isoformat()
        with self._conn() as conn:
            conn.execute(
                """
                UPDATE event_connector_failures
                SET updated_at = ?, status = ?, last_error = ?
                WHERE id = ?
                """,
                (
                    now,
                    EventConnectorFailureStatus.DEAD.value,
                    error_message,
                    row_id,
                ),
            )

    def count_failures(
        self,
        *,
        connector_name: str | None = None,
        status: EventConnectorFailureStatus | None = None,
    ) -> int:
        sql = "SELECT COUNT(1) AS c FROM event_connector_failures"
        conditions: list[str] = []
        params: list[str] = []
        if connector_name:
            conditions.append("connector_name = ?")
            params.append(connector_name)
        if status:
            conditions.append("status = ?")
            params.append(status.value)
        if conditions:
            sql += " WHERE " + " AND ".join(conditions)
        with self._conn() as conn:
            row = conn.execute(sql, params).fetchone()
        return int(row["c"]) if row else 0

    def upsert_sla_breach_state(
        self,
        *,
        dedupe_key: str,
        breach: EventConnectorSLABreach,
        observed_at: datetime,
        cooldown_seconds: int,
    ) -> tuple[EventConnectorSLAAlertStateRecord, bool]:
        now = observed_at if observed_at.tzinfo else observed_at.replace(tzinfo=timezone.utc)
        now_iso = now.astimezone(timezone.utc).isoformat()
        cooldown = max(0, int(cooldown_seconds))

        with self._conn() as conn:
            row = conn.execute(
                """
                SELECT
                    dedupe_key, connector_name, source_name, breach_type, stage, severity,
                    first_seen_at, last_seen_at, last_emitted_at, last_recovered_at, last_escalated_at,
                    repeat_count, escalation_level, escalation_reason, is_open, message
                FROM event_connector_sla_alert_states
                WHERE dedupe_key = ?
                LIMIT 1
                """,
                (dedupe_key,),
            ).fetchone()

            if row is None:
                conn.execute(
                    """
                    INSERT INTO event_connector_sla_alert_states(
                        dedupe_key, connector_name, source_name, breach_type, stage, severity,
                        first_seen_at, last_seen_at, last_emitted_at, last_recovered_at, last_escalated_at,
                        repeat_count, escalation_level, escalation_reason, is_open, message
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, ?, 0, '', 1, ?)
                    """,
                    (
                        dedupe_key,
                        breach.connector_name,
                        breach.source_name,
                        breach.breach_type.value,
                        breach.stage,
                        breach.severity.value,
                        now_iso,
                        now_iso,
                        now_iso,
                        1,
                        breach.message,
                    ),
                )
                refreshed = conn.execute(
                    """
                    SELECT
                        dedupe_key, connector_name, source_name, breach_type, stage, severity,
                        first_seen_at, last_seen_at, last_emitted_at, last_recovered_at, last_escalated_at,
                        repeat_count, escalation_level, escalation_reason, is_open, message
                    FROM event_connector_sla_alert_states
                    WHERE dedupe_key = ?
                    LIMIT 1
                    """,
                    (dedupe_key,),
                ).fetchone()
                return self._to_sla_state(refreshed), True

            last_emitted = _from_iso(str(row["last_emitted_at"])) if row["last_emitted_at"] else None
            prev_stage = str(row["stage"])
            prev_severity = str(row["severity"])
            stage_changed = prev_stage != breach.stage or prev_severity != breach.severity.value
            cooldown_passed = (
                last_emitted is None
                or (now.astimezone(timezone.utc) - last_emitted.astimezone(timezone.utc)).total_seconds() >= cooldown
            )
            was_open = bool(int(row["is_open"]))
            should_emit = (not was_open) or stage_changed or cooldown_passed

            next_repeat = int(row["repeat_count"] or 0) + 1
            next_last_emitted = now_iso if should_emit else (str(row["last_emitted_at"]) if row["last_emitted_at"] else None)
            reset_escalation = 0 if was_open else 1
            conn.execute(
                """
                UPDATE event_connector_sla_alert_states
                SET
                    connector_name = ?,
                    source_name = ?,
                    breach_type = ?,
                    stage = ?,
                    severity = ?,
                    last_seen_at = ?,
                    last_emitted_at = ?,
                    repeat_count = ?,
                    last_escalated_at = CASE WHEN ? = 1 THEN NULL ELSE last_escalated_at END,
                    escalation_level = CASE WHEN ? = 1 THEN 0 ELSE escalation_level END,
                    escalation_reason = CASE WHEN ? = 1 THEN '' ELSE escalation_reason END,
                    is_open = 1,
                    message = ?
                WHERE dedupe_key = ?
                """,
                (
                    breach.connector_name,
                    breach.source_name,
                    breach.breach_type.value,
                    breach.stage,
                    breach.severity.value,
                    now_iso,
                    next_last_emitted,
                    next_repeat,
                    reset_escalation,
                    reset_escalation,
                    reset_escalation,
                    breach.message,
                    dedupe_key,
                ),
            )
            refreshed = conn.execute(
                """
                SELECT
                    dedupe_key, connector_name, source_name, breach_type, stage, severity,
                    first_seen_at, last_seen_at, last_emitted_at, last_recovered_at, last_escalated_at,
                    repeat_count, escalation_level, escalation_reason, is_open, message
                FROM event_connector_sla_alert_states
                WHERE dedupe_key = ?
                LIMIT 1
                """,
                (dedupe_key,),
            ).fetchone()
            return self._to_sla_state(refreshed), should_emit

    def update_sla_state_escalation(
        self,
        *,
        dedupe_key: str,
        escalation_level: int,
        escalation_reason: str,
        escalated_at: datetime,
    ) -> EventConnectorSLAAlertStateRecord | None:
        level = max(0, int(escalation_level))
        now = escalated_at if escalated_at.tzinfo else escalated_at.replace(tzinfo=timezone.utc)
        with self._conn() as conn:
            cur = conn.execute(
                """
                UPDATE event_connector_sla_alert_states
                SET
                    escalation_level = ?,
                    escalation_reason = ?,
                    last_escalated_at = ?
                WHERE dedupe_key = ?
                  AND is_open = 1
                  AND escalation_level < ?
                """,
                (
                    level,
                    escalation_reason,
                    _to_iso(now),
                    dedupe_key,
                    level,
                ),
            )
            if cur.rowcount <= 0:
                return None
            row = conn.execute(
                """
                SELECT
                    dedupe_key, connector_name, source_name, breach_type, stage, severity,
                    first_seen_at, last_seen_at, last_emitted_at, last_recovered_at, last_escalated_at,
                    repeat_count, escalation_level, escalation_reason, is_open, message
                FROM event_connector_sla_alert_states
                WHERE dedupe_key = ?
                LIMIT 1
                """,
                (dedupe_key,),
            ).fetchone()
        return self._to_sla_state(row) if row is not None else None

    def resolve_sla_alert_states(
        self,
        *,
        active_dedupe_keys: set[str],
        observed_at: datetime,
    ) -> list[EventConnectorSLAAlertStateRecord]:
        now = observed_at if observed_at.tzinfo else observed_at.replace(tzinfo=timezone.utc)
        now_iso = now.astimezone(timezone.utc).isoformat()
        active = {x for x in active_dedupe_keys if x}
        closed: list[EventConnectorSLAAlertStateRecord] = []

        with self._conn() as conn:
            rows = conn.execute(
                """
                SELECT
                    dedupe_key, connector_name, source_name, breach_type, stage, severity,
                    first_seen_at, last_seen_at, last_emitted_at, last_recovered_at, last_escalated_at,
                    repeat_count, escalation_level, escalation_reason, is_open, message
                FROM event_connector_sla_alert_states
                WHERE is_open = 1
                """
            ).fetchall()

            for row in rows:
                key = str(row["dedupe_key"])
                if key in active:
                    continue
                conn.execute(
                    """
                    UPDATE event_connector_sla_alert_states
                    SET
                        is_open = 0,
                        last_recovered_at = ?,
                        last_seen_at = ?
                    WHERE dedupe_key = ?
                    """,
                    (now_iso, now_iso, key),
                )
                refreshed = conn.execute(
                    """
                    SELECT
                        dedupe_key, connector_name, source_name, breach_type, stage, severity,
                        first_seen_at, last_seen_at, last_emitted_at, last_recovered_at, last_escalated_at,
                        repeat_count, escalation_level, escalation_reason, is_open, message
                    FROM event_connector_sla_alert_states
                    WHERE dedupe_key = ?
                    LIMIT 1
                    """,
                    (key,),
                ).fetchone()
                if refreshed is not None:
                    closed.append(self._to_sla_state(refreshed))
        return closed

    def list_sla_alert_states(
        self,
        *,
        connector_name: str | None = None,
        open_only: bool = False,
        limit: int = 200,
    ) -> list[EventConnectorSLAAlertStateRecord]:
        sql = """
            SELECT
                dedupe_key, connector_name, source_name, breach_type, stage, severity,
                first_seen_at, last_seen_at, last_emitted_at, last_recovered_at, last_escalated_at,
                repeat_count, escalation_level, escalation_reason, is_open, message
            FROM event_connector_sla_alert_states
        """
        conditions: list[str] = []
        params: list[str | int] = []
        if connector_name:
            conditions.append("connector_name = ?")
            params.append(connector_name)
        if open_only:
            conditions.append("is_open = 1")
        if conditions:
            sql += " WHERE " + " AND ".join(conditions)
        sql += " ORDER BY is_open DESC, last_seen_at DESC, repeat_count DESC LIMIT ?"
        params.append(max(1, min(limit, 5000)))
        with self._conn() as conn:
            rows = conn.execute(sql, params).fetchall()
        return [self._to_sla_state(row) for row in rows]

    def count_open_sla_alert_states(
        self,
        *,
        connector_name: str | None = None,
        min_escalation_level: int = 0,
    ) -> int:
        sql = "SELECT COUNT(1) AS c FROM event_connector_sla_alert_states WHERE is_open = 1"
        params: list[str | int] = []
        if connector_name:
            sql += " AND connector_name = ?"
            params.append(connector_name)
        if min_escalation_level > 0:
            sql += " AND escalation_level >= ?"
            params.append(max(0, int(min_escalation_level)))
        with self._conn() as conn:
            row = conn.execute(sql, params).fetchone()
        return int(row["c"]) if row else 0

    def append_sla_history(
        self,
        *,
        observed_at: datetime,
        breaches: list[EventConnectorSLABreach],
    ) -> int:
        if not breaches:
            return 0
        inserted = 0
        now_iso = _to_iso(observed_at)
        with self._conn() as conn:
            for breach in breaches:
                conn.execute(
                    """
                    INSERT INTO event_connector_sla_history(
                        observed_at, connector_name, source_name, breach_type, severity, stage,
                        freshness_minutes, pending_failures, dead_failures, message
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        now_iso,
                        breach.connector_name,
                        breach.source_name,
                        breach.breach_type.value,
                        breach.severity.value,
                        breach.stage,
                        breach.freshness_minutes,
                        breach.pending_failures,
                        breach.dead_failures,
                        breach.message,
                    ),
                )
                inserted += 1
        return inserted

    def list_sla_history(
        self,
        *,
        connector_name: str | None = None,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
        limit: int = 100000,
    ) -> list[sqlite3.Row]:
        sql = """
            SELECT
                id, observed_at, connector_name, source_name, breach_type, severity, stage,
                freshness_minutes, pending_failures, dead_failures, message
            FROM event_connector_sla_history
        """
        conditions: list[str] = []
        params: list[str | int] = []
        if connector_name:
            conditions.append("connector_name = ?")
            params.append(connector_name)
        if start_time is not None:
            conditions.append("observed_at >= ?")
            params.append(_to_iso(start_time))
        if end_time is not None:
            conditions.append("observed_at <= ?")
            params.append(_to_iso(end_time))
        if conditions:
            sql += " WHERE " + " AND ".join(conditions)
        sql += " ORDER BY observed_at DESC, id DESC LIMIT ?"
        params.append(max(1, min(limit, 500000)))
        with self._conn() as conn:
            return conn.execute(sql, params).fetchall()

    def connector_overview(self, limit: int = 200) -> list[EventConnectorOverviewItem]:
        connectors = self.list_connectors(limit=limit, enabled_only=False)
        out: list[EventConnectorOverviewItem] = []
        for connector in connectors:
            checkpoint = self.get_checkpoint(connector.connector_name)
            latest = self.latest_run(connector.connector_name)
            source_states = self.list_source_states(connector_name=connector.connector_name, limit=200)
            active_source = next((x for x in source_states if x.is_active), None)
            pending = self.count_failures(
                connector_name=connector.connector_name,
                status=EventConnectorFailureStatus.PENDING,
            )
            dead = self.count_failures(
                connector_name=connector.connector_name,
                status=EventConnectorFailureStatus.DEAD,
            )
            out.append(
                EventConnectorOverviewItem(
                    connector_name=connector.connector_name,
                    source_name=connector.source_name,
                    connector_type=connector.connector_type,
                    enabled=connector.enabled,
                    active_source_key=active_source.source_key if active_source else None,
                    active_source_health=active_source.health_score if active_source else None,
                    last_run_status=latest.status if latest else None,
                    last_run_at=latest.started_at if latest else None,
                    last_success_at=checkpoint.last_success_at if checkpoint else None,
                    checkpoint_publish_time=checkpoint.checkpoint_publish_time if checkpoint else None,
                    pending_failures=pending,
                    dead_failures=dead,
                )
            )
        return out

    def coverage_summary(self, lookback_days: int = 30) -> EventOpsCoverageSummary:
        now = datetime.now(timezone.utc)
        days = max(1, min(lookback_days, 3650))
        start = now - timedelta(days=days)
        with self._conn() as conn:
            totals = conn.execute(
                """
                SELECT
                    COUNT(1) AS total_events,
                    SUM(CASE WHEN polarity = 'POSITIVE' THEN 1 ELSE 0 END) AS positive_events,
                    SUM(CASE WHEN polarity = 'NEGATIVE' THEN 1 ELSE 0 END) AS negative_events,
                    SUM(CASE WHEN polarity = 'NEUTRAL' THEN 1 ELSE 0 END) AS neutral_events,
                    COUNT(DISTINCT symbol) AS symbols_covered,
                    COUNT(DISTINCT source_name) AS sources_covered
                FROM event_records
                WHERE publish_time >= ?
                """,
                (start.isoformat(),),
            ).fetchone()
            daily_rows = conn.execute(
                """
                SELECT
                    DATE(publish_time) AS d,
                    COUNT(1) AS total_events,
                    SUM(CASE WHEN polarity = 'POSITIVE' THEN 1 ELSE 0 END) AS positive_events,
                    SUM(CASE WHEN polarity = 'NEGATIVE' THEN 1 ELSE 0 END) AS negative_events,
                    SUM(CASE WHEN polarity = 'NEUTRAL' THEN 1 ELSE 0 END) AS neutral_events
                FROM event_records
                WHERE publish_time >= ?
                GROUP BY DATE(publish_time)
                ORDER BY d ASC
                """,
                (start.isoformat(),),
            ).fetchall()
            source_rows = conn.execute(
                """
                SELECT
                    source_name,
                    COUNT(1) AS total_events,
                    COUNT(DISTINCT symbol) AS symbols,
                    MAX(publish_time) AS last_publish_time
                FROM event_records
                WHERE publish_time >= ?
                GROUP BY source_name
                ORDER BY total_events DESC, source_name ASC
                LIMIT 200
                """,
                (start.isoformat(),),
            ).fetchall()

        daily = [
            EventCoverageDailyPoint(
                trade_date=datetime.fromisoformat(f"{str(row['d'])}T00:00:00").date(),
                total_events=int(row["total_events"] or 0),
                positive_events=int(row["positive_events"] or 0),
                negative_events=int(row["negative_events"] or 0),
                neutral_events=int(row["neutral_events"] or 0),
            )
            for row in daily_rows
        ]
        sources = [
            EventCoverageSourceItem(
                source_name=str(row["source_name"]),
                total_events=int(row["total_events"] or 0),
                symbols=int(row["symbols"] or 0),
                last_publish_time=_from_iso(str(row["last_publish_time"])) if row["last_publish_time"] else None,
            )
            for row in source_rows
        ]
        return EventOpsCoverageSummary(
            generated_at=now,
            lookback_days=days,
            total_events=int((totals["total_events"] if totals else 0) or 0),
            positive_events=int((totals["positive_events"] if totals else 0) or 0),
            negative_events=int((totals["negative_events"] if totals else 0) or 0),
            neutral_events=int((totals["neutral_events"] if totals else 0) or 0),
            symbols_covered=int((totals["symbols_covered"] if totals else 0) or 0),
            sources_covered=int((totals["sources_covered"] if totals else 0) or 0),
            daily=daily,
            sources=sources,
        )

    @staticmethod
    def _to_connector(row: sqlite3.Row) -> EventConnectorRecord:
        return EventConnectorRecord(
            id=int(row["id"]),
            created_at=datetime.fromisoformat(str(row["created_at"])),
            updated_at=datetime.fromisoformat(str(row["updated_at"])),
            connector_name=str(row["connector_name"]),
            source_name=str(row["source_name"]),
            connector_type=EventConnectorType(str(row["connector_type"])),
            enabled=bool(int(row["enabled"])),
            fetch_limit=int(row["fetch_limit"]),
            poll_interval_minutes=int(row["poll_interval_minutes"]),
            replay_backoff_seconds=int(row["replay_backoff_seconds"]),
            max_retry=int(row["max_retry"]),
            config=dict(json.loads(str(row["config"]))),
            created_by=str(row["created_by"]),
            note=str(row["note"]),
        )

    @staticmethod
    def _to_checkpoint(row: sqlite3.Row) -> EventConnectorCheckpointRecord:
        return EventConnectorCheckpointRecord(
            connector_name=str(row["connector_name"]),
            checkpoint_cursor=str(row["checkpoint_cursor"]) if row["checkpoint_cursor"] else None,
            checkpoint_publish_time=_from_iso(str(row["checkpoint_publish_time"]))
            if row["checkpoint_publish_time"]
            else None,
            updated_at=datetime.fromisoformat(str(row["updated_at"])),
            last_run_at=_from_iso(str(row["last_run_at"])) if row["last_run_at"] else None,
            last_success_at=_from_iso(str(row["last_success_at"])) if row["last_success_at"] else None,
        )

    @staticmethod
    def _to_run(row: sqlite3.Row) -> EventConnectorRunRecord:
        return EventConnectorRunRecord(
            run_id=str(row["run_id"]),
            connector_name=str(row["connector_name"]),
            source_name=str(row["source_name"]),
            started_at=datetime.fromisoformat(str(row["started_at"])),
            finished_at=_from_iso(str(row["finished_at"])) if row["finished_at"] else None,
            status=EventConnectorRunStatus(str(row["status"])),
            triggered_by=str(row["triggered_by"]),
            pulled_count=int(row["pulled_count"] or 0),
            normalized_count=int(row["normalized_count"] or 0),
            inserted_count=int(row["inserted_count"] or 0),
            updated_count=int(row["updated_count"] or 0),
            failed_count=int(row["failed_count"] or 0),
            replayed_count=int(row["replayed_count"] or 0),
            checkpoint_before=str(row["checkpoint_before"]) if row["checkpoint_before"] else None,
            checkpoint_after=str(row["checkpoint_after"]) if row["checkpoint_after"] else None,
            error_message=str(row["error_message"]) if row["error_message"] else None,
            details=dict(json.loads(str(row["details"]))),
        )

    @staticmethod
    def _to_failure(row: sqlite3.Row) -> EventConnectorFailureRecord:
        return EventConnectorFailureRecord(
            id=int(row["id"]),
            connector_name=str(row["connector_name"]),
            source_name=str(row["source_name"]),
            run_id=str(row["run_id"]),
            created_at=datetime.fromisoformat(str(row["created_at"])),
            updated_at=datetime.fromisoformat(str(row["updated_at"])),
            status=EventConnectorFailureStatus(str(row["status"])),
            retry_count=int(row["retry_count"]),
            next_retry_at=_from_iso(str(row["next_retry_at"])) if row["next_retry_at"] else None,
            last_error=str(row["last_error"]),
            payload=dict(json.loads(str(row["payload"]))),
        )

    @staticmethod
    def _to_sla_state(row: sqlite3.Row) -> EventConnectorSLAAlertStateRecord:
        return EventConnectorSLAAlertStateRecord(
            dedupe_key=str(row["dedupe_key"]),
            connector_name=str(row["connector_name"]),
            source_name=str(row["source_name"]),
            breach_type=str(row["breach_type"]),
            stage=str(row["stage"]),
            severity=str(row["severity"]),
            first_seen_at=datetime.fromisoformat(str(row["first_seen_at"])),
            last_seen_at=datetime.fromisoformat(str(row["last_seen_at"])),
            last_emitted_at=_from_iso(str(row["last_emitted_at"])) if row["last_emitted_at"] else None,
            last_recovered_at=_from_iso(str(row["last_recovered_at"])) if row["last_recovered_at"] else None,
            last_escalated_at=_from_iso(str(row["last_escalated_at"])) if row["last_escalated_at"] else None,
            repeat_count=int(row["repeat_count"] or 0),
            escalation_level=int(row["escalation_level"] or 0),
            escalation_reason=str(row["escalation_reason"] or ""),
            is_open=bool(int(row["is_open"])),
            message=str(row["message"] or ""),
        )

    @staticmethod
    def _to_source_state(row: sqlite3.Row) -> EventConnectorSourceStateRecord:
        now = datetime.now(timezone.utc)
        last_attempt = _from_iso(str(row["last_attempt_at"])) if row["last_attempt_at"] else None
        stale_penalty = 0.0
        if last_attempt is not None:
            stale_minutes = max(0.0, (now - last_attempt.astimezone(timezone.utc)).total_seconds() / 60.0)
            stale_penalty = min(20.0, stale_minutes / 30.0)
        effective = max(0.0, float(row["health_score"] or 0.0) - stale_penalty)
        return EventConnectorSourceStateRecord(
            connector_name=str(row["connector_name"]),
            source_key=str(row["source_key"]),
            connector_type=EventConnectorType(str(row["connector_type"])),
            priority=int(row["priority"] or 0),
            enabled=bool(int(row["enabled"])),
            health_score=float(row["health_score"] or 0.0),
            effective_health_score=round(effective, 4),
            consecutive_failures=int(row["consecutive_failures"] or 0),
            total_success=int(row["total_success"] or 0),
            total_failures=int(row["total_failures"] or 0),
            last_latency_ms=int(row["last_latency_ms"]) if row["last_latency_ms"] is not None else None,
            last_error=str(row["last_error"] or ""),
            last_attempt_at=last_attempt,
            last_success_at=_from_iso(str(row["last_success_at"])) if row["last_success_at"] else None,
            last_failure_at=_from_iso(str(row["last_failure_at"])) if row["last_failure_at"] else None,
            checkpoint_cursor=str(row["checkpoint_cursor"]) if row["checkpoint_cursor"] else None,
            checkpoint_publish_time=(
                _from_iso(str(row["checkpoint_publish_time"])) if row["checkpoint_publish_time"] else None
            ),
            is_active=bool(int(row["is_active"])),
        )
