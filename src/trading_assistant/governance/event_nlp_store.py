from __future__ import annotations

import json
import sqlite3
from datetime import date, datetime, time, timezone
from pathlib import Path

from trading_assistant.core.models import (
    EventNLPConsensusRecord,
    EventNLPDriftAlert,
    EventNLPDriftSnapshotRecord,
    EventNLPFeedbackRecord,
    EventNLPFeedbackUpsertRequest,
    EventNLPLabelEntryRecord,
    EventNLPLabelEntryUpsertRequest,
    EventNLPLabelSnapshotRecord,
    EventNLPRule,
    EventNLPRulesetActivateRequest,
    EventNLPRulesetRecord,
    EventNLPRulesetUpsertRequest,
    EventNLPWindowMetrics,
)


def _to_iso(dt: datetime) -> str:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc).isoformat()
    return dt.astimezone(timezone.utc).isoformat()


def _to_iso_date_start(d: date) -> str:
    return datetime.combine(d, time.min, tzinfo=timezone.utc).isoformat()


def _to_iso_date_end(d: date) -> str:
    return datetime.combine(d, time.max, tzinfo=timezone.utc).isoformat()


class EventNLPStore:
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
                CREATE TABLE IF NOT EXISTS event_nlp_rulesets (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    version TEXT NOT NULL UNIQUE,
                    created_by TEXT NOT NULL,
                    note TEXT NOT NULL,
                    is_active INTEGER NOT NULL,
                    rules_json TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_event_nlp_rulesets_active
                ON event_nlp_rulesets(is_active, updated_at DESC)
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS event_nlp_drift_snapshots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    created_at TEXT NOT NULL,
                    source_name TEXT,
                    ruleset_version TEXT NOT NULL,
                    current_start TEXT NOT NULL,
                    current_end TEXT NOT NULL,
                    baseline_start TEXT NOT NULL,
                    baseline_end TEXT NOT NULL,
                    sample_size INTEGER NOT NULL,
                    hit_rate REAL NOT NULL,
                    baseline_hit_rate REAL NOT NULL,
                    hit_rate_delta REAL NOT NULL,
                    score_p50 REAL NOT NULL,
                    baseline_score_p50 REAL NOT NULL,
                    score_p50_delta REAL NOT NULL,
                    contribution_delta REAL,
                    alerts_json TEXT NOT NULL,
                    payload_json TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_event_nlp_drift_snapshots_lookup
                ON event_nlp_drift_snapshots(source_name, created_at DESC)
                """
            )
            self._ensure_columns(
                conn=conn,
                table_name="event_nlp_drift_snapshots",
                columns={
                    "feedback_polarity_accuracy_delta": "REAL",
                    "feedback_event_type_accuracy_delta": "REAL",
                },
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS event_nlp_label_feedback (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    source_name TEXT NOT NULL,
                    event_id TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    publish_time TEXT NOT NULL,
                    predicted_event_type TEXT NOT NULL,
                    predicted_polarity TEXT NOT NULL,
                    predicted_score REAL NOT NULL,
                    label_event_type TEXT NOT NULL,
                    label_polarity TEXT NOT NULL,
                    label_score REAL,
                    labeler TEXT NOT NULL,
                    note TEXT NOT NULL,
                    UNIQUE(source_name, event_id)
                )
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_event_nlp_feedback_lookup
                ON event_nlp_label_feedback(source_name, publish_time DESC, updated_at DESC)
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS event_nlp_label_entries (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    source_name TEXT NOT NULL,
                    event_id TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    publish_time TEXT NOT NULL,
                    predicted_event_type TEXT NOT NULL,
                    predicted_polarity TEXT NOT NULL,
                    predicted_score REAL NOT NULL,
                    label_event_type TEXT NOT NULL,
                    label_polarity TEXT NOT NULL,
                    label_score REAL,
                    labeler TEXT NOT NULL,
                    label_version TEXT NOT NULL,
                    note TEXT NOT NULL,
                    UNIQUE(source_name, event_id, labeler)
                )
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_event_nlp_label_entries_lookup
                ON event_nlp_label_entries(source_name, publish_time DESC, updated_at DESC)
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_event_nlp_label_entries_labeler
                ON event_nlp_label_entries(labeler, updated_at DESC)
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS event_nlp_label_consensus (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    source_name TEXT NOT NULL,
                    event_id TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    publish_time TEXT NOT NULL,
                    consensus_event_type TEXT NOT NULL,
                    consensus_polarity TEXT NOT NULL,
                    consensus_score REAL,
                    consensus_confidence REAL NOT NULL,
                    label_count INTEGER NOT NULL,
                    conflict INTEGER NOT NULL,
                    conflict_reasons_json TEXT NOT NULL,
                    adjudicated_by TEXT NOT NULL,
                    label_version TEXT NOT NULL,
                    UNIQUE(source_name, event_id)
                )
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_event_nlp_label_consensus_lookup
                ON event_nlp_label_consensus(source_name, publish_time DESC, updated_at DESC)
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS event_nlp_label_snapshots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    created_at TEXT NOT NULL,
                    source_name TEXT,
                    start_date TEXT NOT NULL,
                    end_date TEXT NOT NULL,
                    min_labelers INTEGER NOT NULL,
                    include_conflicts INTEGER NOT NULL,
                    sample_size INTEGER NOT NULL,
                    consensus_size INTEGER NOT NULL,
                    conflict_size INTEGER NOT NULL,
                    hash_sha256 TEXT NOT NULL,
                    stats_json TEXT NOT NULL,
                    created_by TEXT NOT NULL,
                    note TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_event_nlp_label_snapshots_lookup
                ON event_nlp_label_snapshots(source_name, created_at DESC)
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

    def upsert_ruleset(self, req: EventNLPRulesetUpsertRequest) -> int:
        now = datetime.now(timezone.utc).isoformat()
        rules_json = json.dumps([rule.model_dump(mode="json") for rule in req.rules], ensure_ascii=False)
        with self._conn() as conn:
            conn.execute(
                """
                INSERT INTO event_nlp_rulesets(
                    created_at, updated_at, version, created_by, note, is_active, rules_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(version) DO UPDATE SET
                    updated_at = excluded.updated_at,
                    created_by = excluded.created_by,
                    note = excluded.note,
                    rules_json = excluded.rules_json
                """,
                (
                    now,
                    now,
                    req.version,
                    req.created_by,
                    req.note,
                    1 if req.activate else 0,
                    rules_json,
                ),
            )
            if req.activate:
                conn.execute("UPDATE event_nlp_rulesets SET is_active = 0")
                conn.execute(
                    "UPDATE event_nlp_rulesets SET is_active = 1, updated_at = ? WHERE version = ?",
                    (now, req.version),
                )
            row = conn.execute(
                "SELECT id FROM event_nlp_rulesets WHERE version = ? LIMIT 1",
                (req.version,),
            ).fetchone()
        return int(row["id"]) if row else -1

    def activate_ruleset(self, req: EventNLPRulesetActivateRequest) -> bool:
        now = datetime.now(timezone.utc).isoformat()
        with self._conn() as conn:
            exists = conn.execute(
                "SELECT 1 FROM event_nlp_rulesets WHERE version = ? LIMIT 1",
                (req.version,),
            ).fetchone()
            if exists is None:
                return False
            conn.execute("UPDATE event_nlp_rulesets SET is_active = 0")
            cur = conn.execute(
                "UPDATE event_nlp_rulesets SET is_active = 1, updated_at = ?, note = ? WHERE version = ?",
                (now, req.note, req.version),
            )
        return cur.rowcount > 0

    def get_ruleset(self, version: str, include_rules: bool = True) -> EventNLPRulesetRecord | None:
        with self._conn() as conn:
            row = conn.execute(
                """
                SELECT id, created_at, updated_at, version, created_by, note, is_active, rules_json
                FROM event_nlp_rulesets
                WHERE version = ?
                LIMIT 1
                """,
                (version,),
            ).fetchone()
        return self._to_ruleset(row, include_rules=include_rules) if row else None

    def get_active_ruleset(self, include_rules: bool = True) -> EventNLPRulesetRecord | None:
        with self._conn() as conn:
            row = conn.execute(
                """
                SELECT id, created_at, updated_at, version, created_by, note, is_active, rules_json
                FROM event_nlp_rulesets
                WHERE is_active = 1
                ORDER BY updated_at DESC
                LIMIT 1
                """
            ).fetchone()
        return self._to_ruleset(row, include_rules=include_rules) if row else None

    def list_rulesets(self, limit: int = 50, include_rules: bool = False) -> list[EventNLPRulesetRecord]:
        with self._conn() as conn:
            rows = conn.execute(
                """
                SELECT id, created_at, updated_at, version, created_by, note, is_active, rules_json
                FROM event_nlp_rulesets
                ORDER BY updated_at DESC, id DESC
                LIMIT ?
                """,
                (max(1, min(limit, 500)),),
            ).fetchall()
        return [self._to_ruleset(row, include_rules=include_rules) for row in rows]

    def load_active_ruleset(self) -> tuple[str, list[EventNLPRule]] | None:
        ruleset = self.get_active_ruleset(include_rules=True)
        if ruleset is None or not ruleset.rules:
            return None
        return ruleset.version, ruleset.rules

    def load_event_rows_for_metrics(
        self,
        *,
        source_name: str | None,
        start_date: date,
        end_date: date,
        limit: int = 200000,
    ) -> list[sqlite3.Row]:
        sql = """
            SELECT source_name, event_type, polarity, score, confidence, metadata
            FROM event_records
            WHERE publish_time >= ? AND publish_time <= ?
        """
        params: list[str | int] = [_to_iso_date_start(start_date), _to_iso_date_end(end_date)]
        if source_name:
            sql += " AND source_name = ?"
            params.append(source_name)
        sql += " ORDER BY publish_time DESC LIMIT ?"
        params.append(max(1, min(limit, 500000)))
        with self._conn() as conn:
            return conn.execute(sql, params).fetchall()

    def insert_drift_snapshot(
        self,
        *,
        source_name: str | None,
        ruleset_version: str,
        current_start: date,
        current_end: date,
        baseline_start: date,
        baseline_end: date,
        current: EventNLPWindowMetrics,
        baseline: EventNLPWindowMetrics,
        hit_rate_delta: float,
        score_p50_delta: float,
        contribution_delta: float | None,
        feedback_polarity_accuracy_delta: float | None,
        feedback_event_type_accuracy_delta: float | None,
        alerts: list[EventNLPDriftAlert],
        payload: dict,
    ) -> int:
        now = datetime.now(timezone.utc).isoformat()
        with self._conn() as conn:
            cur = conn.execute(
                """
                INSERT INTO event_nlp_drift_snapshots(
                    created_at, source_name, ruleset_version, current_start, current_end, baseline_start, baseline_end,
                    sample_size, hit_rate, baseline_hit_rate, hit_rate_delta,
                    score_p50, baseline_score_p50, score_p50_delta,
                    contribution_delta, feedback_polarity_accuracy_delta, feedback_event_type_accuracy_delta,
                    alerts_json, payload_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    now,
                    source_name,
                    ruleset_version,
                    current_start.isoformat(),
                    current_end.isoformat(),
                    baseline_start.isoformat(),
                    baseline_end.isoformat(),
                    current.sample_size,
                    current.hit_rate,
                    baseline.hit_rate,
                    hit_rate_delta,
                    current.score_p50,
                    baseline.score_p50,
                    score_p50_delta,
                    contribution_delta,
                    feedback_polarity_accuracy_delta,
                    feedback_event_type_accuracy_delta,
                    json.dumps([a.model_dump(mode="json") for a in alerts], ensure_ascii=False),
                    json.dumps(payload, ensure_ascii=False),
                ),
            )
        return int(cur.lastrowid)

    def list_drift_snapshots(
        self,
        *,
        source_name: str | None = None,
        limit: int = 200,
    ) -> list[EventNLPDriftSnapshotRecord]:
        sql = """
            SELECT
                id, created_at, source_name, ruleset_version, current_start, current_end, baseline_start, baseline_end,
                sample_size, hit_rate, baseline_hit_rate, hit_rate_delta,
                score_p50, baseline_score_p50, score_p50_delta, contribution_delta,
                feedback_polarity_accuracy_delta, feedback_event_type_accuracy_delta, alerts_json
            FROM event_nlp_drift_snapshots
        """
        params: list[str | int] = []
        if source_name:
            sql += " WHERE source_name = ?"
            params.append(source_name)
        sql += " ORDER BY created_at DESC, id DESC LIMIT ?"
        params.append(max(1, min(limit, 2000)))
        with self._conn() as conn:
            rows = conn.execute(sql, params).fetchall()

        out: list[EventNLPDriftSnapshotRecord] = []
        for row in rows:
            raw_alerts = json.loads(str(row["alerts_json"]))
            alerts = [EventNLPDriftAlert.model_validate(item) for item in raw_alerts]
            out.append(
                EventNLPDriftSnapshotRecord(
                    id=int(row["id"]),
                    created_at=datetime.fromisoformat(str(row["created_at"])),
                    source_name=str(row["source_name"]) if row["source_name"] else None,
                    ruleset_version=str(row["ruleset_version"]),
                    current_start=date.fromisoformat(str(row["current_start"])),
                    current_end=date.fromisoformat(str(row["current_end"])),
                    baseline_start=date.fromisoformat(str(row["baseline_start"])),
                    baseline_end=date.fromisoformat(str(row["baseline_end"])),
                    sample_size=int(row["sample_size"]),
                    hit_rate=float(row["hit_rate"]),
                    baseline_hit_rate=float(row["baseline_hit_rate"]),
                    hit_rate_delta=float(row["hit_rate_delta"]),
                    score_p50=float(row["score_p50"]),
                    baseline_score_p50=float(row["baseline_score_p50"]),
                    score_p50_delta=float(row["score_p50_delta"]),
                    contribution_delta=float(row["contribution_delta"]) if row["contribution_delta"] is not None else None,
                    feedback_polarity_accuracy_delta=(
                        float(row["feedback_polarity_accuracy_delta"])
                        if row["feedback_polarity_accuracy_delta"] is not None
                        else None
                    ),
                    feedback_event_type_accuracy_delta=(
                        float(row["feedback_event_type_accuracy_delta"])
                        if row["feedback_event_type_accuracy_delta"] is not None
                        else None
                    ),
                    alerts=alerts,
                )
            )
        return out

    def upsert_feedback(self, req: EventNLPFeedbackUpsertRequest) -> int:
        now = datetime.now(timezone.utc).isoformat()
        with self._conn() as conn:
            event_row = conn.execute(
                """
                SELECT source_name, event_id, symbol, publish_time, event_type, polarity, score
                FROM event_records
                WHERE source_name = ? AND event_id = ?
                LIMIT 1
                """,
                (req.source_name, req.event_id),
            ).fetchone()
            if event_row is None:
                raise KeyError(f"event not found: source_name='{req.source_name}', event_id='{req.event_id}'")

            conn.execute(
                """
                INSERT INTO event_nlp_label_feedback(
                    created_at, updated_at, source_name, event_id, symbol, publish_time,
                    predicted_event_type, predicted_polarity, predicted_score,
                    label_event_type, label_polarity, label_score, labeler, note
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(source_name, event_id) DO UPDATE SET
                    updated_at = excluded.updated_at,
                    symbol = excluded.symbol,
                    publish_time = excluded.publish_time,
                    predicted_event_type = excluded.predicted_event_type,
                    predicted_polarity = excluded.predicted_polarity,
                    predicted_score = excluded.predicted_score,
                    label_event_type = excluded.label_event_type,
                    label_polarity = excluded.label_polarity,
                    label_score = excluded.label_score,
                    labeler = excluded.labeler,
                    note = excluded.note
                """,
                (
                    now,
                    now,
                    req.source_name,
                    req.event_id,
                    str(event_row["symbol"]),
                    str(event_row["publish_time"]),
                    str(event_row["event_type"]),
                    str(event_row["polarity"]),
                    float(event_row["score"]),
                    req.label_event_type,
                    req.label_polarity.value,
                    req.label_score,
                    req.labeler,
                    req.note,
                ),
            )
            row = conn.execute(
                """
                SELECT id
                FROM event_nlp_label_feedback
                WHERE source_name = ? AND event_id = ?
                LIMIT 1
                """,
                (req.source_name, req.event_id),
            ).fetchone()
        return int(row["id"]) if row else -1

    def upsert_label_entry(self, req: EventNLPLabelEntryUpsertRequest) -> int:
        now = datetime.now(timezone.utc).isoformat()
        with self._conn() as conn:
            event_row = conn.execute(
                """
                SELECT source_name, event_id, symbol, publish_time, event_type, polarity, score
                FROM event_records
                WHERE source_name = ? AND event_id = ?
                LIMIT 1
                """,
                (req.source_name, req.event_id),
            ).fetchone()
            if event_row is None:
                raise KeyError(f"event not found: source_name='{req.source_name}', event_id='{req.event_id}'")

            conn.execute(
                """
                INSERT INTO event_nlp_label_entries(
                    created_at, updated_at, source_name, event_id, symbol, publish_time,
                    predicted_event_type, predicted_polarity, predicted_score,
                    label_event_type, label_polarity, label_score, labeler, label_version, note
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(source_name, event_id, labeler) DO UPDATE SET
                    updated_at = excluded.updated_at,
                    symbol = excluded.symbol,
                    publish_time = excluded.publish_time,
                    predicted_event_type = excluded.predicted_event_type,
                    predicted_polarity = excluded.predicted_polarity,
                    predicted_score = excluded.predicted_score,
                    label_event_type = excluded.label_event_type,
                    label_polarity = excluded.label_polarity,
                    label_score = excluded.label_score,
                    label_version = excluded.label_version,
                    note = excluded.note
                """,
                (
                    now,
                    now,
                    req.source_name,
                    req.event_id,
                    str(event_row["symbol"]),
                    str(event_row["publish_time"]),
                    str(event_row["event_type"]),
                    str(event_row["polarity"]),
                    float(event_row["score"]),
                    req.label_event_type,
                    req.label_polarity.value,
                    req.label_score,
                    req.labeler,
                    req.label_version,
                    req.note,
                ),
            )
            row = conn.execute(
                """
                SELECT id
                FROM event_nlp_label_entries
                WHERE source_name = ? AND event_id = ? AND labeler = ?
                LIMIT 1
                """,
                (req.source_name, req.event_id, req.labeler),
            ).fetchone()
        return int(row["id"]) if row else -1

    def list_label_entries(
        self,
        *,
        source_name: str | None = None,
        labeler: str | None = None,
        event_id: str | None = None,
        start_date: date | None = None,
        end_date: date | None = None,
        limit: int = 500,
    ) -> list[EventNLPLabelEntryRecord]:
        sql = """
            SELECT
                id, created_at, updated_at, source_name, event_id, symbol, publish_time,
                predicted_event_type, predicted_polarity, predicted_score,
                label_event_type, label_polarity, label_score, labeler, label_version, note
            FROM event_nlp_label_entries
        """
        conditions: list[str] = []
        params: list[str | int] = []
        if source_name:
            conditions.append("source_name = ?")
            params.append(source_name)
        if labeler:
            conditions.append("labeler = ?")
            params.append(labeler)
        if event_id:
            conditions.append("event_id = ?")
            params.append(event_id)
        if start_date:
            conditions.append("publish_time >= ?")
            params.append(_to_iso_date_start(start_date))
        if end_date:
            conditions.append("publish_time <= ?")
            params.append(_to_iso_date_end(end_date))
        if conditions:
            sql += " WHERE " + " AND ".join(conditions)
        sql += " ORDER BY publish_time DESC, updated_at DESC, id DESC LIMIT ?"
        params.append(max(1, min(limit, 20000)))
        with self._conn() as conn:
            rows = conn.execute(sql, params).fetchall()
        return [self._to_label_entry(row) for row in rows]

    def load_label_entry_rows_for_scope(
        self,
        *,
        source_name: str | None = None,
        event_ids: list[str] | None = None,
        start_date: date | None = None,
        end_date: date | None = None,
        min_labelers: int = 1,
        limit: int = 200000,
    ) -> list[sqlite3.Row]:
        sql = """
            SELECT
                id, source_name, event_id, symbol, publish_time,
                predicted_event_type, predicted_polarity, predicted_score,
                label_event_type, label_polarity, label_score, labeler, label_version, note
            FROM event_nlp_label_entries
        """
        conditions: list[str] = []
        params: list[str | int] = []
        if source_name:
            conditions.append("source_name = ?")
            params.append(source_name)
        if event_ids:
            ids = [x for x in event_ids if x]
            if ids:
                placeholders = ", ".join(["?"] * len(ids))
                conditions.append(f"event_id IN ({placeholders})")
                params.extend(ids)
        if start_date:
            conditions.append("publish_time >= ?")
            params.append(_to_iso_date_start(start_date))
        if end_date:
            conditions.append("publish_time <= ?")
            params.append(_to_iso_date_end(end_date))
        if conditions:
            sql += " WHERE " + " AND ".join(conditions)
        sql += " ORDER BY publish_time DESC, event_id ASC, labeler ASC LIMIT ?"
        params.append(max(1, min(limit, 500000)))
        with self._conn() as conn:
            rows = conn.execute(sql, params).fetchall()
        if min_labelers <= 1:
            return rows
        threshold = max(1, int(min_labelers))
        counts: dict[tuple[str, str], int] = {}
        for row in rows:
            key = (str(row["source_name"]), str(row["event_id"]))
            counts[key] = counts.get(key, 0) + 1
        allowed = {key for key, count in counts.items() if count >= threshold}
        if not allowed:
            return []
        return [row for row in rows if (str(row["source_name"]), str(row["event_id"])) in allowed]

    def upsert_consensus(
        self,
        *,
        source_name: str,
        event_id: str,
        symbol: str,
        publish_time: datetime,
        consensus_event_type: str,
        consensus_polarity: str,
        consensus_score: float | None,
        consensus_confidence: float,
        label_count: int,
        conflict: bool,
        conflict_reasons: list[str],
        adjudicated_by: str,
        label_version: str,
    ) -> int:
        now = datetime.now(timezone.utc).isoformat()
        with self._conn() as conn:
            conn.execute(
                """
                INSERT INTO event_nlp_label_consensus(
                    created_at, updated_at, source_name, event_id, symbol, publish_time,
                    consensus_event_type, consensus_polarity, consensus_score, consensus_confidence,
                    label_count, conflict, conflict_reasons_json, adjudicated_by, label_version
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(source_name, event_id) DO UPDATE SET
                    updated_at = excluded.updated_at,
                    symbol = excluded.symbol,
                    publish_time = excluded.publish_time,
                    consensus_event_type = excluded.consensus_event_type,
                    consensus_polarity = excluded.consensus_polarity,
                    consensus_score = excluded.consensus_score,
                    consensus_confidence = excluded.consensus_confidence,
                    label_count = excluded.label_count,
                    conflict = excluded.conflict,
                    conflict_reasons_json = excluded.conflict_reasons_json,
                    adjudicated_by = excluded.adjudicated_by,
                    label_version = excluded.label_version
                """,
                (
                    now,
                    now,
                    source_name,
                    event_id,
                    symbol,
                    _to_iso(publish_time),
                    consensus_event_type,
                    consensus_polarity,
                    consensus_score,
                    max(0.0, min(1.0, float(consensus_confidence))),
                    max(0, int(label_count)),
                    1 if conflict else 0,
                    json.dumps(conflict_reasons, ensure_ascii=False),
                    adjudicated_by,
                    label_version,
                ),
            )
            row = conn.execute(
                """
                SELECT id
                FROM event_nlp_label_consensus
                WHERE source_name = ? AND event_id = ?
                LIMIT 1
                """,
                (source_name, event_id),
            ).fetchone()
        return int(row["id"]) if row else -1

    def list_consensus(
        self,
        *,
        source_name: str | None = None,
        start_date: date | None = None,
        end_date: date | None = None,
        limit: int = 500,
    ) -> list[EventNLPConsensusRecord]:
        sql = """
            SELECT
                id, created_at, updated_at, source_name, event_id, symbol, publish_time,
                consensus_event_type, consensus_polarity, consensus_score, consensus_confidence,
                label_count, conflict, conflict_reasons_json, adjudicated_by, label_version
            FROM event_nlp_label_consensus
        """
        conditions: list[str] = []
        params: list[str | int] = []
        if source_name:
            conditions.append("source_name = ?")
            params.append(source_name)
        if start_date:
            conditions.append("publish_time >= ?")
            params.append(_to_iso_date_start(start_date))
        if end_date:
            conditions.append("publish_time <= ?")
            params.append(_to_iso_date_end(end_date))
        if conditions:
            sql += " WHERE " + " AND ".join(conditions)
        sql += " ORDER BY publish_time DESC, updated_at DESC LIMIT ?"
        params.append(max(1, min(limit, 20000)))
        with self._conn() as conn:
            rows = conn.execute(sql, params).fetchall()
        return [self._to_consensus(row) for row in rows]

    def create_label_snapshot(
        self,
        *,
        source_name: str | None,
        start_date: date,
        end_date: date,
        min_labelers: int,
        include_conflicts: bool,
        sample_size: int,
        consensus_size: int,
        conflict_size: int,
        hash_sha256: str,
        stats: dict,
        created_by: str,
        note: str,
    ) -> int:
        now = datetime.now(timezone.utc).isoformat()
        with self._conn() as conn:
            cur = conn.execute(
                """
                INSERT INTO event_nlp_label_snapshots(
                    created_at, source_name, start_date, end_date, min_labelers, include_conflicts,
                    sample_size, consensus_size, conflict_size, hash_sha256, stats_json, created_by, note
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    now,
                    source_name,
                    start_date.isoformat(),
                    end_date.isoformat(),
                    max(1, int(min_labelers)),
                    1 if include_conflicts else 0,
                    max(0, int(sample_size)),
                    max(0, int(consensus_size)),
                    max(0, int(conflict_size)),
                    hash_sha256,
                    json.dumps(stats, ensure_ascii=False),
                    created_by,
                    note,
                ),
            )
        return int(cur.lastrowid)

    def list_label_snapshots(
        self,
        *,
        source_name: str | None = None,
        limit: int = 200,
    ) -> list[EventNLPLabelSnapshotRecord]:
        sql = """
            SELECT
                id, created_at, source_name, start_date, end_date, min_labelers, include_conflicts,
                sample_size, consensus_size, conflict_size, hash_sha256, stats_json, created_by, note
            FROM event_nlp_label_snapshots
        """
        params: list[str | int] = []
        if source_name:
            sql += " WHERE source_name = ?"
            params.append(source_name)
        sql += " ORDER BY created_at DESC, id DESC LIMIT ?"
        params.append(max(1, min(limit, 2000)))
        with self._conn() as conn:
            rows = conn.execute(sql, params).fetchall()
        return [self._to_label_snapshot(row) for row in rows]

    def list_feedback(
        self,
        *,
        source_name: str | None = None,
        labeler: str | None = None,
        start_date: date | None = None,
        end_date: date | None = None,
        limit: int = 200,
    ) -> list[EventNLPFeedbackRecord]:
        sql = """
            SELECT
                id, created_at, updated_at, source_name, event_id, symbol, publish_time,
                predicted_event_type, predicted_polarity, predicted_score,
                label_event_type, label_polarity, label_score, labeler, note
            FROM event_nlp_label_feedback
        """
        conditions: list[str] = []
        params: list[str | int] = []
        if source_name:
            conditions.append("source_name = ?")
            params.append(source_name)
        if labeler:
            conditions.append("labeler = ?")
            params.append(labeler)
        if start_date:
            conditions.append("publish_time >= ?")
            params.append(_to_iso_date_start(start_date))
        if end_date:
            conditions.append("publish_time <= ?")
            params.append(_to_iso_date_end(end_date))
        if conditions:
            sql += " WHERE " + " AND ".join(conditions)
        sql += " ORDER BY updated_at DESC, id DESC LIMIT ?"
        params.append(max(1, min(limit, 5000)))
        with self._conn() as conn:
            rows = conn.execute(sql, params).fetchall()
        return [self._to_feedback(row) for row in rows]

    def load_feedback_rows_for_metrics(
        self,
        *,
        source_name: str | None,
        start_date: date,
        end_date: date,
        limit: int = 200000,
    ) -> list[sqlite3.Row]:
        sql = """
            SELECT
                source_name, event_id, predicted_event_type, predicted_polarity, predicted_score,
                label_event_type, label_polarity, label_score
            FROM event_nlp_label_feedback
            WHERE publish_time >= ? AND publish_time <= ?
        """
        params: list[str | int] = [_to_iso_date_start(start_date), _to_iso_date_end(end_date)]
        if source_name:
            sql += " AND source_name = ?"
            params.append(source_name)
        sql += " ORDER BY updated_at DESC LIMIT ?"
        params.append(max(1, min(limit, 500000)))
        with self._conn() as conn:
            return conn.execute(sql, params).fetchall()

    @staticmethod
    def _to_ruleset(row: sqlite3.Row, *, include_rules: bool) -> EventNLPRulesetRecord:
        parsed_rules = [EventNLPRule.model_validate(item) for item in json.loads(str(row["rules_json"]))]
        return EventNLPRulesetRecord(
            id=int(row["id"]),
            created_at=datetime.fromisoformat(str(row["created_at"])),
            updated_at=datetime.fromisoformat(str(row["updated_at"])),
            version=str(row["version"]),
            created_by=str(row["created_by"]),
            note=str(row["note"]),
            is_active=bool(int(row["is_active"])),
            rule_count=len(parsed_rules),
            rules=parsed_rules if include_rules else [],
        )

    @staticmethod
    def _to_feedback(row: sqlite3.Row) -> EventNLPFeedbackRecord:
        return EventNLPFeedbackRecord(
            id=int(row["id"]),
            created_at=datetime.fromisoformat(str(row["created_at"])),
            updated_at=datetime.fromisoformat(str(row["updated_at"])),
            source_name=str(row["source_name"]),
            event_id=str(row["event_id"]),
            symbol=str(row["symbol"]),
            publish_time=datetime.fromisoformat(str(row["publish_time"])),
            predicted_event_type=str(row["predicted_event_type"]),
            predicted_polarity=str(row["predicted_polarity"]),
            predicted_score=float(row["predicted_score"]),
            label_event_type=str(row["label_event_type"]),
            label_polarity=str(row["label_polarity"]),
            label_score=float(row["label_score"]) if row["label_score"] is not None else None,
            labeler=str(row["labeler"]),
            note=str(row["note"] or ""),
        )

    @staticmethod
    def _to_label_entry(row: sqlite3.Row) -> EventNLPLabelEntryRecord:
        return EventNLPLabelEntryRecord(
            id=int(row["id"]),
            created_at=datetime.fromisoformat(str(row["created_at"])),
            updated_at=datetime.fromisoformat(str(row["updated_at"])),
            source_name=str(row["source_name"]),
            event_id=str(row["event_id"]),
            symbol=str(row["symbol"]),
            publish_time=datetime.fromisoformat(str(row["publish_time"])),
            predicted_event_type=str(row["predicted_event_type"]),
            predicted_polarity=str(row["predicted_polarity"]),
            predicted_score=float(row["predicted_score"]),
            label_event_type=str(row["label_event_type"]),
            label_polarity=str(row["label_polarity"]),
            label_score=float(row["label_score"]) if row["label_score"] is not None else None,
            labeler=str(row["labeler"]),
            label_version=str(row["label_version"] or "v1"),
            note=str(row["note"] or ""),
        )

    @staticmethod
    def _to_consensus(row: sqlite3.Row) -> EventNLPConsensusRecord:
        reasons_raw = row["conflict_reasons_json"]
        reasons = list(json.loads(str(reasons_raw))) if reasons_raw else []
        return EventNLPConsensusRecord(
            id=int(row["id"]),
            created_at=datetime.fromisoformat(str(row["created_at"])),
            updated_at=datetime.fromisoformat(str(row["updated_at"])),
            source_name=str(row["source_name"]),
            event_id=str(row["event_id"]),
            symbol=str(row["symbol"]),
            publish_time=datetime.fromisoformat(str(row["publish_time"])),
            consensus_event_type=str(row["consensus_event_type"]),
            consensus_polarity=str(row["consensus_polarity"]),
            consensus_score=float(row["consensus_score"]) if row["consensus_score"] is not None else None,
            consensus_confidence=float(row["consensus_confidence"] or 0.0),
            label_count=int(row["label_count"] or 0),
            conflict=bool(int(row["conflict"] or 0)),
            conflict_reasons=[str(x) for x in reasons],
            adjudicated_by=str(row["adjudicated_by"] or ""),
            label_version=str(row["label_version"] or "v1"),
        )

    @staticmethod
    def _to_label_snapshot(row: sqlite3.Row) -> EventNLPLabelSnapshotRecord:
        stats = dict(json.loads(str(row["stats_json"]))) if row["stats_json"] else {}
        return EventNLPLabelSnapshotRecord(
            id=int(row["id"]),
            created_at=datetime.fromisoformat(str(row["created_at"])),
            source_name=str(row["source_name"]) if row["source_name"] else None,
            start_date=date.fromisoformat(str(row["start_date"])),
            end_date=date.fromisoformat(str(row["end_date"])),
            min_labelers=int(row["min_labelers"] or 1),
            include_conflicts=bool(int(row["include_conflicts"] or 0)),
            sample_size=int(row["sample_size"] or 0),
            consensus_size=int(row["consensus_size"] or 0),
            conflict_size=int(row["conflict_size"] or 0),
            hash_sha256=str(row["hash_sha256"]),
            stats=stats,
            created_by=str(row["created_by"] or ""),
            note=str(row["note"] or ""),
        )
