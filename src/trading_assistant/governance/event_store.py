from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from trading_assistant.core.models import (
    EventBatchIngestRequest,
    EventPolarity,
    EventRecord,
    EventRecordCreate,
    EventSourceRecord,
    EventSourceRegisterRequest,
    EventSourceType,
)


def _to_iso(dt: datetime) -> str:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc).isoformat()
    return dt.astimezone(timezone.utc).isoformat()


class EventStore:
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
                CREATE TABLE IF NOT EXISTS event_sources (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    source_name TEXT NOT NULL UNIQUE,
                    source_type TEXT NOT NULL,
                    provider TEXT NOT NULL,
                    timezone TEXT NOT NULL,
                    ingestion_lag_minutes INTEGER NOT NULL,
                    reliability_score REAL NOT NULL,
                    description TEXT NOT NULL,
                    created_by TEXT NOT NULL,
                    note TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS event_records (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    source_name TEXT NOT NULL,
                    event_id TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    publish_time TEXT NOT NULL,
                    effective_time TEXT,
                    polarity TEXT NOT NULL,
                    score REAL NOT NULL,
                    confidence REAL NOT NULL,
                    title TEXT NOT NULL,
                    summary TEXT NOT NULL,
                    raw_ref TEXT,
                    tags TEXT NOT NULL,
                    metadata TEXT NOT NULL,
                    FOREIGN KEY(source_name) REFERENCES event_sources(source_name)
                )
                """
            )
            conn.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS idx_event_unique_source_event
                ON event_records(source_name, event_id)
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_event_symbol_time
                ON event_records(symbol, publish_time DESC)
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_event_source_time
                ON event_records(source_name, publish_time DESC)
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_event_id_lookup
                ON event_records(event_id)
                """
            )

    def register_source(self, req: EventSourceRegisterRequest) -> int:
        now = datetime.now(timezone.utc).isoformat()
        with self._conn() as conn:
            conn.execute(
                """
                INSERT INTO event_sources(
                    created_at, updated_at, source_name, source_type, provider, timezone,
                    ingestion_lag_minutes, reliability_score, description, created_by, note
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(source_name) DO UPDATE SET
                    updated_at=excluded.updated_at,
                    source_type=excluded.source_type,
                    provider=excluded.provider,
                    timezone=excluded.timezone,
                    ingestion_lag_minutes=excluded.ingestion_lag_minutes,
                    reliability_score=excluded.reliability_score,
                    description=excluded.description,
                    created_by=excluded.created_by,
                    note=excluded.note
                """,
                (
                    now,
                    now,
                    req.source_name,
                    req.source_type.value,
                    req.provider,
                    req.timezone,
                    req.ingestion_lag_minutes,
                    req.reliability_score,
                    req.description,
                    req.created_by,
                    req.note,
                ),
            )
            row = conn.execute(
                "SELECT id FROM event_sources WHERE source_name = ? LIMIT 1",
                (req.source_name,),
            ).fetchone()
        return int(row["id"]) if row else -1

    def get_source(self, source_name: str) -> EventSourceRecord | None:
        with self._conn() as conn:
            row = conn.execute(
                """
                SELECT
                    id, created_at, updated_at, source_name, source_type, provider, timezone,
                    ingestion_lag_minutes, reliability_score, description, created_by, note
                FROM event_sources
                WHERE source_name = ?
                LIMIT 1
                """,
                (source_name,),
            ).fetchone()
        return self._to_source(row) if row else None

    def list_sources(self, limit: int = 200) -> list[EventSourceRecord]:
        with self._conn() as conn:
            rows = conn.execute(
                """
                SELECT
                    id, created_at, updated_at, source_name, source_type, provider, timezone,
                    ingestion_lag_minutes, reliability_score, description, created_by, note
                FROM event_sources
                ORDER BY id DESC
                LIMIT ?
                """,
                (max(1, min(limit, 1000)),),
            ).fetchall()
        return [self._to_source(row) for row in rows]

    def ingest_batch(self, req: EventBatchIngestRequest) -> tuple[int, int, list[str]]:
        if self.get_source(req.source_name) is None:
            raise KeyError(f"event source '{req.source_name}' not found")

        inserted = 0
        updated = 0
        errors: list[str] = []
        with self._conn() as conn:
            for idx, event in enumerate(req.events):
                try:
                    exists = conn.execute(
                        """
                        SELECT 1
                        FROM event_records
                        WHERE source_name = ? AND event_id = ?
                        LIMIT 1
                        """,
                        (req.source_name, event.event_id),
                    ).fetchone()
                    now = datetime.now(timezone.utc).isoformat()
                    if exists is None:
                        conn.execute(
                            """
                            INSERT INTO event_records(
                                created_at, updated_at, source_name, event_id, symbol, event_type, publish_time, effective_time,
                                polarity, score, confidence, title, summary, raw_ref, tags, metadata
                            )
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """,
                            self._event_values(now, req.source_name, event),
                        )
                        inserted += 1
                    else:
                        conn.execute(
                            """
                            UPDATE event_records
                            SET
                                updated_at = ?,
                                symbol = ?,
                                event_type = ?,
                                publish_time = ?,
                                effective_time = ?,
                                polarity = ?,
                                score = ?,
                                confidence = ?,
                                title = ?,
                                summary = ?,
                                raw_ref = ?,
                                tags = ?,
                                metadata = ?
                            WHERE source_name = ? AND event_id = ?
                            """,
                            (
                                now,
                                event.symbol,
                                event.event_type,
                                _to_iso(event.publish_time),
                                _to_iso(event.effective_time) if event.effective_time else None,
                                event.polarity.value,
                                float(event.score),
                                float(event.confidence),
                                event.title,
                                event.summary,
                                event.raw_ref,
                                json.dumps(event.tags, ensure_ascii=False),
                                json.dumps(event.metadata, ensure_ascii=False),
                                req.source_name,
                                event.event_id,
                            ),
                        )
                        updated += 1
                except Exception as exc:  # noqa: BLE001
                    errors.append(f"idx={idx}, event_id={event.event_id}: {exc}")
        return inserted, updated, errors

    def list_events(
        self,
        symbol: str | None = None,
        source_name: str | None = None,
        event_type: str | None = None,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
        limit: int = 500,
    ) -> list[EventRecord]:
        sql = """
            SELECT
                id, created_at, updated_at, source_name, event_id, symbol, event_type, publish_time, effective_time,
                polarity, score, confidence, title, summary, raw_ref, tags, metadata
            FROM event_records
        """
        conditions: list[str] = []
        params: list[str | int] = []
        if symbol:
            conditions.append("symbol = ?")
            params.append(symbol)
        if source_name:
            conditions.append("source_name = ?")
            params.append(source_name)
        if event_type:
            conditions.append("event_type = ?")
            params.append(event_type)
        if start_time:
            conditions.append("publish_time >= ?")
            params.append(_to_iso(start_time))
        if end_time:
            conditions.append("publish_time <= ?")
            params.append(_to_iso(end_time))
        if conditions:
            sql += " WHERE " + " AND ".join(conditions)
        sql += " ORDER BY publish_time DESC, id DESC LIMIT ?"
        params.append(max(1, min(limit, 5000)))
        with self._conn() as conn:
            rows = conn.execute(sql, params).fetchall()
        return [self._to_event(row) for row in rows]

    def list_symbol_events_between(
        self,
        symbol: str,
        start_time: datetime,
        end_time: datetime,
        limit: int = 20000,
    ) -> list[EventRecord]:
        return self.list_events(
            symbol=symbol,
            start_time=start_time,
            end_time=end_time,
            limit=limit,
        )

    def get_event(self, source_name: str, event_id: str) -> EventRecord | None:
        with self._conn() as conn:
            row = conn.execute(
                """
                SELECT
                    id, created_at, updated_at, source_name, event_id, symbol, event_type, publish_time, effective_time,
                    polarity, score, confidence, title, summary, raw_ref, tags, metadata
                FROM event_records
                WHERE source_name = ? AND event_id = ?
                LIMIT 1
                """,
                (source_name, event_id),
            ).fetchone()
        return self._to_event(row) if row else None

    def find_events_by_event_id(self, event_id: str, limit: int = 20) -> list[EventRecord]:
        with self._conn() as conn:
            rows = conn.execute(
                """
                SELECT
                    id, created_at, updated_at, source_name, event_id, symbol, event_type, publish_time, effective_time,
                    polarity, score, confidence, title, summary, raw_ref, tags, metadata
                FROM event_records
                WHERE event_id = ?
                ORDER BY publish_time DESC, id DESC
                LIMIT ?
                """,
                (event_id, max(1, min(limit, 1000))),
            ).fetchall()
        return [self._to_event(row) for row in rows]

    @staticmethod
    def _event_values(now: str, source_name: str, event: EventRecordCreate) -> tuple:
        return (
            now,
            now,
            source_name,
            event.event_id,
            event.symbol,
            event.event_type,
            _to_iso(event.publish_time),
            _to_iso(event.effective_time) if event.effective_time else None,
            event.polarity.value,
            float(event.score),
            float(event.confidence),
            event.title,
            event.summary,
            event.raw_ref,
            json.dumps(event.tags, ensure_ascii=False),
            json.dumps(event.metadata, ensure_ascii=False),
        )

    @staticmethod
    def _to_source(row: sqlite3.Row) -> EventSourceRecord:
        return EventSourceRecord(
            id=int(row["id"]),
            created_at=datetime.fromisoformat(str(row["created_at"])),
            updated_at=datetime.fromisoformat(str(row["updated_at"])),
            source_name=str(row["source_name"]),
            source_type=EventSourceType(str(row["source_type"])),
            provider=str(row["provider"]),
            timezone=str(row["timezone"]),
            ingestion_lag_minutes=int(row["ingestion_lag_minutes"]),
            reliability_score=float(row["reliability_score"]),
            description=str(row["description"]),
            created_by=str(row["created_by"]),
            note=str(row["note"]),
        )

    @staticmethod
    def _to_event(row: sqlite3.Row) -> EventRecord:
        return EventRecord(
            id=int(row["id"]),
            created_at=datetime.fromisoformat(str(row["created_at"])),
            updated_at=datetime.fromisoformat(str(row["updated_at"])),
            source_name=str(row["source_name"]),
            event_id=str(row["event_id"]),
            symbol=str(row["symbol"]),
            event_type=str(row["event_type"]),
            publish_time=datetime.fromisoformat(str(row["publish_time"])),
            effective_time=datetime.fromisoformat(str(row["effective_time"])) if row["effective_time"] else None,
            polarity=EventPolarity(str(row["polarity"])),
            score=float(row["score"]),
            confidence=float(row["confidence"]),
            title=str(row["title"]),
            summary=str(row["summary"]),
            raw_ref=str(row["raw_ref"]) if row["raw_ref"] else None,
            tags=list(json.loads(str(row["tags"]))),
            metadata=dict(json.loads(str(row["metadata"]))),
        )
