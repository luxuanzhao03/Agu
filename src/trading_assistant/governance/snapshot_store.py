from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from trading_assistant.core.models import DataSnapshotRecord, DataSnapshotRegisterRequest


class DataSnapshotStore:
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
                CREATE TABLE IF NOT EXISTS data_snapshots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    created_at TEXT NOT NULL,
                    dataset_name TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    start_date TEXT NOT NULL,
                    end_date TEXT NOT NULL,
                    provider TEXT NOT NULL,
                    row_count INTEGER NOT NULL,
                    schema_version TEXT NOT NULL,
                    content_hash TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS idx_snapshot_unique
                ON data_snapshots(dataset_name, symbol, start_date, end_date, provider, content_hash)
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_snapshot_lookup ON data_snapshots(dataset_name, symbol, created_at DESC)"
            )

    def register(self, req: DataSnapshotRegisterRequest) -> int:
        now = datetime.now(timezone.utc).isoformat()
        with self._conn() as conn:
            cur = conn.execute(
                """
                INSERT OR IGNORE INTO data_snapshots(
                    created_at, dataset_name, symbol, start_date, end_date, provider,
                    row_count, schema_version, content_hash
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    now,
                    req.dataset_name,
                    req.symbol,
                    req.start_date.isoformat(),
                    req.end_date.isoformat(),
                    req.provider,
                    req.row_count,
                    req.schema_version,
                    req.content_hash,
                ),
            )
            if cur.lastrowid:
                return int(cur.lastrowid)

            # If duplicate inserted before, return existing id.
            row = conn.execute(
                """
                SELECT id
                FROM data_snapshots
                WHERE dataset_name = ? AND symbol = ? AND start_date = ? AND end_date = ?
                  AND provider = ? AND content_hash = ?
                LIMIT 1
                """,
                (
                    req.dataset_name,
                    req.symbol,
                    req.start_date.isoformat(),
                    req.end_date.isoformat(),
                    req.provider,
                    req.content_hash,
                ),
            ).fetchone()
            return int(row["id"]) if row else -1

    def list_snapshots(self, dataset_name: str | None = None, symbol: str | None = None, limit: int = 200) -> list[DataSnapshotRecord]:
        limit = max(1, min(limit, 1000))
        sql = """
            SELECT id, created_at, dataset_name, symbol, start_date, end_date, provider,
                   row_count, schema_version, content_hash
            FROM data_snapshots
        """
        conditions: list[str] = []
        params: list[str | int] = []
        if dataset_name:
            conditions.append("dataset_name = ?")
            params.append(dataset_name)
        if symbol:
            conditions.append("symbol = ?")
            params.append(symbol)
        if conditions:
            sql += " WHERE " + " AND ".join(conditions)
        sql += " ORDER BY id DESC LIMIT ?"
        params.append(limit)

        with self._conn() as conn:
            rows = conn.execute(sql, params).fetchall()
        return [self._to_record(row) for row in rows]

    def latest_snapshot(self, dataset_name: str, symbol: str) -> DataSnapshotRecord | None:
        with self._conn() as conn:
            row = conn.execute(
                """
                SELECT id, created_at, dataset_name, symbol, start_date, end_date, provider,
                       row_count, schema_version, content_hash
                FROM data_snapshots
                WHERE dataset_name = ? AND symbol = ?
                ORDER BY id DESC
                LIMIT 1
                """,
                (dataset_name, symbol),
            ).fetchone()
        return self._to_record(row) if row else None

    def _to_record(self, row: sqlite3.Row) -> DataSnapshotRecord:
        return DataSnapshotRecord(
            id=int(row["id"]),
            created_at=datetime.fromisoformat(str(row["created_at"])),
            dataset_name=str(row["dataset_name"]),
            symbol=str(row["symbol"]),
            start_date=datetime.fromisoformat(str(row["start_date"])).date(),
            end_date=datetime.fromisoformat(str(row["end_date"])).date(),
            provider=str(row["provider"]),
            row_count=int(row["row_count"]),
            schema_version=str(row["schema_version"]),
            content_hash=str(row["content_hash"]),
        )

