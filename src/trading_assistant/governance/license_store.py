from __future__ import annotations

import json
import sqlite3
from datetime import date, datetime, timezone
from pathlib import Path

from trading_assistant.core.models import DataLicenseRecord, DataLicenseRegisterRequest


class DataLicenseStore:
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
                CREATE TABLE IF NOT EXISTS data_licenses (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    created_at TEXT NOT NULL,
                    dataset_name TEXT NOT NULL,
                    provider TEXT NOT NULL,
                    licensor TEXT NOT NULL,
                    usage_scopes TEXT NOT NULL,
                    allow_export INTEGER NOT NULL,
                    enforce_watermark TEXT NOT NULL,
                    valid_from TEXT NOT NULL,
                    valid_to TEXT,
                    max_export_rows INTEGER,
                    created_by TEXT NOT NULL,
                    note TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_data_license_lookup
                ON data_licenses(dataset_name, provider, id DESC)
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_data_license_valid
                ON data_licenses(valid_from, valid_to)
                """
            )

    def register(self, req: DataLicenseRegisterRequest) -> int:
        now = datetime.now(timezone.utc).isoformat()
        with self._conn() as conn:
            cur = conn.execute(
                """
                INSERT INTO data_licenses(
                    created_at, dataset_name, provider, licensor, usage_scopes, allow_export,
                    enforce_watermark, valid_from, valid_to, max_export_rows, created_by, note
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    now,
                    req.dataset_name,
                    req.provider,
                    req.licensor,
                    json.dumps(req.usage_scopes, ensure_ascii=False),
                    1 if req.allow_export else 0,
                    req.enforce_watermark,
                    req.valid_from.isoformat(),
                    req.valid_to.isoformat() if req.valid_to else None,
                    req.max_export_rows,
                    req.created_by,
                    req.note,
                ),
            )
            return int(cur.lastrowid)

    def list_licenses(
        self,
        dataset_name: str | None = None,
        provider: str | None = None,
        active_only: bool = False,
        as_of: date | None = None,
        limit: int = 200,
    ) -> list[DataLicenseRecord]:
        sql = """
            SELECT
                id, created_at, dataset_name, provider, licensor, usage_scopes, allow_export,
                enforce_watermark, valid_from, valid_to, max_export_rows, created_by, note
            FROM data_licenses
        """
        conditions: list[str] = []
        params: list[str | int] = []
        if dataset_name:
            conditions.append("dataset_name = ?")
            params.append(dataset_name)
        if provider:
            conditions.append("provider = ?")
            params.append(provider)
        if active_only:
            now = (as_of or date.today()).isoformat()
            conditions.append("valid_from <= ?")
            params.append(now)
            conditions.append("(valid_to IS NULL OR valid_to >= ?)")
            params.append(now)
        if conditions:
            sql += " WHERE " + " AND ".join(conditions)
        sql += " ORDER BY id DESC LIMIT ?"
        params.append(max(1, min(limit, 1000)))

        with self._conn() as conn:
            rows = conn.execute(sql, params).fetchall()
        return [self._to_record(row) for row in rows]

    def latest_active(self, dataset_name: str, provider: str, as_of: date | None = None) -> DataLicenseRecord | None:
        now = (as_of or date.today()).isoformat()
        with self._conn() as conn:
            row = conn.execute(
                """
                SELECT
                    id, created_at, dataset_name, provider, licensor, usage_scopes, allow_export,
                    enforce_watermark, valid_from, valid_to, max_export_rows, created_by, note
                FROM data_licenses
                WHERE dataset_name = ? AND provider = ?
                  AND valid_from <= ?
                  AND (valid_to IS NULL OR valid_to >= ?)
                ORDER BY id DESC
                LIMIT 1
                """,
                (dataset_name, provider, now, now),
            ).fetchone()
        return self._to_record(row) if row else None

    def _to_record(self, row: sqlite3.Row) -> DataLicenseRecord:
        return DataLicenseRecord(
            id=int(row["id"]),
            created_at=datetime.fromisoformat(str(row["created_at"])),
            dataset_name=str(row["dataset_name"]),
            provider=str(row["provider"]),
            licensor=str(row["licensor"]),
            usage_scopes=list(json.loads(str(row["usage_scopes"]))),
            allow_export=bool(int(row["allow_export"])),
            enforce_watermark=str(row["enforce_watermark"]),
            valid_from=date.fromisoformat(str(row["valid_from"])),
            valid_to=date.fromisoformat(str(row["valid_to"])) if row["valid_to"] else None,
            max_export_rows=int(row["max_export_rows"]) if row["max_export_rows"] is not None else None,
            created_by=str(row["created_by"]),
            note=str(row["note"]),
        )
