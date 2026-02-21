from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from trading_assistant.core.models import (
    StrategyDecisionRecord,
    StrategyDecisionType,
    StrategyVersionRecord,
    StrategyVersionStatus,
)


class StrategyGovernanceStore:
    def __init__(self, db_path: str) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_schema()

    def _conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _table_columns(self, conn: sqlite3.Connection, table: str) -> set[str]:
        rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
        return {str(r["name"]) for r in rows}

    def _init_schema(self) -> None:
        with self._conn() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS strategy_versions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    strategy_name TEXT NOT NULL,
                    version TEXT NOT NULL,
                    status TEXT NOT NULL,
                    description TEXT NOT NULL,
                    params_hash TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    created_by TEXT NOT NULL,
                    approved_at TEXT,
                    approved_by TEXT,
                    note TEXT NOT NULL
                )
                """
            )
            cols = self._table_columns(conn, "strategy_versions")
            if "approved_at" not in cols:
                conn.execute("ALTER TABLE strategy_versions ADD COLUMN approved_at TEXT")
            if "approved_by" not in cols:
                conn.execute("ALTER TABLE strategy_versions ADD COLUMN approved_by TEXT")
            if "note" not in cols:
                conn.execute("ALTER TABLE strategy_versions ADD COLUMN note TEXT NOT NULL DEFAULT ''")

            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS strategy_decisions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    strategy_name TEXT NOT NULL,
                    version TEXT NOT NULL,
                    reviewer TEXT NOT NULL,
                    reviewer_role TEXT NOT NULL,
                    decision TEXT NOT NULL,
                    note TEXT NOT NULL,
                    created_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS idx_strategy_unique
                ON strategy_versions(strategy_name, version)
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_strategy_latest
                ON strategy_versions(strategy_name, id DESC)
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_strategy_decisions_lookup
                ON strategy_decisions(strategy_name, version, reviewer_role, created_at DESC)
                """
            )

    def register_draft(
        self,
        strategy_name: str,
        version: str,
        description: str,
        params_hash: str,
        created_by: str,
    ) -> int:
        now = datetime.now(timezone.utc).isoformat()
        with self._conn() as conn:
            cur = conn.execute(
                """
                INSERT INTO strategy_versions(
                    strategy_name, version, status, description, params_hash, created_at,
                    created_by, approved_at, approved_by, note
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, NULL, NULL, '')
                """,
                (
                    strategy_name,
                    version,
                    StrategyVersionStatus.DRAFT.value,
                    description,
                    params_hash,
                    now,
                    created_by,
                ),
            )
            return int(cur.lastrowid)

    def update_status(
        self,
        strategy_name: str,
        version: str,
        status: StrategyVersionStatus,
        note: str = "",
        approved_by: str | None = None,
    ) -> int:
        now = datetime.now(timezone.utc).isoformat()
        with self._conn() as conn:
            if status == StrategyVersionStatus.APPROVED:
                conn.execute(
                    """
                    UPDATE strategy_versions
                    SET status = ?, approved_at = ?, approved_by = ?, note = ?
                    WHERE strategy_name = ? AND version = ?
                    """,
                    (status.value, now, approved_by or "", note, strategy_name, version),
                )
            else:
                conn.execute(
                    """
                    UPDATE strategy_versions
                    SET status = ?, note = ?
                    WHERE strategy_name = ? AND version = ?
                    """,
                    (status.value, note, strategy_name, version),
                )
            row = conn.execute(
                "SELECT id FROM strategy_versions WHERE strategy_name = ? AND version = ? LIMIT 1",
                (strategy_name, version),
            ).fetchone()
            return int(row["id"]) if row else -1

    def record_decision(
        self,
        strategy_name: str,
        version: str,
        reviewer: str,
        reviewer_role: str,
        decision: StrategyDecisionType,
        note: str,
    ) -> int:
        now = datetime.now(timezone.utc).isoformat()
        with self._conn() as conn:
            cur = conn.execute(
                """
                INSERT INTO strategy_decisions(
                    strategy_name, version, reviewer, reviewer_role, decision, note, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    strategy_name,
                    version,
                    reviewer,
                    reviewer_role,
                    decision.value,
                    note,
                    now,
                ),
            )
            return int(cur.lastrowid)

    def list_decisions(self, strategy_name: str, version: str, limit: int = 200) -> list[StrategyDecisionRecord]:
        with self._conn() as conn:
            rows = conn.execute(
                """
                SELECT id, strategy_name, version, reviewer, reviewer_role, decision, note, created_at
                FROM strategy_decisions
                WHERE strategy_name = ? AND version = ?
                ORDER BY id DESC
                LIMIT ?
                """,
                (strategy_name, version, max(1, min(limit, 1000))),
            ).fetchall()
        return [self._to_decision_record(r) for r in rows]

    def latest_decision_by_role(self, strategy_name: str, version: str) -> dict[str, StrategyDecisionRecord]:
        with self._conn() as conn:
            rows = conn.execute(
                """
                SELECT d1.id, d1.strategy_name, d1.version, d1.reviewer, d1.reviewer_role, d1.decision, d1.note, d1.created_at
                FROM strategy_decisions d1
                JOIN (
                    SELECT reviewer_role, MAX(id) AS max_id
                    FROM strategy_decisions
                    WHERE strategy_name = ? AND version = ?
                    GROUP BY reviewer_role
                ) d2 ON d1.id = d2.max_id
                """,
                (strategy_name, version),
            ).fetchall()
        result: dict[str, StrategyDecisionRecord] = {}
        for row in rows:
            rec = self._to_decision_record(row)
            result[rec.reviewer_role] = rec
        return result

    def get_version(self, strategy_name: str, version: str) -> StrategyVersionRecord | None:
        with self._conn() as conn:
            row = conn.execute(
                """
                SELECT id, strategy_name, version, status, description, params_hash,
                       created_at, created_by, approved_at, approved_by, note
                FROM strategy_versions
                WHERE strategy_name = ? AND version = ?
                LIMIT 1
                """,
                (strategy_name, version),
            ).fetchone()
        return self._to_record(row) if row else None

    def list_versions(self, strategy_name: str | None = None, limit: int = 200) -> list[StrategyVersionRecord]:
        sql = """
            SELECT id, strategy_name, version, status, description, params_hash,
                   created_at, created_by, approved_at, approved_by, note
            FROM strategy_versions
        """
        params: list[str | int] = []
        if strategy_name:
            sql += " WHERE strategy_name = ?"
            params.append(strategy_name)
        sql += " ORDER BY id DESC LIMIT ?"
        params.append(max(1, min(limit, 1000)))
        with self._conn() as conn:
            rows = conn.execute(sql, params).fetchall()
        return [self._to_record(r) for r in rows]

    def latest_approved(self, strategy_name: str) -> StrategyVersionRecord | None:
        with self._conn() as conn:
            row = conn.execute(
                """
                SELECT id, strategy_name, version, status, description, params_hash,
                       created_at, created_by, approved_at, approved_by, note
                FROM strategy_versions
                WHERE strategy_name = ? AND status = ?
                ORDER BY id DESC LIMIT 1
                """,
                (strategy_name, StrategyVersionStatus.APPROVED.value),
            ).fetchone()
        return self._to_record(row) if row else None

    def _to_record(self, row: sqlite3.Row) -> StrategyVersionRecord:
        return StrategyVersionRecord(
            id=int(row["id"]),
            strategy_name=str(row["strategy_name"]),
            version=str(row["version"]),
            status=StrategyVersionStatus(str(row["status"])),
            description=str(row["description"]),
            params_hash=str(row["params_hash"]),
            created_at=datetime.fromisoformat(str(row["created_at"])),
            created_by=str(row["created_by"]),
            approved_at=datetime.fromisoformat(str(row["approved_at"])) if row["approved_at"] else None,
            approved_by=str(row["approved_by"]) if row["approved_by"] else None,
            note=str(row["note"]),
        )

    def _to_decision_record(self, row: sqlite3.Row) -> StrategyDecisionRecord:
        return StrategyDecisionRecord(
            id=int(row["id"]),
            strategy_name=str(row["strategy_name"]),
            version=str(row["version"]),
            reviewer=str(row["reviewer"]),
            reviewer_role=str(row["reviewer_role"]),
            decision=StrategyDecisionType(str(row["decision"])),
            note=str(row["note"]),
            created_at=datetime.fromisoformat(str(row["created_at"])),
        )

