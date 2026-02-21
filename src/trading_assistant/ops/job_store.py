from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from trading_assistant.core.models import (
    JobDefinitionRecord,
    JobRegisterRequest,
    JobRunRecord,
    JobRunStatus,
    JobStatus,
    JobType,
)


class JobStore:
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
                CREATE TABLE IF NOT EXISTS job_definitions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    name TEXT NOT NULL,
                    job_type TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    owner TEXT NOT NULL,
                    schedule_cron TEXT,
                    status TEXT NOT NULL,
                    description TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS job_runs (
                    run_id TEXT PRIMARY KEY,
                    job_id INTEGER NOT NULL,
                    started_at TEXT NOT NULL,
                    finished_at TEXT,
                    status TEXT NOT NULL,
                    triggered_by TEXT NOT NULL,
                    error_message TEXT,
                    result_summary TEXT NOT NULL,
                    FOREIGN KEY(job_id) REFERENCES job_definitions(id)
                )
                """
            )
            conn.execute("CREATE INDEX IF NOT EXISTS idx_job_def_status ON job_definitions(status, id DESC)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_job_run_job_id ON job_runs(job_id, started_at DESC)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_job_run_started_at ON job_runs(started_at DESC)")

    def register(self, req: JobRegisterRequest) -> int:
        now = datetime.now(timezone.utc).isoformat()
        with self._conn() as conn:
            cur = conn.execute(
                """
                INSERT INTO job_definitions(
                    created_at, updated_at, name, job_type, payload, owner, schedule_cron, status, description
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    now,
                    now,
                    req.name,
                    req.job_type.value,
                    json.dumps(req.payload, ensure_ascii=False),
                    req.owner,
                    req.schedule_cron,
                    JobStatus.ACTIVE.value if req.enabled else JobStatus.DISABLED.value,
                    req.description,
                ),
            )
            return int(cur.lastrowid)

    def list_jobs(self, active_only: bool = False, limit: int = 200) -> list[JobDefinitionRecord]:
        sql = """
            SELECT id, created_at, updated_at, name, job_type, payload, owner, schedule_cron, status, description
            FROM job_definitions
        """
        params: list[str | int] = []
        if active_only:
            sql += " WHERE status = ?"
            params.append(JobStatus.ACTIVE.value)
        sql += " ORDER BY id DESC LIMIT ?"
        params.append(max(1, min(limit, 1000)))
        with self._conn() as conn:
            rows = conn.execute(sql, params).fetchall()
        return [self._to_job(row) for row in rows]

    def get_job(self, job_id: int) -> JobDefinitionRecord | None:
        with self._conn() as conn:
            row = conn.execute(
                """
                SELECT id, created_at, updated_at, name, job_type, payload, owner, schedule_cron, status, description
                FROM job_definitions
                WHERE id = ?
                LIMIT 1
                """,
                (job_id,),
            ).fetchone()
        return self._to_job(row) if row else None

    def create_run(self, run_id: str, job_id: int, triggered_by: str) -> None:
        now = datetime.now(timezone.utc).isoformat()
        with self._conn() as conn:
            conn.execute(
                """
                INSERT INTO job_runs(
                    run_id, job_id, started_at, finished_at, status, triggered_by, error_message, result_summary
                )
                VALUES (?, ?, ?, NULL, ?, ?, NULL, ?)
                """,
                (run_id, job_id, now, JobRunStatus.RUNNING.value, triggered_by, "{}"),
            )

    def finish_run(
        self,
        run_id: str,
        status: JobRunStatus,
        result_summary: dict,
        error_message: str | None = None,
    ) -> None:
        now = datetime.now(timezone.utc).isoformat()
        with self._conn() as conn:
            conn.execute(
                """
                UPDATE job_runs
                SET finished_at = ?, status = ?, error_message = ?, result_summary = ?
                WHERE run_id = ?
                """,
                (
                    now,
                    status.value,
                    error_message,
                    json.dumps(result_summary, ensure_ascii=False),
                    run_id,
                ),
            )

    def list_runs(self, job_id: int, limit: int = 200) -> list[JobRunRecord]:
        with self._conn() as conn:
            rows = conn.execute(
                """
                SELECT run_id, job_id, started_at, finished_at, status, triggered_by, error_message, result_summary
                FROM job_runs
                WHERE job_id = ?
                ORDER BY started_at DESC
                LIMIT ?
                """,
                (job_id, max(1, min(limit, 1000))),
            ).fetchall()
        return [self._to_run(row) for row in rows]

    def get_run(self, run_id: str) -> JobRunRecord | None:
        with self._conn() as conn:
            row = conn.execute(
                """
                SELECT run_id, job_id, started_at, finished_at, status, triggered_by, error_message, result_summary
                FROM job_runs
                WHERE run_id = ?
                LIMIT 1
                """,
                (run_id,),
            ).fetchone()
        return self._to_run(row) if row else None

    def get_latest_run(self, job_id: int) -> JobRunRecord | None:
        with self._conn() as conn:
            row = conn.execute(
                """
                SELECT run_id, job_id, started_at, finished_at, status, triggered_by, error_message, result_summary
                FROM job_runs
                WHERE job_id = ?
                ORDER BY started_at DESC
                LIMIT 1
                """,
                (job_id,),
            ).fetchone()
        return self._to_run(row) if row else None

    def list_recent_runs(
        self,
        limit: int = 200,
        since: datetime | None = None,
        job_id: int | None = None,
    ) -> list[JobRunRecord]:
        sql = """
            SELECT run_id, job_id, started_at, finished_at, status, triggered_by, error_message, result_summary
            FROM job_runs
        """
        conditions: list[str] = []
        params: list[str | int] = []
        if since is not None:
            conditions.append("started_at >= ?")
            params.append(since.isoformat())
        if job_id is not None:
            conditions.append("job_id = ?")
            params.append(job_id)
        if conditions:
            sql += " WHERE " + " AND ".join(conditions)
        sql += " ORDER BY started_at DESC LIMIT ?"
        params.append(max(1, min(limit, 5000)))
        with self._conn() as conn:
            rows = conn.execute(sql, params).fetchall()
        return [self._to_run(row) for row in rows]

    def _to_job(self, row: sqlite3.Row) -> JobDefinitionRecord:
        return JobDefinitionRecord(
            id=int(row["id"]),
            created_at=datetime.fromisoformat(str(row["created_at"])),
            updated_at=datetime.fromisoformat(str(row["updated_at"])),
            name=str(row["name"]),
            job_type=JobType(str(row["job_type"])),
            payload=dict(json.loads(str(row["payload"]))),
            owner=str(row["owner"]),
            schedule_cron=str(row["schedule_cron"]) if row["schedule_cron"] else None,
            status=JobStatus(str(row["status"])),
            description=str(row["description"]),
        )

    def _to_run(self, row: sqlite3.Row) -> JobRunRecord:
        return JobRunRecord(
            run_id=str(row["run_id"]),
            job_id=int(row["job_id"]),
            started_at=datetime.fromisoformat(str(row["started_at"])),
            finished_at=datetime.fromisoformat(str(row["finished_at"])) if row["finished_at"] else None,
            status=JobRunStatus(str(row["status"])),
            triggered_by=str(row["triggered_by"]),
            error_message=str(row["error_message"]) if row["error_message"] else None,
            result_summary=dict(json.loads(str(row["result_summary"]))),
        )
