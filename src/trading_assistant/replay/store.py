from __future__ import annotations

import json
import sqlite3
from datetime import date, datetime, timezone
from pathlib import Path

from trading_assistant.core.models import (
    CostModelCalibrationRecord,
    CostModelCalibrationResult,
    ExecutionRecordCreate,
    SignalAction,
    SignalDecisionRecord,
)


class ReplayStore:
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
                CREATE TABLE IF NOT EXISTS signal_records (
                    signal_id TEXT PRIMARY KEY,
                    symbol TEXT NOT NULL,
                    strategy_name TEXT NOT NULL,
                    trade_date TEXT NOT NULL,
                    action TEXT NOT NULL,
                    confidence REAL NOT NULL,
                    reason TEXT NOT NULL,
                    suggested_position REAL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS execution_records (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    signal_id TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    execution_date TEXT NOT NULL,
                    side TEXT NOT NULL,
                    quantity INTEGER NOT NULL,
                    price REAL NOT NULL,
                    fee REAL NOT NULL,
                    note TEXT NOT NULL,
                    FOREIGN KEY(signal_id) REFERENCES signal_records(signal_id)
                )
                """
            )
            self._ensure_execution_column(conn, "reference_price", "REAL")
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS cost_model_calibration_runs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    created_at TEXT NOT NULL,
                    symbol TEXT,
                    strategy_name TEXT,
                    start_date TEXT,
                    end_date TEXT,
                    sample_size INTEGER NOT NULL,
                    executed_samples INTEGER NOT NULL,
                    payload_json TEXT NOT NULL
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_exec_signal_id ON execution_records(signal_id)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_signal_symbol_date ON signal_records(symbol, trade_date DESC)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_cost_calibration_created ON cost_model_calibration_runs(created_at DESC)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_cost_calibration_symbol ON cost_model_calibration_runs(symbol, created_at DESC)"
            )

    def record_signal(self, record: SignalDecisionRecord) -> str:
        with self._conn() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO signal_records(
                    signal_id, symbol, strategy_name, trade_date, action, confidence, reason, suggested_position
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    record.signal_id,
                    record.symbol,
                    record.strategy_name,
                    record.trade_date.isoformat(),
                    record.action.value,
                    record.confidence,
                    record.reason,
                    record.suggested_position,
                ),
            )
        return record.signal_id

    def record_execution(self, record: ExecutionRecordCreate) -> int:
        with self._conn() as conn:
            cur = conn.execute(
                """
                INSERT INTO execution_records(
                    signal_id, symbol, execution_date, side, quantity, price, reference_price, fee, note
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    record.signal_id,
                    record.symbol,
                    record.execution_date.isoformat(),
                    record.side.value,
                    record.quantity,
                    record.price,
                    record.reference_price,
                    record.fee,
                    record.note,
                ),
            )
            return int(cur.lastrowid)

    def load_pairs(
        self,
        symbol: str | None = None,
        strategy_name: str | None = None,
        start_date: date | None = None,
        end_date: date | None = None,
        limit: int = 500,
    ) -> list[sqlite3.Row]:
        sql = """
            SELECT
                s.signal_id,
                s.symbol,
                s.strategy_name,
                s.trade_date,
                s.action AS signal_action,
                s.confidence,
                (
                    SELECT x.side
                    FROM execution_records x
                    WHERE x.signal_id = s.signal_id
                    ORDER BY x.execution_date DESC, x.id DESC
                    LIMIT 1
                ) AS executed_action,
                (
                    SELECT MAX(x.execution_date)
                    FROM execution_records x
                    WHERE x.signal_id = s.signal_id
                ) AS execution_date,
                (
                    SELECT COALESCE(SUM(x.quantity), 0)
                    FROM execution_records x
                    WHERE x.signal_id = s.signal_id
                ) AS quantity,
                (
                    SELECT
                        CASE
                            WHEN SUM(x.quantity) > 0 THEN SUM(x.price * x.quantity) / SUM(x.quantity)
                            ELSE 0.0
                        END
                    FROM execution_records x
                    WHERE x.signal_id = s.signal_id
                ) AS price,
                (
                    SELECT
                        CASE
                            WHEN SUM(CASE WHEN x.reference_price IS NOT NULL AND x.reference_price > 0 THEN x.quantity ELSE 0 END) > 0
                                THEN
                                    SUM(
                                        CASE
                                            WHEN x.reference_price IS NOT NULL AND x.reference_price > 0
                                                THEN x.reference_price * x.quantity
                                            ELSE 0.0
                                        END
                                    ) / SUM(CASE WHEN x.reference_price IS NOT NULL AND x.reference_price > 0 THEN x.quantity ELSE 0 END)
                            ELSE NULL
                        END
                    FROM execution_records x
                    WHERE x.signal_id = s.signal_id
                ) AS reference_price,
                (
                    SELECT COALESCE(SUM(x.fee), 0.0)
                    FROM execution_records x
                    WHERE x.signal_id = s.signal_id
                ) AS fee
            FROM signal_records s
        """
        conditions: list[str] = []
        params: list[str | int] = []
        if symbol:
            conditions.append("s.symbol = ?")
            params.append(symbol)
        if strategy_name:
            conditions.append("s.strategy_name = ?")
            params.append(strategy_name)
        if start_date:
            conditions.append("s.trade_date >= ?")
            params.append(start_date.isoformat())
        if end_date:
            conditions.append("s.trade_date <= ?")
            params.append(end_date.isoformat())
        if conditions:
            sql += " WHERE " + " AND ".join(conditions)
        sql += " ORDER BY s.trade_date DESC LIMIT ?"
        params.append(max(1, min(limit, 2000)))

        with self._conn() as conn:
            rows = conn.execute(sql, params).fetchall()
        return rows

    def save_cost_calibration(self, result: CostModelCalibrationResult) -> int:
        payload = dict(result.model_dump(mode="json"))
        created_at = datetime.now(timezone.utc).isoformat()
        with self._conn() as conn:
            cur = conn.execute(
                """
                INSERT INTO cost_model_calibration_runs(
                    created_at, symbol, strategy_name, start_date, end_date, sample_size, executed_samples, payload_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    created_at,
                    result.symbol,
                    result.strategy_name,
                    result.start_date.isoformat() if result.start_date else None,
                    result.end_date.isoformat() if result.end_date else None,
                    int(result.sample_size),
                    int(result.executed_samples),
                    json.dumps(payload, ensure_ascii=False),
                ),
            )
            return int(cur.lastrowid)

    def list_cost_calibrations(self, symbol: str | None = None, limit: int = 30) -> list[CostModelCalibrationRecord]:
        sql = """
            SELECT id, created_at, payload_json
            FROM cost_model_calibration_runs
        """
        params: list[str | int] = []
        if symbol:
            sql += " WHERE symbol = ?"
            params.append(symbol)
        sql += " ORDER BY created_at DESC LIMIT ?"
        params.append(max(1, min(limit, 200)))
        with self._conn() as conn:
            rows = conn.execute(sql, params).fetchall()
        return [
            CostModelCalibrationRecord(
                id=int(row["id"]),
                created_at=datetime.fromisoformat(str(row["created_at"])),
                result=CostModelCalibrationResult.model_validate(json.loads(str(row["payload_json"]))).model_copy(
                    update={
                        "calibration_id": int(row["id"])
                    }
                ),
            )
            for row in rows
        ]

    def signal_exists(self, signal_id: str) -> bool:
        with self._conn() as conn:
            row = conn.execute("SELECT 1 FROM signal_records WHERE signal_id = ? LIMIT 1", (signal_id,)).fetchone()
        return row is not None

    def list_signals(self, symbol: str | None = None, limit: int = 200) -> list[SignalDecisionRecord]:
        sql = """
            SELECT signal_id, symbol, strategy_name, trade_date, action, confidence, reason, suggested_position
            FROM signal_records
        """
        params: list[str | int] = []
        if symbol:
            sql += " WHERE symbol = ?"
            params.append(symbol)
        sql += " ORDER BY trade_date DESC LIMIT ?"
        params.append(max(1, min(limit, 2000)))

        with self._conn() as conn:
            rows = conn.execute(sql, params).fetchall()
        return [
            SignalDecisionRecord(
                signal_id=str(row["signal_id"]),
                symbol=str(row["symbol"]),
                strategy_name=str(row["strategy_name"]),
                trade_date=date.fromisoformat(str(row["trade_date"])),
                action=SignalAction(str(row["action"])),
                confidence=float(row["confidence"]),
                reason=str(row["reason"]),
                suggested_position=float(row["suggested_position"]) if row["suggested_position"] is not None else None,
            )
            for row in rows
        ]

    @staticmethod
    def parse_action(value: str | None) -> SignalAction | None:
        if not value:
            return None
        return SignalAction(value)

    @staticmethod
    def _ensure_execution_column(conn: sqlite3.Connection, column: str, column_type: str) -> None:
        rows = conn.execute("PRAGMA table_info(execution_records)").fetchall()
        cols = {str(row[1]) for row in rows}
        if column not in cols:
            conn.execute(f"ALTER TABLE execution_records ADD COLUMN {column} {column_type}")
