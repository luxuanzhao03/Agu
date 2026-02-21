from __future__ import annotations

import sqlite3
from datetime import date
from pathlib import Path

from trading_assistant.core.models import ExecutionRecordCreate, SignalDecisionRecord, SignalAction


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
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_exec_signal_id ON execution_records(signal_id)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_signal_symbol_date ON signal_records(symbol, trade_date DESC)"
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
                    signal_id, symbol, execution_date, side, quantity, price, fee, note
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    record.signal_id,
                    record.symbol,
                    record.execution_date.isoformat(),
                    record.side.value,
                    record.quantity,
                    record.price,
                    record.fee,
                    record.note,
                ),
            )
            return int(cur.lastrowid)

    def load_pairs(self, symbol: str | None = None, start_date: date | None = None, end_date: date | None = None, limit: int = 500) -> list[sqlite3.Row]:
        sql = """
            SELECT
                s.signal_id,
                s.symbol,
                s.trade_date,
                s.action AS signal_action,
                s.confidence,
                e.side AS executed_action,
                e.execution_date,
                e.quantity,
                e.price
            FROM signal_records s
            LEFT JOIN execution_records e
              ON s.signal_id = e.signal_id
        """
        conditions: list[str] = []
        params: list[str | int] = []
        if symbol:
            conditions.append("s.symbol = ?")
            params.append(symbol)
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
