from __future__ import annotations

from datetime import date, datetime, timezone
import sqlite3
from pathlib import Path

from trading_assistant.core.models import ManualHoldingSide, ManualHoldingTradeCreate, ManualHoldingTradeRecord


class HoldingStore:
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
                CREATE TABLE IF NOT EXISTS manual_holding_trades (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    created_at TEXT NOT NULL,
                    trade_date TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    symbol_name TEXT NOT NULL,
                    side TEXT NOT NULL,
                    price REAL NOT NULL,
                    lots INTEGER NOT NULL,
                    lot_size INTEGER NOT NULL,
                    fee REAL NOT NULL,
                    note TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_manual_holding_trades_symbol_date
                ON manual_holding_trades(symbol, trade_date DESC, id DESC)
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_manual_holding_trades_date
                ON manual_holding_trades(trade_date DESC, id DESC)
                """
            )

    def insert_trade(self, req: ManualHoldingTradeCreate) -> ManualHoldingTradeRecord:
        created_at = datetime.now(timezone.utc).isoformat()
        symbol = str(req.symbol or "").strip().upper()
        symbol_name = str(req.symbol_name or "").strip()
        note = str(req.note or "").strip()
        with self._conn() as conn:
            cur = conn.execute(
                """
                INSERT INTO manual_holding_trades(
                    created_at, trade_date, symbol, symbol_name, side, price, lots, lot_size, fee, note
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    created_at,
                    req.trade_date.isoformat(),
                    symbol,
                    symbol_name,
                    req.side.value,
                    float(req.price),
                    int(req.lots),
                    int(req.lot_size),
                    float(req.fee),
                    note,
                ),
            )
            trade_id = int(cur.lastrowid)
        return ManualHoldingTradeRecord(
            id=trade_id,
            created_at=datetime.fromisoformat(created_at),
            trade_date=req.trade_date,
            symbol=symbol,
            symbol_name=symbol_name,
            side=req.side,
            price=float(req.price),
            lots=int(req.lots),
            lot_size=int(req.lot_size),
            quantity=int(req.lots) * int(req.lot_size),
            fee=float(req.fee),
            note=note,
        )

    def list_trades(
        self,
        *,
        symbol: str | None = None,
        start_date: date | None = None,
        end_date: date | None = None,
        limit: int = 2000,
    ) -> list[ManualHoldingTradeRecord]:
        sql = """
            SELECT id, created_at, trade_date, symbol, symbol_name, side, price, lots, lot_size, fee, note
            FROM manual_holding_trades
        """
        where: list[str] = []
        params: list[str | int] = []
        if symbol:
            where.append("symbol = ?")
            params.append(str(symbol).strip().upper())
        if start_date is not None:
            where.append("trade_date >= ?")
            params.append(start_date.isoformat())
        if end_date is not None:
            where.append("trade_date <= ?")
            params.append(end_date.isoformat())
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += " ORDER BY trade_date DESC, id DESC LIMIT ?"
        params.append(max(1, min(int(limit), 5000)))
        with self._conn() as conn:
            rows = conn.execute(sql, params).fetchall()
        out: list[ManualHoldingTradeRecord] = []
        for row in rows:
            lots = int(row["lots"] or 0)
            lot_size = int(row["lot_size"] or 100)
            out.append(
                ManualHoldingTradeRecord(
                    id=int(row["id"]),
                    created_at=datetime.fromisoformat(str(row["created_at"])),
                    trade_date=date.fromisoformat(str(row["trade_date"])),
                    symbol=str(row["symbol"]),
                    symbol_name=str(row["symbol_name"] or ""),
                    side=ManualHoldingSide(str(row["side"])),
                    price=float(row["price"] or 0.0),
                    lots=lots,
                    lot_size=lot_size,
                    quantity=lots * lot_size,
                    fee=float(row["fee"] or 0.0),
                    note=str(row["note"] or ""),
                )
            )
        return out

    def delete_trade(self, trade_id: int) -> bool:
        with self._conn() as conn:
            cur = conn.execute("DELETE FROM manual_holding_trades WHERE id = ?", (int(trade_id),))
        return int(cur.rowcount or 0) > 0

