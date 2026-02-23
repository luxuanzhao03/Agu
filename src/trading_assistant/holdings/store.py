from __future__ import annotations

from datetime import date, datetime, timezone
import json
import sqlite3
from pathlib import Path
from uuid import uuid4

from trading_assistant.core.models import (
    HoldingRecommendationAction,
    ManualHoldingAnalysisResult,
    ManualHoldingRecommendationSnapshot,
    ManualHoldingSide,
    ManualHoldingTradeCreate,
    ManualHoldingTradeRecord,
)


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
                    reference_price REAL,
                    executed_at TEXT,
                    is_partial_fill INTEGER NOT NULL DEFAULT 0,
                    unfilled_reason TEXT NOT NULL DEFAULT '',
                    note TEXT NOT NULL
                )
                """
            )
            self._ensure_column(conn, "manual_holding_trades", "reference_price", "REAL")
            self._ensure_column(conn, "manual_holding_trades", "executed_at", "TEXT")
            self._ensure_column(conn, "manual_holding_trades", "is_partial_fill", "INTEGER NOT NULL DEFAULT 0")
            self._ensure_column(conn, "manual_holding_trades", "unfilled_reason", "TEXT NOT NULL DEFAULT ''")
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
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS manual_holding_analysis_runs (
                    run_id TEXT PRIMARY KEY,
                    generated_at TEXT NOT NULL,
                    as_of_date TEXT NOT NULL,
                    next_trade_date TEXT,
                    strategy_name TEXT NOT NULL,
                    provider TEXT NOT NULL,
                    market_overview TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS manual_holding_analysis_recommendations (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id TEXT NOT NULL,
                    generated_at TEXT NOT NULL,
                    as_of_date TEXT NOT NULL,
                    next_trade_date TEXT,
                    strategy_name TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    symbol_name TEXT NOT NULL,
                    action TEXT NOT NULL,
                    target_lots INTEGER NOT NULL,
                    delta_lots INTEGER NOT NULL,
                    confidence REAL NOT NULL,
                    expected_next_day_return REAL NOT NULL,
                    up_probability REAL NOT NULL,
                    style_regime TEXT NOT NULL,
                    execution_window TEXT NOT NULL,
                    intraday_risk_level TEXT NOT NULL,
                    risk_flags_json TEXT NOT NULL,
                    rationale TEXT NOT NULL,
                    FOREIGN KEY(run_id) REFERENCES manual_holding_analysis_runs(run_id)
                )
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_manual_holding_analysis_recommendations_symbol_date
                ON manual_holding_analysis_recommendations(symbol, as_of_date DESC, id DESC)
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_manual_holding_analysis_recommendations_strategy_date
                ON manual_holding_analysis_recommendations(strategy_name, as_of_date DESC, id DESC)
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_manual_holding_analysis_recommendations_run
                ON manual_holding_analysis_recommendations(run_id)
                """
            )

    def insert_trade(self, req: ManualHoldingTradeCreate) -> ManualHoldingTradeRecord:
        created_at = datetime.now(timezone.utc).isoformat()
        symbol = str(req.symbol or "").strip().upper()
        symbol_name = str(req.symbol_name or "").strip()
        note = str(req.note or "").strip()
        unfilled_reason = str(req.unfilled_reason or "").strip()
        executed_at = req.executed_at.isoformat() if req.executed_at else None
        with self._conn() as conn:
            cur = conn.execute(
                """
                INSERT INTO manual_holding_trades(
                    created_at, trade_date, symbol, symbol_name, side, price, lots, lot_size, fee,
                    reference_price, executed_at, is_partial_fill, unfilled_reason, note
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                    float(req.reference_price) if req.reference_price is not None else None,
                    executed_at,
                    1 if req.is_partial_fill else 0,
                    unfilled_reason,
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
            reference_price=(float(req.reference_price) if req.reference_price is not None else None),
            executed_at=req.executed_at,
            is_partial_fill=bool(req.is_partial_fill),
            unfilled_reason=unfilled_reason,
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
            SELECT
                id, created_at, trade_date, symbol, symbol_name, side, price, lots, lot_size, fee,
                reference_price, executed_at, is_partial_fill, unfilled_reason, note
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
        params.append(max(1, min(int(limit), 20_000)))
        with self._conn() as conn:
            rows = conn.execute(sql, params).fetchall()
        out: list[ManualHoldingTradeRecord] = []
        for row in rows:
            lots = int(row["lots"] or 0)
            lot_size = int(row["lot_size"] or 100)
            ref_price = None
            try:
                if row["reference_price"] is not None:
                    ref_val = float(row["reference_price"])
                    if ref_val > 0:
                        ref_price = ref_val
            except Exception:  # noqa: BLE001
                ref_price = None

            executed_at = None
            try:
                if row["executed_at"] is not None and str(row["executed_at"]).strip():
                    executed_at = datetime.fromisoformat(str(row["executed_at"]))
            except Exception:  # noqa: BLE001
                executed_at = None
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
                    reference_price=ref_price,
                    executed_at=executed_at,
                    is_partial_fill=bool(int(row["is_partial_fill"] or 0)),
                    unfilled_reason=str(row["unfilled_reason"] or ""),
                    note=str(row["note"] or ""),
                )
            )
        return out

    def delete_trade(self, trade_id: int) -> bool:
        with self._conn() as conn:
            cur = conn.execute("DELETE FROM manual_holding_trades WHERE id = ?", (int(trade_id),))
        return int(cur.rowcount or 0) > 0

    def save_analysis_snapshot(self, result: ManualHoldingAnalysisResult) -> str:
        run_id = str(result.analysis_run_id or uuid4().hex)
        generated_at = result.generated_at.isoformat()
        next_trade_date = result.next_trade_date.isoformat() if result.next_trade_date else None

        with self._conn() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO manual_holding_analysis_runs(
                    run_id, generated_at, as_of_date, next_trade_date, strategy_name, provider, market_overview
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    generated_at,
                    result.as_of_date.isoformat(),
                    next_trade_date,
                    str(result.strategy_name or "").strip().lower(),
                    str(result.provider or ""),
                    str(result.market_overview or ""),
                ),
            )
            conn.execute(
                "DELETE FROM manual_holding_analysis_recommendations WHERE run_id = ?",
                (run_id,),
            )
            for item in result.recommendations:
                conn.execute(
                    """
                    INSERT INTO manual_holding_analysis_recommendations(
                        run_id, generated_at, as_of_date, next_trade_date, strategy_name, symbol, symbol_name,
                        action, target_lots, delta_lots, confidence, expected_next_day_return, up_probability,
                        style_regime, execution_window, intraday_risk_level, risk_flags_json, rationale
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        run_id,
                        generated_at,
                        result.as_of_date.isoformat(),
                        next_trade_date,
                        str(result.strategy_name or "").strip().lower(),
                        str(item.symbol or "").strip().upper(),
                        str(item.symbol_name or "").strip(),
                        item.action.value,
                        int(item.target_lots),
                        int(item.delta_lots),
                        float(item.confidence),
                        float(item.expected_next_day_return),
                        float(item.up_probability),
                        str(item.style_regime or ""),
                        str(item.execution_window or ""),
                        str(item.intraday_risk_level or ""),
                        json.dumps(list(item.risk_flags or []), ensure_ascii=False),
                        str(item.rationale or ""),
                    ),
                )
        return run_id

    def list_analysis_recommendations(
        self,
        *,
        symbol: str | None = None,
        strategy_name: str | None = None,
        start_date: date | None = None,
        end_date: date | None = None,
        min_confidence: float = 0.0,
        limit: int = 5000,
    ) -> list[ManualHoldingRecommendationSnapshot]:
        sql = """
            SELECT
                run_id, generated_at, as_of_date, next_trade_date, strategy_name, symbol, symbol_name, action,
                target_lots, delta_lots, confidence, expected_next_day_return, up_probability,
                style_regime, execution_window, intraday_risk_level, risk_flags_json, rationale
            FROM manual_holding_analysis_recommendations
        """
        where: list[str] = []
        params: list[str | int | float] = []
        if symbol:
            where.append("symbol = ?")
            params.append(str(symbol).strip().upper())
        if strategy_name:
            where.append("strategy_name = ?")
            params.append(str(strategy_name).strip().lower())
        if start_date is not None:
            where.append("as_of_date >= ?")
            params.append(start_date.isoformat())
        if end_date is not None:
            where.append("as_of_date <= ?")
            params.append(end_date.isoformat())
        if float(min_confidence) > 0:
            where.append("confidence >= ?")
            params.append(float(min_confidence))
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += " ORDER BY as_of_date DESC, generated_at DESC, id DESC LIMIT ?"
        params.append(max(1, min(int(limit), 20_000)))

        with self._conn() as conn:
            rows = conn.execute(sql, params).fetchall()

        out: list[ManualHoldingRecommendationSnapshot] = []
        for row in rows:
            raw_flags = str(row["risk_flags_json"] or "[]")
            try:
                parsed_flags = json.loads(raw_flags)
            except Exception:  # noqa: BLE001
                parsed_flags = []
            if not isinstance(parsed_flags, list):
                parsed_flags = []
            out.append(
                ManualHoldingRecommendationSnapshot(
                    run_id=str(row["run_id"]),
                    generated_at=datetime.fromisoformat(str(row["generated_at"])),
                    as_of_date=date.fromisoformat(str(row["as_of_date"])),
                    next_trade_date=(
                        date.fromisoformat(str(row["next_trade_date"]))
                        if row["next_trade_date"] is not None and str(row["next_trade_date"]).strip()
                        else None
                    ),
                    strategy_name=str(row["strategy_name"]),
                    symbol=str(row["symbol"]),
                    symbol_name=str(row["symbol_name"] or ""),
                    action=HoldingRecommendationAction(str(row["action"])),
                    target_lots=int(row["target_lots"] or 0),
                    delta_lots=int(row["delta_lots"] or 0),
                    confidence=float(row["confidence"] or 0.0),
                    expected_next_day_return=float(row["expected_next_day_return"] or 0.0),
                    up_probability=float(row["up_probability"] or 0.5),
                    style_regime=str(row["style_regime"] or ""),
                    execution_window=str(row["execution_window"] or ""),
                    intraday_risk_level=str(row["intraday_risk_level"] or ""),
                    risk_flags=[str(x) for x in parsed_flags if str(x).strip()],
                    rationale=str(row["rationale"] or ""),
                )
            )
        return out

    @staticmethod
    def _ensure_column(conn: sqlite3.Connection, table: str, column: str, column_type: str) -> None:
        rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
        cols = {str(row[1]) for row in rows}
        if column not in cols:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {column_type}")
