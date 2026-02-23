from __future__ import annotations

from datetime import date, datetime
import sqlite3
from pathlib import Path

import pandas as pd


class LocalTimeseriesCache:
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
                CREATE TABLE IF NOT EXISTS daily_bars_cache (
                    provider TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    trade_date TEXT NOT NULL,
                    open REAL,
                    high REAL,
                    low REAL,
                    close REAL,
                    volume REAL,
                    amount REAL,
                    is_suspended INTEGER,
                    is_st INTEGER,
                    PRIMARY KEY(provider, symbol, trade_date)
                )
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_daily_bars_cache_lookup
                ON daily_bars_cache(provider, symbol, trade_date)
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS intraday_bars_cache (
                    provider TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    interval TEXT NOT NULL,
                    bar_time TEXT NOT NULL,
                    open REAL,
                    high REAL,
                    low REAL,
                    close REAL,
                    volume REAL,
                    amount REAL,
                    is_suspended INTEGER,
                    is_st INTEGER,
                    PRIMARY KEY(provider, symbol, interval, bar_time)
                )
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_intraday_bars_cache_lookup
                ON intraday_bars_cache(provider, symbol, interval, bar_time)
                """
            )

    def upsert_daily_bars(self, *, provider: str, symbol: str, bars: pd.DataFrame) -> int:
        if bars is None or bars.empty:
            return 0
        frame = bars.copy()
        frame["trade_date"] = pd.to_datetime(frame["trade_date"], errors="coerce").dt.date
        frame = frame[frame["trade_date"].notna()]
        if frame.empty:
            return 0
        rows = []
        for _, row in frame.iterrows():
            rows.append(
                (
                    provider,
                    symbol,
                    row["trade_date"].isoformat(),
                    self._to_float(row.get("open")),
                    self._to_float(row.get("high")),
                    self._to_float(row.get("low")),
                    self._to_float(row.get("close")),
                    self._to_float(row.get("volume")),
                    self._to_float(row.get("amount")),
                    1 if bool(row.get("is_suspended", False)) else 0,
                    1 if bool(row.get("is_st", False)) else 0,
                )
            )
        with self._conn() as conn:
            conn.executemany(
                """
                INSERT INTO daily_bars_cache(
                    provider, symbol, trade_date, open, high, low, close, volume, amount, is_suspended, is_st
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(provider, symbol, trade_date) DO UPDATE SET
                    open = excluded.open,
                    high = excluded.high,
                    low = excluded.low,
                    close = excluded.close,
                    volume = excluded.volume,
                    amount = excluded.amount,
                    is_suspended = excluded.is_suspended,
                    is_st = excluded.is_st
                """,
                rows,
            )
        return len(rows)

    def load_daily_bars(self, *, provider: str, symbol: str, start_date: date, end_date: date) -> pd.DataFrame:
        with self._conn() as conn:
            rows = conn.execute(
                """
                SELECT
                    trade_date, symbol, open, high, low, close, volume, amount, is_suspended, is_st
                FROM daily_bars_cache
                WHERE provider = ? AND symbol = ? AND trade_date >= ? AND trade_date <= ?
                ORDER BY trade_date
                """,
                (provider, symbol, start_date.isoformat(), end_date.isoformat()),
            ).fetchall()
        if not rows:
            return pd.DataFrame(
                columns=[
                    "trade_date",
                    "symbol",
                    "open",
                    "high",
                    "low",
                    "close",
                    "volume",
                    "amount",
                    "is_suspended",
                    "is_st",
                ]
            )
        frame = pd.DataFrame([dict(row) for row in rows])
        frame["trade_date"] = pd.to_datetime(frame["trade_date"], errors="coerce").dt.date
        frame["is_suspended"] = frame["is_suspended"].astype(int).eq(1)
        frame["is_st"] = frame["is_st"].astype(int).eq(1)
        for col in ("open", "high", "low", "close", "volume", "amount"):
            frame[col] = pd.to_numeric(frame[col], errors="coerce")
        return frame

    def coverage(self, *, provider: str, symbol: str) -> tuple[date | None, date | None, int]:
        with self._conn() as conn:
            row = conn.execute(
                """
                SELECT MIN(trade_date) AS min_date, MAX(trade_date) AS max_date, COUNT(1) AS cnt
                FROM daily_bars_cache
                WHERE provider = ? AND symbol = ?
                """,
                (provider, symbol),
            ).fetchone()
        if row is None:
            return None, None, 0
        min_date = date.fromisoformat(str(row["min_date"])) if row["min_date"] else None
        max_date = date.fromisoformat(str(row["max_date"])) if row["max_date"] else None
        cnt = int(row["cnt"] or 0)
        return min_date, max_date, cnt

    def upsert_intraday_bars(
        self,
        *,
        provider: str,
        symbol: str,
        interval: str,
        bars: pd.DataFrame,
    ) -> int:
        if bars is None or bars.empty:
            return 0
        frame = bars.copy()
        frame["bar_time"] = pd.to_datetime(frame["bar_time"], errors="coerce")
        frame = frame[frame["bar_time"].notna()]
        if frame.empty:
            return 0
        rows = []
        for _, row in frame.iterrows():
            rows.append(
                (
                    provider,
                    symbol,
                    interval,
                    row["bar_time"].isoformat(),
                    self._to_float(row.get("open")),
                    self._to_float(row.get("high")),
                    self._to_float(row.get("low")),
                    self._to_float(row.get("close")),
                    self._to_float(row.get("volume")),
                    self._to_float(row.get("amount")),
                    1 if bool(row.get("is_suspended", False)) else 0,
                    1 if bool(row.get("is_st", False)) else 0,
                )
            )
        with self._conn() as conn:
            conn.executemany(
                """
                INSERT INTO intraday_bars_cache(
                    provider, symbol, interval, bar_time, open, high, low, close, volume, amount, is_suspended, is_st
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(provider, symbol, interval, bar_time) DO UPDATE SET
                    open = excluded.open,
                    high = excluded.high,
                    low = excluded.low,
                    close = excluded.close,
                    volume = excluded.volume,
                    amount = excluded.amount,
                    is_suspended = excluded.is_suspended,
                    is_st = excluded.is_st
                """,
                rows,
            )
        return len(rows)

    def load_intraday_bars(
        self,
        *,
        provider: str,
        symbol: str,
        interval: str,
        start_datetime: datetime,
        end_datetime: datetime,
    ) -> pd.DataFrame:
        with self._conn() as conn:
            rows = conn.execute(
                """
                SELECT
                    bar_time, symbol, open, high, low, close, volume, amount, interval, is_suspended, is_st
                FROM intraday_bars_cache
                WHERE provider = ? AND symbol = ? AND interval = ? AND bar_time >= ? AND bar_time <= ?
                ORDER BY bar_time
                """,
                (
                    provider,
                    symbol,
                    interval,
                    start_datetime.isoformat(),
                    end_datetime.isoformat(),
                ),
            ).fetchall()
        if not rows:
            return pd.DataFrame(
                columns=[
                    "bar_time",
                    "symbol",
                    "open",
                    "high",
                    "low",
                    "close",
                    "volume",
                    "amount",
                    "interval",
                    "is_suspended",
                    "is_st",
                ]
            )
        frame = pd.DataFrame([dict(row) for row in rows])
        frame["bar_time"] = pd.to_datetime(frame["bar_time"], errors="coerce")
        frame = frame[frame["bar_time"].notna()].copy()
        frame["is_suspended"] = frame["is_suspended"].astype(int).eq(1)
        frame["is_st"] = frame["is_st"].astype(int).eq(1)
        for col in ("open", "high", "low", "close", "volume", "amount"):
            frame[col] = pd.to_numeric(frame[col], errors="coerce")
        return frame

    def intraday_coverage(
        self,
        *,
        provider: str,
        symbol: str,
        interval: str,
    ) -> tuple[datetime | None, datetime | None, int]:
        with self._conn() as conn:
            row = conn.execute(
                """
                SELECT MIN(bar_time) AS min_time, MAX(bar_time) AS max_time, COUNT(1) AS cnt
                FROM intraday_bars_cache
                WHERE provider = ? AND symbol = ? AND interval = ?
                """,
                (provider, symbol, interval),
            ).fetchone()
        if row is None:
            return None, None, 0
        min_time = datetime.fromisoformat(str(row["min_time"])) if row["min_time"] else None
        max_time = datetime.fromisoformat(str(row["max_time"])) if row["max_time"] else None
        cnt = int(row["cnt"] or 0)
        return min_time, max_time, cnt

    @staticmethod
    def _to_float(value: object) -> float | None:
        if value is None:
            return None
        try:
            out = float(value)
        except Exception:  # noqa: BLE001
            return None
        return out
