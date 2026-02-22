from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import pandas as pd

from trading_assistant.data.base import MarketDataProvider
from trading_assistant.data.cache_store import LocalTimeseriesCache
from trading_assistant.data.composite_provider import CompositeDataProvider


class CountingProvider(MarketDataProvider):
    name = "counting"

    def __init__(self) -> None:
        self.calls: list[tuple[date, date]] = []

    def get_daily_bars(self, symbol: str, start_date: date, end_date: date) -> pd.DataFrame:
        _ = symbol
        self.calls.append((start_date, end_date))
        rows = []
        cursor = start_date
        while cursor <= end_date:
            rows.append(
                {
                    "trade_date": cursor,
                    "symbol": "000001",
                    "open": 10.0,
                    "high": 10.2,
                    "low": 9.8,
                    "close": 10.1,
                    "volume": 100000,
                    "amount": 1_010_000.0,
                    "is_suspended": False,
                    "is_st": False,
                }
            )
            cursor += timedelta(days=1)
        return pd.DataFrame(rows)

    def get_trade_calendar(self, start_date: date, end_date: date) -> pd.DataFrame:
        rows = []
        cursor = start_date
        while cursor <= end_date:
            rows.append({"trade_date": cursor, "is_open": True})
            cursor += timedelta(days=1)
        return pd.DataFrame(rows)

    def get_security_status(self, symbol: str) -> dict[str, bool]:
        _ = symbol
        return {"is_st": False, "is_suspended": False}


def test_composite_provider_uses_incremental_cache(tmp_path: Path) -> None:
    provider = CountingProvider()
    cache = LocalTimeseriesCache(str(tmp_path / "market_cache.db"))
    composite = CompositeDataProvider([provider], cache_store=cache, enable_cache=True)
    start = date(2025, 1, 1)
    end = date(2025, 1, 10)

    _, bars1 = composite.get_daily_bars_with_source("000001", start, end)
    _, bars2 = composite.get_daily_bars_with_source("000001", start, end)
    assert not bars1.empty
    assert not bars2.empty
    assert len(provider.calls) == 1

    _, bars3 = composite.get_daily_bars_with_source("000001", start, date(2025, 1, 15))
    assert not bars3.empty
    assert len(provider.calls) == 2


def test_composite_provider_backfills_internal_missing_dates(tmp_path: Path) -> None:
    provider = CountingProvider()
    cache = LocalTimeseriesCache(str(tmp_path / "market_cache_gap.db"))
    composite = CompositeDataProvider([provider], cache_store=cache, enable_cache=True)

    start = date(2025, 1, 1)
    end = date(2025, 1, 10)
    gap_day = date(2025, 1, 6)
    seed = pd.DataFrame(
        [
            {
                "trade_date": d,
                "symbol": "000001",
                "open": 10.0,
                "high": 10.2,
                "low": 9.8,
                "close": 10.1,
                "volume": 100000,
                "amount": 1_010_000.0,
                "is_suspended": False,
                "is_st": False,
            }
            for d in pd.date_range(start=start, end=end, freq="D").date
            if d != gap_day
        ]
    )
    cache.upsert_daily_bars(provider=provider.name, symbol="000001", bars=seed)

    _, bars = composite.get_daily_bars_with_source("000001", start, end)
    fetched_dates = set(pd.to_datetime(bars["trade_date"]).dt.date.tolist())
    assert gap_day in fetched_dates
    assert len(provider.calls) == 1
    assert provider.calls[0] == (gap_day, gap_day)
