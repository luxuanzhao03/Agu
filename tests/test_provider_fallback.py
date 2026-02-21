from datetime import date

import pandas as pd

from trading_assistant.data.base import MarketDataProvider
from trading_assistant.data.composite_provider import CompositeDataProvider


class FailedProvider(MarketDataProvider):
    name = "failed"

    def get_daily_bars(self, symbol: str, start_date: date, end_date: date) -> pd.DataFrame:
        raise RuntimeError("boom")

    def get_trade_calendar(self, start_date: date, end_date: date) -> pd.DataFrame:
        raise RuntimeError("boom")

    def get_security_status(self, symbol: str) -> dict[str, bool]:
        raise RuntimeError("boom")


class OkProvider(MarketDataProvider):
    name = "ok"

    def get_daily_bars(self, symbol: str, start_date: date, end_date: date) -> pd.DataFrame:
        return pd.DataFrame(
            [
                {
                    "trade_date": start_date,
                    "symbol": symbol,
                    "open": 1,
                    "high": 1,
                    "low": 1,
                    "close": 1,
                    "volume": 1,
                    "amount": 1,
                    "is_suspended": False,
                    "is_st": False,
                }
            ]
        )

    def get_trade_calendar(self, start_date: date, end_date: date) -> pd.DataFrame:
        return pd.DataFrame([{"trade_date": start_date, "is_open": True}])

    def get_security_status(self, symbol: str) -> dict[str, bool]:
        return {"is_st": False, "is_suspended": False}


def test_fallback_to_second_provider() -> None:
    provider = CompositeDataProvider([FailedProvider(), OkProvider()])
    bars = provider.get_daily_bars("000001", date(2025, 1, 2), date(2025, 1, 2))
    assert not bars.empty
    assert bars.iloc[0]["symbol"] == "000001"


def test_fallback_returns_used_provider_name() -> None:
    provider = CompositeDataProvider([FailedProvider(), OkProvider()])
    used_provider, bars = provider.get_daily_bars_with_source("000001", date(2025, 1, 2), date(2025, 1, 2))
    assert used_provider == "ok"
    assert len(bars) == 1


def test_calendar_with_source() -> None:
    provider = CompositeDataProvider([FailedProvider(), OkProvider()])
    used_provider, cal = provider.get_trade_calendar_with_source(date(2025, 1, 2), date(2025, 1, 2))
    assert used_provider == "ok"
    assert len(cal) == 1
