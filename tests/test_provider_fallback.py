from datetime import date, datetime

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

    def get_intraday_bars(
        self,
        symbol: str,
        start_datetime: datetime,
        end_datetime: datetime,
        *,
        interval: str = "15m",
    ) -> pd.DataFrame:
        _ = (symbol, start_datetime, end_datetime, interval)
        raise RuntimeError("boom")

    def get_corporate_event_snapshot(
        self,
        symbol: str,
        as_of: date,
        *,
        lookback_days: int = 120,
    ) -> dict[str, object]:
        _ = (symbol, as_of, lookback_days)
        raise RuntimeError("boom")

    def get_market_style_snapshot(
        self,
        as_of: date,
        *,
        lookback_days: int = 30,
    ) -> dict[str, object]:
        _ = (as_of, lookback_days)
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

    def get_fundamental_snapshot(self, symbol: str, as_of: date) -> dict[str, object]:
        return {
            "report_date": as_of,
            "publish_date": as_of,
            "roe": 12.0,
            "revenue_yoy": 15.0,
            "net_profit_yoy": 18.0,
            "gross_margin": 35.0,
            "debt_to_asset": 40.0,
            "ocf_to_profit": 1.1,
            "eps": 0.8,
        }

    def get_intraday_bars(
        self,
        symbol: str,
        start_datetime: datetime,
        end_datetime: datetime,
        *,
        interval: str = "15m",
    ) -> pd.DataFrame:
        _ = end_datetime
        return pd.DataFrame(
            [
                {
                    "bar_time": start_datetime,
                    "symbol": symbol,
                    "open": 1,
                    "high": 1,
                    "low": 1,
                    "close": 1,
                    "volume": 1,
                    "amount": 1,
                    "interval": interval,
                    "is_suspended": False,
                    "is_st": False,
                }
            ]
        )

    def get_corporate_event_snapshot(
        self,
        symbol: str,
        as_of: date,
        *,
        lookback_days: int = 120,
    ) -> dict[str, object]:
        _ = (symbol, as_of, lookback_days)
        return {
            "event_score": 0.62,
            "negative_event_score": 0.14,
            "event_count": 3,
        }

    def get_market_style_snapshot(
        self,
        as_of: date,
        *,
        lookback_days: int = 30,
    ) -> dict[str, object]:
        _ = (as_of, lookback_days)
        return {
            "risk_on_score": 0.58,
            "flow_score": 0.56,
            "leverage_score": 0.52,
            "theme_heat_score": 0.61,
            "regime": "RISK_ON",
        }


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


def test_fundamental_snapshot_fallback_with_source() -> None:
    provider = CompositeDataProvider([FailedProvider(), OkProvider()])
    used_provider, snapshot = provider.get_fundamental_snapshot_with_source("000001", date(2025, 1, 2))
    assert used_provider == "ok"
    assert snapshot["roe"] == 12.0


def test_intraday_fallback_with_source() -> None:
    provider = CompositeDataProvider([FailedProvider(), OkProvider()])
    used_provider, bars = provider.get_intraday_bars_with_source(
        "000001",
        datetime(2025, 1, 2, 9, 30),
        datetime(2025, 1, 2, 10, 0),
        interval="15m",
    )
    assert used_provider == "ok"
    assert len(bars) == 1


def test_corporate_event_snapshot_fallback_with_source() -> None:
    provider = CompositeDataProvider([FailedProvider(), OkProvider()])
    used_provider, snapshot = provider.get_corporate_event_snapshot_with_source(
        symbol="000001",
        as_of=date(2025, 1, 2),
        lookback_days=120,
    )
    assert used_provider == "ok"
    assert snapshot["event_count"] == 3


def test_market_style_snapshot_fallback_with_source() -> None:
    provider = CompositeDataProvider([FailedProvider(), OkProvider()])
    used_provider, snapshot = provider.get_market_style_snapshot_with_source(
        as_of=date(2025, 1, 2),
        lookback_days=30,
    )
    assert used_provider == "ok"
    assert snapshot["regime"] == "RISK_ON"
