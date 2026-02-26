from datetime import date

import pandas as pd

from trading_assistant.data.base import MarketDataProvider
from trading_assistant.data.composite_provider import CompositeDataProvider
from trading_assistant.fundamentals.service import FundamentalService


class FundamentalProvider(MarketDataProvider):
    name = "fund-ok"

    def get_daily_bars(self, symbol: str, start_date: date, end_date: date) -> pd.DataFrame:
        return pd.DataFrame()

    def get_trade_calendar(self, start_date: date, end_date: date) -> pd.DataFrame:
        return pd.DataFrame()

    def get_security_status(self, symbol: str) -> dict[str, bool]:
        return {"is_st": False, "is_suspended": False}

    def get_fundamental_snapshot(self, symbol: str, as_of: date) -> dict[str, object]:
        _ = symbol
        return {
            "report_date": date(2024, 12, 31),
            "publish_date": date(2025, 1, 1),
            "roe": 11.5,
            "revenue_yoy": 9.0,
            "net_profit_yoy": 13.0,
            "gross_margin": 33.0,
            "debt_to_asset": 42.0,
            "ocf_to_profit": 1.1,
            "eps": 0.9,
        }


class FuturePublishProvider(FundamentalProvider):
    name = "fund-future"

    def get_fundamental_snapshot(self, symbol: str, as_of: date) -> dict[str, object]:
        _ = symbol, as_of
        return {
            "report_date": date(2024, 12, 31),
            "publish_date": date(2025, 2, 1),
            "roe": 11.5,
            "revenue_yoy": 9.0,
            "net_profit_yoy": 13.0,
            "gross_margin": 33.0,
            "debt_to_asset": 42.0,
            "ocf_to_profit": 1.1,
            "eps": 0.9,
        }


class QuarterlyPublishProvider(FundamentalProvider):
    name = "fund-quarterly"

    def get_fundamental_snapshot(self, symbol: str, as_of: date) -> dict[str, object]:
        _ = symbol
        # Mimic quarterly updates becoming available over time.
        if as_of < date(2025, 3, 31):
            return {
                "report_date": date(2024, 9, 30),
                "publish_date": date(2024, 10, 31),
                "roe": 10.0,
                "revenue_yoy": 6.0,
                "net_profit_yoy": 8.0,
                "gross_margin": 31.0,
                "debt_to_asset": 45.0,
                "ocf_to_profit": 0.95,
                "eps": 0.75,
            }
        return {
            "report_date": date(2024, 12, 31),
            "publish_date": date(2025, 3, 31),
            "roe": 12.0,
            "revenue_yoy": 7.5,
            "net_profit_yoy": 9.5,
            "gross_margin": 32.0,
            "debt_to_asset": 44.0,
            "ocf_to_profit": 1.05,
            "eps": 0.82,
        }


def build_bars() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "trade_date": date(2025, 1, 2),
                "symbol": "000001",
                "open": 10.0,
                "high": 10.2,
                "low": 9.8,
                "close": 10.1,
                "volume": 100_000,
                "amount": 1_010_000,
                "is_suspended": False,
                "is_st": False,
            }
        ]
    )


def build_multi_month_bars() -> pd.DataFrame:
    dates = [
        date(2025, 1, 2),
        date(2025, 2, 28),
        date(2025, 3, 31),
        date(2025, 4, 30),
        date(2025, 5, 30),
    ]
    rows: list[dict[str, object]] = []
    for i, d in enumerate(dates):
        close = 10.0 + 0.1 * i
        rows.append(
            {
                "trade_date": d,
                "symbol": "000001",
                "open": close * 0.99,
                "high": close * 1.01,
                "low": close * 0.98,
                "close": close,
                "volume": 100_000 + i * 1_000,
                "amount": close * (100_000 + i * 1_000),
                "is_suspended": False,
                "is_st": False,
            }
        )
    return pd.DataFrame(rows)


def test_fundamental_service_enrich_success() -> None:
    service = FundamentalService(provider=CompositeDataProvider([FundamentalProvider()]))
    bars, stats = service.enrich_bars(
        symbol="000001",
        bars=build_bars(),
        as_of=date(2025, 1, 2),
        max_staleness_days=540,
    )
    assert bool(stats["available"]) is True
    assert bool(stats["pit_ok"]) is True
    assert bool(bars.iloc[-1]["fundamental_available"]) is True
    assert bars.iloc[-1]["fundamental_source"] == "fund-ok"
    assert float(bars.iloc[-1]["roe"]) == 11.5


def test_fundamental_service_marks_pit_failure() -> None:
    service = FundamentalService(provider=CompositeDataProvider([FuturePublishProvider()]))
    bars, stats = service.enrich_bars(
        symbol="000001",
        bars=build_bars(),
        as_of=date(2025, 1, 2),
        max_staleness_days=540,
    )
    assert bool(stats["available"]) is True
    assert bool(stats["pit_ok"]) is False
    assert bool(bars.iloc[-1]["fundamental_pit_ok"]) is False


def test_fundamental_service_enrich_point_in_time_varies() -> None:
    service = FundamentalService(provider=CompositeDataProvider([QuarterlyPublishProvider()]))
    bars, stats = service.enrich_bars_point_in_time(
        symbol="000001",
        bars=build_multi_month_bars(),
        max_staleness_days=540,
        anchor_frequency="month",
    )
    assert bool(stats["available"]) is True
    assert stats["mode"] == "pit"
    assert int(stats["anchors"]) >= 2
    assert bars["roe"].nunique(dropna=True) >= 2
    assert bars["fundamental_report_date"].nunique(dropna=True) >= 2
