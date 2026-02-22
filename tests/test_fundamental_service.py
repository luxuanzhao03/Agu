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

