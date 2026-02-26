from __future__ import annotations

from datetime import date, timedelta

import pandas as pd

from trading_assistant.applied_stats.service import AppliedStatisticsService
from trading_assistant.data.base import MarketDataProvider
from trading_assistant.data.composite_provider import CompositeDataProvider
from trading_assistant.factors.engine import FactorEngine
from trading_assistant.fundamentals.service import FundamentalService


class StatsStudyProvider(MarketDataProvider):
    name = "stats_study_provider"

    def get_daily_bars(self, symbol: str, start_date: date, end_date: date) -> pd.DataFrame:
        rows: list[dict[str, object]] = []
        cursor = start_date
        idx = 0
        while cursor <= end_date:
            if cursor.weekday() < 5:
                drift = 0.0012 * idx
                seasonal = ((idx % 7) - 3) * 0.0025
                close = 10.0 + drift + seasonal + 0.02 * (idx // 20)
                rows.append(
                    {
                        "trade_date": cursor,
                        "symbol": symbol,
                        "open": close * 0.998,
                        "high": close * 1.006,
                        "low": close * 0.994,
                        "close": close,
                        "volume": 100_000 + idx * 1_500,
                        "amount": close * (100_000 + idx * 1_500),
                        "is_suspended": False,
                        "is_st": False,
                    }
                )
                idx += 1
            cursor += timedelta(days=1)
        return pd.DataFrame(rows)

    def get_trade_calendar(self, start_date: date, end_date: date) -> pd.DataFrame:
        rows: list[dict[str, object]] = []
        cursor = start_date
        while cursor <= end_date:
            rows.append({"trade_date": cursor, "is_open": cursor.weekday() < 5})
            cursor += timedelta(days=1)
        return pd.DataFrame(rows)

    def get_security_status(self, symbol: str) -> dict[str, bool]:
        _ = symbol
        return {"is_st": False, "is_suspended": False}

    def get_fundamental_snapshot(self, symbol: str, as_of: date) -> dict[str, object]:
        _ = (symbol, as_of)
        return {
            "report_date": date(2024, 12, 31),
            "publish_date": date(2025, 3, 31),
            "roe": 12.6,
            "revenue_yoy": 8.4,
            "net_profit_yoy": 10.1,
            "gross_margin": 29.0,
            "debt_to_asset": 44.2,
            "ocf_to_profit": 0.96,
            "eps": 0.82,
        }


def _build_service() -> AppliedStatisticsService:
    provider = CompositeDataProvider([StatsStudyProvider()])
    return AppliedStatisticsService(
        provider=provider,
        factor_engine=FactorEngine(),
        fundamental_service=FundamentalService(provider=provider),
    )


def test_descriptive_analysis_returns_core_sections() -> None:
    service = _build_service()
    result = service.descriptive_analysis(
        dataset_name="admission_case",
        rows=[
            {"math": 120, "english": 70, "prob": 0.80},
            {"math": 110, "english": 66, "prob": 0.72},
            {"math": 128, "english": 74, "prob": 0.88},
            {"math": 96, "english": 62, "prob": 0.55},
        ],
        columns=["math", "english", "prob"],
    )
    assert result["dataset_name"] == "admission_case"
    assert result["row_count"] == 4
    assert "descriptive_statistics" in result
    assert "correlation" in result


def test_ols_analysis_returns_coefficients() -> None:
    service = _build_service()
    result = service.ols_analysis(
        target=[62, 65, 71, 74, 79, 83, 88, 92],
        features={
            "hours": [3, 4, 5, 5, 6, 7, 8, 9],
            "attendance": [0.70, 0.72, 0.75, 0.77, 0.82, 0.85, 0.90, 0.92],
        },
    )
    assert result["n"] >= 8
    assert result["r2"] > 0
    assert any(row["term"] == "hours" for row in result["coefficients"])


def test_market_factor_study_runs() -> None:
    service = _build_service()
    result = service.market_factor_study(
        symbol="000001",
        start_date=date(2025, 1, 1),
        end_date=date(2025, 6, 30),
        include_fundamentals=True,
        permutations=300,
        bootstrap_samples=300,
        random_seed=7,
    )
    assert result["symbol"] == "000001"
    assert result["sample_size"] >= 24
    assert result["fundamental_enrichment"] is not None
    assert result["fundamental_enrichment"]["mode"] == "pit"
    assert int(result["target_selected_horizon"]) == 10
    assert result["target_selected"] == "ret_next_10d"
    assert result["target_selection_mode"] == "fixed_horizon"
    assert "ols" in result
    assert len(result["interpretation"]) >= 3
