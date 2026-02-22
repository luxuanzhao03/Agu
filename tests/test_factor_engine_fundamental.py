from datetime import date

import pandas as pd

from trading_assistant.factors.engine import FactorEngine


def build_row(**kwargs: object) -> dict[str, object]:
    base: dict[str, object] = {
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
        "fundamental_available": True,
        "fundamental_pit_ok": True,
        "fundamental_stale_days": 40,
        "fundamental_is_stale": False,
        "roe": 14.0,
        "revenue_yoy": 12.0,
        "net_profit_yoy": 18.0,
        "gross_margin": 34.0,
        "debt_to_asset": 42.0,
        "ocf_to_profit": 1.0,
    }
    base.update(kwargs)
    return base


def test_factor_engine_builds_fundamental_scores() -> None:
    df = pd.DataFrame([build_row()])
    out = FactorEngine().compute(df)
    latest = out.iloc[-1]
    assert 0.0 <= float(latest["fundamental_score"]) <= 1.0
    assert float(latest["fundamental_score"]) > 0.55
    assert float(latest["fundamental_completeness"]) == 1.0


def test_factor_engine_penalizes_stale_and_pit_failed() -> None:
    good = FactorEngine().compute(pd.DataFrame([build_row()])).iloc[-1]
    bad = FactorEngine().compute(
        pd.DataFrame(
            [
                build_row(
                    fundamental_pit_ok=False,
                    fundamental_is_stale=True,
                    fundamental_stale_days=700,
                )
            ]
        )
    ).iloc[-1]
    assert float(bad["fundamental_score"]) < float(good["fundamental_score"])

