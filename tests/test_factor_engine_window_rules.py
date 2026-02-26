from __future__ import annotations

from datetime import date, timedelta

import pandas as pd

from trading_assistant.factors.engine import FactorEngine


def _bars(days: int = 30) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    close = 10.0
    for i in range(days):
        trade_date = date(2025, 1, 1) + timedelta(days=i)
        close = close * 1.002
        volume = 200_000 + i * 1000
        rows.append(
            {
                "trade_date": trade_date,
                "symbol": "000001",
                "open": close * 0.99,
                "high": close * 1.01,
                "low": close * 0.98,
                "close": close,
                "volume": volume,
                "amount": close * volume,
                "is_suspended": False,
                "is_st": False,
            }
        )
    return pd.DataFrame(rows)


def test_factor_engine_keeps_insufficient_windows_as_nan() -> None:
    out = FactorEngine().compute(_bars(days=30))

    # 20-day windows should remain NaN before window maturity.
    assert out["momentum20"].iloc[:20].isna().all()
    assert out["volatility20"].iloc[:20].isna().all()
    assert out["momentum20"].iloc[20:].notna().any()
    assert out["volatility20"].iloc[20:].notna().any()

    # 120-day momentum is unavailable for this sample length.
    assert out["momentum120"].isna().all()

