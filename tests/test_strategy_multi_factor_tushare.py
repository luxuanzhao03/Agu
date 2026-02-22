from __future__ import annotations

from datetime import date

import pandas as pd

from trading_assistant.core.models import SignalAction
from trading_assistant.strategy.multi_factor import MultiFactorStrategy


def test_multi_factor_downgrades_buy_when_tushare_advanced_weak() -> None:
    features = pd.DataFrame(
        [
            {
                "trade_date": date(2025, 1, 2),
                "symbol": "000001",
                "momentum20": 0.05,
                "momentum60": 0.10,
                "volatility20": 0.018,
                "turnover20": 35_000_000.0,
                "fundamental_available": True,
                "fundamental_score": 0.72,
                "tushare_advanced_available": True,
                "tushare_advanced_score": 0.18,
            }
        ]
    )
    out = MultiFactorStrategy().generate(features)
    assert out
    assert out[0].action == SignalAction.WATCH
    assert "tushare advanced score" in out[0].reason.lower()
