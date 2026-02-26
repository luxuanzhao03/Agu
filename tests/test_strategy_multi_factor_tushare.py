from __future__ import annotations

from datetime import date

import pandas as pd

from trading_assistant.core.models import SignalAction
from trading_assistant.strategy.base import StrategyContext
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


def test_multi_factor_supports_contrarian_liquidity_direction() -> None:
    features = pd.DataFrame(
        [
            {
                "trade_date": date(2025, 1, 2),
                "symbol": "002175",
                "momentum20": 0.0,
                "momentum60": 0.0,
                "volatility20": 0.02,
                "turnover20": 1_000_000.0,
                "fundamental_available": False,
                "tushare_advanced_available": False,
            }
        ]
    )
    strategy = MultiFactorStrategy()

    positive = strategy.generate(
        features,
        context=StrategyContext(
            params={
                "buy_threshold": 0.8,
                "sell_threshold": 0.2,
                "w_momentum": 0.0,
                "w_quality": 0.0,
                "w_low_vol": 0.0,
                "w_liquidity": 1.0,
                "w_fundamental": 0.0,
                "w_tushare_advanced": 0.0,
                "liquidity_direction": 1.0,
            }
        ),
    )
    assert positive and positive[0].action == SignalAction.SELL
    assert positive[0].metadata.get("liquidity_direction") == 1.0

    contrarian = strategy.generate(
        features,
        context=StrategyContext(
            params={
                "buy_threshold": 0.8,
                "sell_threshold": 0.2,
                "w_momentum": 0.0,
                "w_quality": 0.0,
                "w_low_vol": 0.0,
                "w_liquidity": 1.0,
                "w_fundamental": 0.0,
                "w_tushare_advanced": 0.0,
                "liquidity_direction": -1.0,
            }
        ),
    )
    assert contrarian and contrarian[0].action == SignalAction.BUY
    assert contrarian[0].metadata.get("liquidity_direction") == -1.0
