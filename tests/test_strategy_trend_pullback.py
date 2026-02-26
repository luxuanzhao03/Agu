from __future__ import annotations

from datetime import date

import pandas as pd

from trading_assistant.core.models import SignalAction
from trading_assistant.strategy.base import StrategyContext
from trading_assistant.strategy.trend_pullback import TrendPullbackStrategy


def test_trend_pullback_buys_on_uptrend_pullback() -> None:
    features = pd.DataFrame(
        [
            {
                "trade_date": date(2025, 1, 3),
                "symbol": "000001",
                "close": 10.2,
                "momentum60": 0.08,
                "momentum20": 0.03,
                "zscore20": -0.65,
                "turnover20": 25_000_000.0,
                "volatility20": 0.025,
                "style_regime": "RISK_ON",
                "style_risk_on_score": 0.62,
            }
        ]
    )
    out = TrendPullbackStrategy().generate(features)
    assert out
    assert out[-1].action == SignalAction.BUY
    assert "trend-confirmed pullback" in out[-1].reason.lower()


def test_trend_pullback_blocks_buy_when_trend_not_confirmed() -> None:
    features = pd.DataFrame(
        [
            {
                "trade_date": date(2025, 1, 3),
                "symbol": "000001",
                "close": 10.2,
                "momentum60": -0.03,
                "momentum20": -0.01,
                "zscore20": -0.70,
                "turnover20": 25_000_000.0,
                "volatility20": 0.028,
                "style_regime": "NEUTRAL",
                "style_risk_on_score": 0.50,
            }
        ]
    )
    out = TrendPullbackStrategy().generate(features)
    assert out
    assert out[-1].action != SignalAction.BUY


def test_trend_pullback_respects_market_regime_filter() -> None:
    features = pd.DataFrame(
        [
            {
                "trade_date": date(2025, 1, 3),
                "symbol": "000001",
                "close": 10.2,
                "momentum60": 0.10,
                "momentum20": 0.04,
                "zscore20": -0.80,
                "turnover20": 25_000_000.0,
                "volatility20": 0.026,
                "style_regime": "RISK_ON",
                "style_risk_on_score": 0.60,
            }
        ]
    )
    out = TrendPullbackStrategy().generate(
        features,
        context=StrategyContext(market_state={"regime": "RISK_OFF", "risk_on_score": 0.30}),
    )
    assert out
    assert out[-1].action == SignalAction.WATCH
    assert "risk-off" in out[-1].reason.lower()

