from datetime import date

import pandas as pd

from trading_assistant.core.models import SignalAction
from trading_assistant.strategy.event_driven import EventDrivenStrategy
from trading_assistant.strategy.trend import TrendFollowingStrategy


def test_trend_strategy_downgrades_buy_when_fundamental_weak() -> None:
    features = pd.DataFrame(
        [
            {
                "trade_date": date(2025, 1, 2),
                "symbol": "000001",
                "close": 10.0,
                "ma20": 10.0,
                "ma60": 9.8,
                "atr14": 0.2,
                "momentum20": 0.05,
                "fundamental_available": True,
                "fundamental_score": 0.20,
            },
            {
                "trade_date": date(2025, 1, 3),
                "symbol": "000001",
                "close": 10.4,
                "ma20": 10.2,
                "ma60": 9.9,
                "atr14": 0.2,
                "momentum20": 0.08,
                "fundamental_available": True,
                "fundamental_score": 0.20,
            },
        ]
    )
    out = TrendFollowingStrategy().generate(features)
    assert out
    assert out[-1].action == SignalAction.WATCH
    assert "fundamental score" in out[-1].reason.lower()


def test_event_strategy_downgrades_buy_when_fundamental_weak() -> None:
    features = pd.DataFrame(
        [
            {
                "trade_date": date(2025, 1, 3),
                "symbol": "000001",
                "close": 10.4,
                "momentum20": 0.02,
                "event_score": 0.85,
                "negative_event_score": 0.05,
                "fundamental_available": True,
                "fundamental_score": 0.15,
            }
        ]
    )
    out = EventDrivenStrategy().generate(features)
    assert out
    assert out[-1].action == SignalAction.WATCH
    assert "fundamental score" in out[-1].reason.lower()

