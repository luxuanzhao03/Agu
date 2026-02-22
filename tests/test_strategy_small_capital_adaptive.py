from __future__ import annotations

from datetime import date

import pandas as pd

from trading_assistant.core.models import SignalAction
from trading_assistant.strategy.base import StrategyContext
from trading_assistant.strategy.small_capital_adaptive import SmallCapitalAdaptiveStrategy


def _make_features(
    *,
    close: float,
    ma20: float,
    ma60: float,
    atr14: float,
    momentum20: float,
    momentum60: float,
    volatility20: float,
    turnover20: float,
    event_score: float = 0.0,
    negative_event_score: float = 0.0,
    fundamental_available: bool = True,
    fundamental_score: float = 0.6,
    tushare_advanced_available: bool = False,
    tushare_advanced_score: float = 0.5,
    tushare_tradability_score: float = 0.5,
    tushare_moneyflow_score: float = 0.5,
) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "trade_date": date(2025, 1, 10),
                "symbol": "000001",
                "close": close,
                "ma20": ma20,
                "ma60": ma60,
                "atr14": atr14,
                "momentum20": momentum20,
                "momentum60": momentum60,
                "volatility20": volatility20,
                "turnover20": turnover20,
                "event_score": event_score,
                "negative_event_score": negative_event_score,
                "fundamental_available": fundamental_available,
                "fundamental_score": fundamental_score,
                "tushare_advanced_available": tushare_advanced_available,
                "tushare_advanced_score": tushare_advanced_score,
                "tushare_tradability_score": tushare_tradability_score,
                "tushare_moneyflow_score": tushare_moneyflow_score,
            }
        ]
    )


def test_small_capital_strategy_watches_when_min_lot_not_affordable() -> None:
    strategy = SmallCapitalAdaptiveStrategy()
    features = _make_features(
        close=120.0,
        ma20=118.0,
        ma60=110.0,
        atr14=3.5,
        momentum20=0.08,
        momentum60=0.15,
        volatility20=0.03,
        turnover20=35_000_000.0,
        event_score=0.7,
        negative_event_score=0.1,
        fundamental_available=True,
        fundamental_score=0.7,
    )

    signals = strategy.generate(
        features,
        context=StrategyContext(
            market_state={
                "enable_small_capital_mode": True,
                "small_capital_principal": 6000.0,
                "small_capital_lot_size": 100,
                "commission_rate": 0.0003,
                "min_commission_cny": 5.0,
                "transfer_fee_rate": 0.00001,
            }
        ),
    )
    assert len(signals) == 1
    assert signals[0].action == SignalAction.WATCH
    assert "Minimum lot cash" in signals[0].reason


def test_small_capital_strategy_buy_with_dynamic_position() -> None:
    strategy = SmallCapitalAdaptiveStrategy()
    features = _make_features(
        close=12.0,
        ma20=11.6,
        ma60=10.9,
        atr14=0.35,
        momentum20=0.06,
        momentum60=0.14,
        volatility20=0.021,
        turnover20=26_000_000.0,
        event_score=0.66,
        negative_event_score=0.08,
        fundamental_available=True,
        fundamental_score=0.67,
    )

    signals = strategy.generate(
        features,
        context=StrategyContext(
            market_state={
                "enable_small_capital_mode": True,
                "small_capital_principal": 8000.0,
                "small_capital_lot_size": 100,
                "commission_rate": 0.0003,
                "min_commission_cny": 5.0,
                "transfer_fee_rate": 0.00001,
            }
        ),
    )
    assert len(signals) == 1
    assert signals[0].action == SignalAction.BUY
    assert signals[0].suggested_position is not None
    assert 0.10 <= float(signals[0].suggested_position) <= 0.35


def test_small_capital_strategy_sell_on_negative_event() -> None:
    strategy = SmallCapitalAdaptiveStrategy()
    features = _make_features(
        close=11.5,
        ma20=11.3,
        ma60=11.1,
        atr14=0.28,
        momentum20=0.03,
        momentum60=0.08,
        volatility20=0.02,
        turnover20=20_000_000.0,
        event_score=0.15,
        negative_event_score=0.82,
        fundamental_available=True,
        fundamental_score=0.62,
    )

    signals = strategy.generate(features, context=StrategyContext(market_state={"small_capital_principal": 9000.0}))
    assert len(signals) == 1
    assert signals[0].action == SignalAction.SELL
    assert "Negative event risk" in signals[0].reason


def test_small_capital_strategy_watch_on_low_tushare_advanced_score() -> None:
    strategy = SmallCapitalAdaptiveStrategy()
    features = _make_features(
        close=13.0,
        ma20=12.6,
        ma60=12.0,
        atr14=0.30,
        momentum20=0.06,
        momentum60=0.13,
        volatility20=0.02,
        turnover20=28_000_000.0,
        event_score=0.55,
        negative_event_score=0.08,
        fundamental_available=True,
        fundamental_score=0.66,
        tushare_advanced_available=True,
        tushare_advanced_score=0.22,
        tushare_tradability_score=0.28,
        tushare_moneyflow_score=0.30,
    )

    signals = strategy.generate(
        features,
        context=StrategyContext(
            market_state={
                "enable_small_capital_mode": True,
                "small_capital_principal": 8000.0,
                "small_capital_lot_size": 100,
            }
        ),
    )
    assert len(signals) == 1
    assert signals[0].action == SignalAction.WATCH
    assert "Tushare advanced score" in signals[0].reason
