from datetime import date, timedelta

import pandas as pd

from trading_assistant.backtest.engine import BacktestEngine
from trading_assistant.core.models import BacktestRequest, SignalAction, SignalCandidate, StrategyInfo
from trading_assistant.factors.engine import FactorEngine
from trading_assistant.risk.engine import RiskEngine
from trading_assistant.strategy.base import BaseStrategy, StrategyContext


class ToggleStrategy(BaseStrategy):
    info = StrategyInfo(
        name="toggle",
        title="Toggle",
        description="Buy then sell for test.",
        frequency="D",
    )

    def generate(self, features: pd.DataFrame, context: StrategyContext | None = None) -> list[SignalCandidate]:
        latest = features.sort_values("trade_date").iloc[-1]
        if len(features) == 1:
            action = SignalAction.BUY
            reason = "enter"
            pos = 0.1
        else:
            action = SignalAction.SELL
            reason = "exit"
            pos = None
        return [
            SignalCandidate(
                symbol=str(latest["symbol"]),
                trade_date=latest["trade_date"],
                action=action,
                confidence=0.8,
                reason=reason,
                strategy_name="toggle",
                suggested_position=pos,
            )
        ]


class SparseStrategy(BaseStrategy):
    info = StrategyInfo(
        name="sparse",
        title="Sparse",
        description="Signal only on first bar, then no signal.",
        frequency="D",
    )

    def generate(self, features: pd.DataFrame, context: StrategyContext | None = None) -> list[SignalCandidate]:
        latest = features.sort_values("trade_date").iloc[-1]
        if len(features) == 1:
            return [
                SignalCandidate(
                    symbol=str(latest["symbol"]),
                    trade_date=latest["trade_date"],
                    action=SignalAction.BUY,
                    confidence=0.7,
                    reason="first bar buy",
                    strategy_name="sparse",
                    suggested_position=0.1,
                )
            ]
        return []


def build_bars() -> pd.DataFrame:
    start = date(2025, 1, 2)
    rows = []
    prices = [10, 10.5, 10.8, 10.3]
    for i, p in enumerate(prices):
        d = start + timedelta(days=i)
        rows.append(
            {
                "trade_date": d,
                "symbol": "000001",
                "open": p,
                "high": p * 1.01,
                "low": p * 0.99,
                "close": p,
                "volume": 100000,
                "amount": p * 100000,
                "is_suspended": False,
                "is_st": False,
            }
        )
    return pd.DataFrame(rows)


def test_backtest_runs_and_outputs_metrics() -> None:
    risk_engine = RiskEngine(
        max_single_position=0.2,
        max_drawdown=0.5,
        max_industry_exposure=0.5,
        min_turnover_20d=1000,
    )
    engine = BacktestEngine(factor_engine=FactorEngine(), risk_engine=risk_engine)
    req = BacktestRequest(
        symbol="000001",
        start_date=date(2025, 1, 2),
        end_date=date(2025, 1, 9),
        strategy_name="toggle",
        initial_cash=100000,
        max_single_position=0.2,
    )
    result = engine.run(build_bars(), req=req, strategy=ToggleStrategy())
    assert result.metrics.trade_count >= 1
    assert len(result.equity_curve) >= 1


def test_backtest_keeps_equity_points_even_without_daily_signals() -> None:
    risk_engine = RiskEngine(
        max_single_position=0.2,
        max_drawdown=0.5,
        max_industry_exposure=0.5,
        min_turnover_20d=1000,
    )
    bars = build_bars()
    engine = BacktestEngine(factor_engine=FactorEngine(), risk_engine=risk_engine)
    req = BacktestRequest(
        symbol="000001",
        start_date=date(2025, 1, 2),
        end_date=date(2025, 1, 9),
        strategy_name="sparse",
        initial_cash=100000,
        max_single_position=0.2,
    )
    result = engine.run(bars, req=req, strategy=SparseStrategy())
    assert len(result.equity_curve) == len(bars)
