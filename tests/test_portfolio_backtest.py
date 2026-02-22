from __future__ import annotations

from datetime import date, timedelta

import pandas as pd

from trading_assistant.backtest.portfolio_engine import PortfolioBacktestEngine
from trading_assistant.core.models import PortfolioBacktestRequest, SignalAction, SignalCandidate, StrategyInfo
from trading_assistant.factors.engine import FactorEngine
from trading_assistant.portfolio.optimizer import PortfolioOptimizer
from trading_assistant.risk.engine import RiskEngine
from trading_assistant.strategy.base import BaseStrategy, StrategyContext


class AlwaysBuyStrategy(BaseStrategy):
    info = StrategyInfo(
        name="always_buy",
        title="Always Buy",
        description="test strategy",
        frequency="D",
    )

    def generate(self, features: pd.DataFrame, context: StrategyContext | None = None) -> list[SignalCandidate]:
        _ = context
        latest = features.iloc[-1]
        return [
            SignalCandidate(
                symbol=str(latest["symbol"]),
                trade_date=latest["trade_date"],
                action=SignalAction.BUY,
                confidence=0.8,
                reason="always buy",
                strategy_name="always_buy",
                suggested_position=0.12,
            )
        ]


def _bars(symbol: str, start: date, base_price: float) -> pd.DataFrame:
    rows = []
    for i in range(15):
        d = start + timedelta(days=i)
        close = base_price * (1.0 + 0.005 * i)
        rows.append(
            {
                "trade_date": d,
                "symbol": symbol,
                "open": close * 0.99,
                "high": close * 1.01,
                "low": close * 0.99,
                "close": close,
                "volume": 200_000 + i * 1000,
                "amount": close * (200_000 + i * 1000),
                "is_suspended": False,
                "is_st": False,
            }
        )
    return pd.DataFrame(rows)


def _downtrend_bars(symbol: str, start: date, base_price: float) -> pd.DataFrame:
    rows = []
    for i in range(18):
        d = start + timedelta(days=i)
        close = max(0.5, base_price * (1.0 - 0.016 * i))
        rows.append(
            {
                "trade_date": d,
                "symbol": symbol,
                "open": close * 1.005,
                "high": close * 1.01,
                "low": close * 0.99,
                "close": close,
                "volume": 240_000 + i * 800,
                "amount": close * (240_000 + i * 800),
                "is_suspended": False,
                "is_st": False,
            }
        )
    return pd.DataFrame(rows)


def test_portfolio_backtest_engine_runs_multi_symbol() -> None:
    engine = PortfolioBacktestEngine(factor_engine=FactorEngine(), optimizer=PortfolioOptimizer())
    req = PortfolioBacktestRequest(
        symbols=["000001", "000002"],
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 15),
        strategy_name="always_buy",
        rebalance_interval_days=3,
        initial_cash=200_000,
        max_single_position=0.2,
        max_industry_exposure=0.6,
        target_gross_exposure=0.9,
        industry_map={"000001": "BANK", "000002": "TECH"},
        theme_map={"000001": "DIVIDEND", "000002": "AI"},
    )
    result = engine.run(
        bars_by_symbol={
            "000001": _bars("000001", req.start_date, 10.0),
            "000002": _bars("000002", req.start_date, 12.0),
        },
        req=req,
        strategy=AlwaysBuyStrategy(),
    )
    assert result.metrics.trade_count >= 1
    assert result.metrics.avg_utilization > 0
    assert len(result.equity_curve) >= 5


def test_portfolio_backtest_risk_circuit_breaker_blocks_rebalance() -> None:
    engine = PortfolioBacktestEngine(
        factor_engine=FactorEngine(),
        optimizer=PortfolioOptimizer(),
        risk_engine=RiskEngine(
            max_single_position=0.6,
            max_drawdown=0.5,
            max_industry_exposure=0.9,
            min_turnover_20d=1.0,
        ),
    )
    req = PortfolioBacktestRequest(
        symbols=["000001", "000002"],
        start_date=date(2025, 2, 1),
        end_date=date(2025, 2, 18),
        strategy_name="always_buy",
        rebalance_interval_days=1,
        initial_cash=200_000,
        max_single_position=0.5,
        max_industry_exposure=0.8,
        max_theme_exposure=0.9,
        target_gross_exposure=0.95,
        risk_max_drawdown=0.5,
        risk_max_daily_loss=0.005,
        risk_max_consecutive_losses=2,
        risk_var_confidence=0.95,
        risk_max_var=0.01,
        risk_max_es=0.015,
        industry_map={"000001": "BANK", "000002": "TECH"},
        theme_map={"000001": "DIVIDEND", "000002": "AI"},
    )
    result = engine.run(
        bars_by_symbol={
            "000001": _downtrend_bars("000001", req.start_date, 10.0),
            "000002": _downtrend_bars("000002", req.start_date, 12.0),
        },
        req=req,
        strategy=AlwaysBuyStrategy(),
    )
    assert result.metrics.risk_blocked_days >= 1
    assert result.metrics.trade_count >= 1
