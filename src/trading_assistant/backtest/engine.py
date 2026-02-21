from __future__ import annotations

from dataclasses import dataclass
import math

import pandas as pd

from trading_assistant.core.models import (
    BacktestMetrics,
    BacktestRequest,
    BacktestResult,
    BacktestTrade,
    EquityPoint,
    PortfolioSnapshot,
    Position,
    RiskCheckRequest,
    SignalAction,
)
from trading_assistant.factors.engine import FactorEngine
from trading_assistant.risk.engine import RiskEngine
from trading_assistant.strategy.base import BaseStrategy, StrategyContext


@dataclass
class BacktestState:
    cash: float
    quantity: int
    avg_cost: float
    peak_equity: float
    blocked_signal_count: int = 0
    realized_trades: int = 0
    winning_trades: int = 0


class BacktestEngine:
    """
    A-share aware backtest skeleton:
    - Single symbol
    - Long-only
    - T+1 availability by quantity lock
    - Slippage + commission modeling
    """

    def __init__(self, factor_engine: FactorEngine, risk_engine: RiskEngine) -> None:
        self.factor_engine = factor_engine
        self.risk_engine = risk_engine

    def run(self, bars: pd.DataFrame, req: BacktestRequest, strategy: BaseStrategy) -> BacktestResult:
        if bars.empty:
            return BacktestResult(
                symbol=req.symbol,
                strategy_name=req.strategy_name,
                start_date=req.start_date,
                end_date=req.end_date,
                metrics=BacktestMetrics(
                    total_return=0.0,
                    max_drawdown=0.0,
                    trade_count=0,
                    win_rate=0.0,
                    blocked_signal_count=0,
                ),
                trades=[],
                equity_curve=[],
            )

        sorted_bars = bars.sort_values("trade_date").reset_index(drop=True)
        state = BacktestState(
            cash=req.initial_cash,
            quantity=0,
            avg_cost=0.0,
            peak_equity=req.initial_cash,
        )
        equity_curve: list[EquityPoint] = []
        trades: list[BacktestTrade] = []
        buy_date = None

        for i in range(len(sorted_bars)):
            window = sorted_bars.iloc[: i + 1]
            features = self.factor_engine.compute(window)
            context = StrategyContext(params=req.strategy_params)
            signals = strategy.generate(features, context=context)
            if not signals:
                continue

            signal = signals[-1]
            close = float(window.iloc[-1]["close"])
            turnover20 = float(features.iloc[-1].get("turnover20", 0.0))

            position_value = state.quantity * close
            equity = state.cash + position_value
            state.peak_equity = max(state.peak_equity, equity)
            drawdown = 0.0 if state.peak_equity <= 0 else max(0.0, 1 - equity / state.peak_equity)
            portfolio = PortfolioSnapshot(
                total_value=equity,
                cash=state.cash,
                peak_value=state.peak_equity,
                current_drawdown=drawdown,
                industry_exposure={},
            )

            available_qty = state.quantity
            if buy_date == signal.trade_date:
                available_qty = 0
            position = Position(
                symbol=req.symbol,
                quantity=state.quantity,
                available_quantity=available_qty,
                avg_cost=state.avg_cost,
                market_value=position_value,
                last_buy_date=buy_date,
            )

            risk_req = RiskCheckRequest(
                signal=signal,
                position=position,
                portfolio=portfolio,
                is_st=bool(window.iloc[-1].get("is_st", False)),
                is_suspended=bool(window.iloc[-1].get("is_suspended", False)),
                at_limit_up=False,
                at_limit_down=False,
                avg_turnover_20d=turnover20,
            )
            risk_result = self.risk_engine.evaluate(risk_req)
            if risk_result.blocked:
                state.blocked_signal_count += 1
                trades.append(
                    BacktestTrade(
                        date=signal.trade_date,
                        action=signal.action,
                        price=close,
                        quantity=0,
                        cost=0.0,
                        reason=f"Blocked: {risk_result.summary}",
                        blocked=True,
                    )
                )
            else:
                self._execute_signal(
                    req=req,
                    signal=signal,
                    close=close,
                    state=state,
                    trades=trades,
                    available_qty=available_qty,
                )
                if signal.action == SignalAction.BUY and state.quantity > 0:
                    buy_date = signal.trade_date
                elif signal.action == SignalAction.SELL and state.quantity == 0:
                    buy_date = None

            updated_position_value = state.quantity * close
            updated_equity = state.cash + updated_position_value
            state.peak_equity = max(state.peak_equity, updated_equity)
            updated_drawdown = (
                0.0 if state.peak_equity <= 0 else max(0.0, 1 - updated_equity / state.peak_equity)
            )
            equity_curve.append(
                EquityPoint(
                    date=signal.trade_date,
                    cash=round(state.cash, 2),
                    position_value=round(updated_position_value, 2),
                    equity=round(updated_equity, 2),
                    drawdown=round(updated_drawdown, 6),
                )
            )

        return self._build_result(req=req, state=state, trades=trades, equity_curve=equity_curve)

    def _execute_signal(
        self,
        req: BacktestRequest,
        signal,
        close: float,
        state: BacktestState,
        trades: list[BacktestTrade],
        available_qty: int,
    ) -> None:
        if signal.action == SignalAction.BUY and state.quantity == 0:
            target_alloc = float(signal.suggested_position or req.max_single_position)
            budget = state.cash * target_alloc
            trade_price = close * (1 + req.slippage_rate)
            qty = int(budget // trade_price // req.lot_size * req.lot_size)
            if qty <= 0:
                return

            gross = qty * trade_price
            fee = gross * req.commission_rate
            total_cost = gross + fee
            if total_cost > state.cash:
                return

            state.cash -= total_cost
            state.quantity += qty
            state.avg_cost = trade_price
            trades.append(
                BacktestTrade(
                    date=signal.trade_date,
                    action=SignalAction.BUY,
                    price=round(trade_price, 4),
                    quantity=qty,
                    cost=round(total_cost, 2),
                    reason=signal.reason,
                )
            )
            return

        if signal.action == SignalAction.SELL and state.quantity > 0 and available_qty > 0:
            qty = state.quantity
            trade_price = close * (1 - req.slippage_rate)
            gross = qty * trade_price
            fee = gross * req.commission_rate
            net = gross - fee

            pnl = (trade_price - state.avg_cost) * qty - fee
            state.realized_trades += 1
            if pnl > 0:
                state.winning_trades += 1

            state.cash += net
            state.quantity = 0
            state.avg_cost = 0.0
            trades.append(
                BacktestTrade(
                    date=signal.trade_date,
                    action=SignalAction.SELL,
                    price=round(trade_price, 4),
                    quantity=qty,
                    cost=round(fee, 2),
                    reason=signal.reason,
                )
            )

    def _build_result(
        self,
        req: BacktestRequest,
        state: BacktestState,
        trades: list[BacktestTrade],
        equity_curve: list[EquityPoint],
    ) -> BacktestResult:
        final_equity = equity_curve[-1].equity if equity_curve else req.initial_cash
        total_return = final_equity / req.initial_cash - 1
        max_drawdown = max((p.drawdown for p in equity_curve), default=0.0)
        win_rate = 0.0 if state.realized_trades == 0 else state.winning_trades / state.realized_trades
        annualized_return = 0.0
        sharpe = 0.0
        if equity_curve:
            days = max(1, (req.end_date - req.start_date).days)
            annualized_return = (1 + total_return) ** (365 / days) - 1 if total_return > -1 else -1.0

        if len(equity_curve) >= 3:
            rets: list[float] = []
            prev = equity_curve[0].equity
            for point in equity_curve[1:]:
                if prev > 0:
                    rets.append(point.equity / prev - 1)
                prev = point.equity
            if rets:
                mean_ret = sum(rets) / len(rets)
                var = sum((r - mean_ret) ** 2 for r in rets) / len(rets)
                std = math.sqrt(var)
                if std > 1e-12:
                    sharpe = mean_ret / std * math.sqrt(252)

        return BacktestResult(
            symbol=req.symbol,
            strategy_name=req.strategy_name,
            start_date=req.start_date,
            end_date=req.end_date,
            metrics=BacktestMetrics(
                total_return=round(total_return, 6),
                max_drawdown=round(max_drawdown, 6),
                trade_count=len([t for t in trades if not t.blocked and t.quantity > 0]),
                win_rate=round(win_rate, 6),
                blocked_signal_count=state.blocked_signal_count,
                annualized_return=round(annualized_return, 6),
                sharpe=round(sharpe, 6),
            ),
            trades=trades,
            equity_curve=equity_curve,
        )
