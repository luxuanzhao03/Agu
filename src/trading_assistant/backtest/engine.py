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
from trading_assistant.trading.costs import (
    calc_side_fee,
    estimate_fill_probability,
    estimate_market_impact_rate,
    estimate_roundtrip_cost_bps,
    filled_quantity_by_probability,
    infer_expected_edge_bps,
    required_cash_for_min_lot,
    tiered_slippage_rate,
)
from trading_assistant.trading.small_capital import apply_small_capital_overrides


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
            context = StrategyContext(
                params=req.strategy_params,
                market_state={
                    "enable_small_capital_mode": req.enable_small_capital_mode,
                    "small_capital_principal": float(req.small_capital_principal or req.initial_cash),
                    "small_capital_lot_size": int(req.lot_size),
                    "small_capital_cash_buffer_ratio": 0.10,
                    "commission_rate": req.commission_rate,
                    "min_commission_cny": req.min_commission_cny,
                    "transfer_fee_rate": req.transfer_fee_rate,
                    "stamp_duty_sell_rate": req.stamp_duty_sell_rate,
                    "slippage_rate": req.slippage_rate,
                    "available_cash": state.cash,
                },
            )
            signals = strategy.generate(features, context=context)
            if not signals:
                continue

            signal = signals[-1]
            close = float(window.iloc[-1]["close"])
            _ = apply_small_capital_overrides(
                signal=signal,
                enable_small_capital_mode=req.enable_small_capital_mode,
                principal=float(req.small_capital_principal or req.initial_cash),
                latest_price=close,
                lot_size=int(req.lot_size),
                commission_rate=req.commission_rate,
                min_commission=req.min_commission_cny,
                transfer_fee_rate=req.transfer_fee_rate,
                cash_buffer_ratio=0.10,
                max_single_position=float(req.max_single_position),
                max_positions=max(1, int(float(req.strategy_params.get("max_positions", 3)))),
            )
            turnover20 = float(features.iloc[-1].get("turnover20", 0.0))
            fundamental_available = bool(features.iloc[-1].get("fundamental_available", False))
            fundamental_score = (
                float(features.iloc[-1].get("fundamental_score", 0.5)) if fundamental_available else None
            )
            def _opt_float(value):
                return float(value) if (value is not None and value == value) else None

            tushare_disclosure_risk = _opt_float(features.iloc[-1].get("tushare_disclosure_risk_score"))
            tushare_audit_risk = _opt_float(features.iloc[-1].get("tushare_audit_opinion_risk"))
            tushare_forecast_mid = _opt_float(features.iloc[-1].get("tushare_forecast_pchg_mid"))
            tushare_pledge_ratio = _opt_float(features.iloc[-1].get("tushare_pledge_ratio"))
            tushare_unlock_ratio = _opt_float(features.iloc[-1].get("tushare_share_float_unlock_ratio"))
            tushare_holder_crowding = _opt_float(features.iloc[-1].get("tushare_holder_crowding_ratio"))
            tushare_overhang_risk = _opt_float(features.iloc[-1].get("tushare_overhang_risk_score"))
            stale_days_raw = int(features.iloc[-1].get("fundamental_stale_days", -1))
            small_principal = float(req.small_capital_principal or req.initial_cash)
            small_lot = max(1, int(req.lot_size))
            required_cash = required_cash_for_min_lot(
                price=close,
                lot_size=small_lot,
                commission_rate=req.commission_rate,
                min_commission=req.min_commission_cny,
                transfer_fee_rate=req.transfer_fee_rate,
            )
            roundtrip_cost_bps = estimate_roundtrip_cost_bps(
                price=close,
                lot_size=small_lot,
                commission_rate=req.commission_rate,
                min_commission=req.min_commission_cny,
                transfer_fee_rate=req.transfer_fee_rate,
                stamp_duty_sell_rate=req.stamp_duty_sell_rate,
                slippage_rate=req.slippage_rate,
            )
            expected_edge_bps = infer_expected_edge_bps(
                confidence=float(signal.confidence),
                momentum20=float(features.iloc[-1].get("momentum20", 0.0)),
                event_score=float(features.iloc[-1].get("event_score", 0.0))
                if "event_score" in features.columns
                else None,
                fundamental_score=fundamental_score,
            )

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
                at_limit_up=bool(window.iloc[-1].get("at_limit_up", False)),
                at_limit_down=bool(window.iloc[-1].get("at_limit_down", False)),
                avg_turnover_20d=turnover20,
                fundamental_score=fundamental_score,
                fundamental_available=fundamental_available,
                fundamental_pit_ok=bool(features.iloc[-1].get("fundamental_pit_ok", True)),
                fundamental_stale_days=stale_days_raw if stale_days_raw >= 0 else None,
                tushare_disclosure_risk_score=tushare_disclosure_risk,
                tushare_audit_opinion_risk=tushare_audit_risk,
                tushare_forecast_pchg_mid=tushare_forecast_mid,
                tushare_pledge_ratio=tushare_pledge_ratio,
                tushare_share_float_unlock_ratio=tushare_unlock_ratio,
                tushare_holder_crowding_ratio=tushare_holder_crowding,
                tushare_overhang_risk_score=tushare_overhang_risk,
                enable_small_capital_mode=req.enable_small_capital_mode,
                small_capital_principal=small_principal,
                available_cash=state.cash,
                latest_price=close,
                lot_size=small_lot,
                required_cash_for_min_lot=required_cash,
                estimated_roundtrip_cost_bps=roundtrip_cost_bps,
                expected_edge_bps=expected_edge_bps,
                min_expected_edge_bps=req.small_capital_min_expected_edge_bps,
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
                    turnover20=turnover20,
                    is_suspended=bool(window.iloc[-1].get("is_suspended", False)),
                    at_limit_up=bool(window.iloc[-1].get("at_limit_up", False)),
                    at_limit_down=bool(window.iloc[-1].get("at_limit_down", False)),
                    is_one_word_limit_up=bool(window.iloc[-1].get("is_one_word_limit_up", False)),
                    is_one_word_limit_down=bool(window.iloc[-1].get("is_one_word_limit_down", False)),
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
        turnover20: float | None,
        is_suspended: bool,
        at_limit_up: bool,
        at_limit_down: bool,
        is_one_word_limit_up: bool,
        is_one_word_limit_down: bool,
    ) -> None:
        if signal.action == SignalAction.BUY and state.quantity == 0:
            target_alloc = float(signal.suggested_position or req.max_single_position)
            budget = state.cash * target_alloc
            if req.enable_small_capital_mode:
                budget = min(budget, float(req.small_capital_principal or req.initial_cash))
            desired_qty = int(max(0.0, budget) // close // req.lot_size * req.lot_size)
            if desired_qty <= 0:
                return

            desired_notional = desired_qty * close
            if req.enable_realistic_cost_model:
                slip_rate = tiered_slippage_rate(
                    order_notional=desired_notional,
                    avg_turnover_20d=turnover20,
                    base_slippage_rate=req.slippage_rate,
                )
                impact_rate = estimate_market_impact_rate(
                    order_notional=desired_notional,
                    avg_turnover_20d=turnover20,
                    impact_coeff=req.impact_cost_coeff,
                    impact_exponent=req.impact_cost_exponent,
                )
                fill_prob = estimate_fill_probability(
                    side=SignalAction.BUY,
                    is_suspended=is_suspended,
                    at_limit_up=at_limit_up,
                    at_limit_down=at_limit_down,
                    is_one_word_limit_up=is_one_word_limit_up,
                    is_one_word_limit_down=is_one_word_limit_down,
                    avg_turnover_20d=turnover20,
                    order_notional=desired_notional,
                    probability_floor=req.fill_probability_floor,
                )
            else:
                slip_rate = max(0.0, req.slippage_rate)
                impact_rate = 0.0
                fill_prob = 1.0

            qty = filled_quantity_by_probability(
                desired_qty=desired_qty,
                lot_size=req.lot_size,
                fill_probability=fill_prob,
            )
            if qty <= 0:
                trades.append(
                    BacktestTrade(
                        date=signal.trade_date,
                        action=SignalAction.BUY,
                        price=round(close, 4),
                        quantity=0,
                        cost=0.0,
                        reason=f"No fill: prob={fill_prob:.2f}",
                        blocked=True,
                    )
                )
                return

            trade_price = close * (1 + slip_rate + impact_rate)
            while qty > 0:
                gross = qty * trade_price
                fee = calc_side_fee(
                    notional=gross,
                    commission_rate=req.commission_rate,
                    min_commission=req.min_commission_cny,
                    transfer_fee_rate=req.transfer_fee_rate,
                    stamp_duty_sell_rate=req.stamp_duty_sell_rate,
                    is_sell=False,
                )
                total_cost = gross + fee
                if total_cost <= state.cash:
                    break
                qty -= req.lot_size
            if qty <= 0:
                return

            gross = qty * trade_price
            fee = calc_side_fee(
                notional=gross,
                commission_rate=req.commission_rate,
                min_commission=req.min_commission_cny,
                transfer_fee_rate=req.transfer_fee_rate,
                stamp_duty_sell_rate=req.stamp_duty_sell_rate,
                is_sell=False,
            )
            total_cost = gross + fee
            state.cash -= total_cost
            prev_qty = state.quantity
            prev_cost = state.avg_cost
            state.quantity += qty
            if state.quantity > 0:
                state.avg_cost = ((prev_cost * prev_qty) + total_cost) / state.quantity
            trades.append(
                BacktestTrade(
                    date=signal.trade_date,
                    action=SignalAction.BUY,
                    price=round(trade_price, 4),
                    quantity=qty,
                    cost=round(fee, 2),
                    reason=f"{signal.reason}; fill_prob={fill_prob:.2f}",
                )
            )
            return

        if signal.action == SignalAction.SELL and state.quantity > 0 and available_qty > 0:
            desired_qty = min(state.quantity, available_qty)
            desired_notional = desired_qty * close
            if req.enable_realistic_cost_model:
                slip_rate = tiered_slippage_rate(
                    order_notional=desired_notional,
                    avg_turnover_20d=turnover20,
                    base_slippage_rate=req.slippage_rate,
                )
                impact_rate = estimate_market_impact_rate(
                    order_notional=desired_notional,
                    avg_turnover_20d=turnover20,
                    impact_coeff=req.impact_cost_coeff,
                    impact_exponent=req.impact_cost_exponent,
                )
                fill_prob = estimate_fill_probability(
                    side=SignalAction.SELL,
                    is_suspended=is_suspended,
                    at_limit_up=at_limit_up,
                    at_limit_down=at_limit_down,
                    is_one_word_limit_up=is_one_word_limit_up,
                    is_one_word_limit_down=is_one_word_limit_down,
                    avg_turnover_20d=turnover20,
                    order_notional=desired_notional,
                    probability_floor=req.fill_probability_floor,
                )
            else:
                slip_rate = max(0.0, req.slippage_rate)
                impact_rate = 0.0
                fill_prob = 1.0
            qty = filled_quantity_by_probability(
                desired_qty=desired_qty,
                lot_size=req.lot_size,
                fill_probability=fill_prob,
            )
            if qty <= 0:
                trades.append(
                    BacktestTrade(
                        date=signal.trade_date,
                        action=SignalAction.SELL,
                        price=round(close, 4),
                        quantity=0,
                        cost=0.0,
                        reason=f"No fill: prob={fill_prob:.2f}",
                        blocked=True,
                    )
                )
                return

            trade_price = close * (1 - slip_rate - impact_rate)
            gross = qty * trade_price
            fee = calc_side_fee(
                notional=gross,
                commission_rate=req.commission_rate,
                min_commission=req.min_commission_cny,
                transfer_fee_rate=req.transfer_fee_rate,
                stamp_duty_sell_rate=req.stamp_duty_sell_rate,
                is_sell=True,
            )
            net = gross - fee

            pnl = (trade_price - state.avg_cost) * qty - fee
            state.realized_trades += 1
            if pnl > 0:
                state.winning_trades += 1

            state.cash += net
            state.quantity -= qty
            if state.quantity <= 0:
                state.quantity = 0
                state.avg_cost = 0.0
            trades.append(
                BacktestTrade(
                    date=signal.trade_date,
                    action=SignalAction.SELL,
                    price=round(trade_price, 4),
                    quantity=qty,
                    cost=round(fee, 2),
                    reason=f"{signal.reason}; fill_prob={fill_prob:.2f}",
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
